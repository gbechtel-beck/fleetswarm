"""
Fleet Poller — polls AxeOS, CGMiner, and Pool API endpoints.
Stores results in SQLite for history + serves to the Flask app.
"""
import json
import os
import re
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import requests

DB_PATH = Path("/data/fleet.db")
CONFIG_PATH = Path("/data/config.json")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
DEFAULT_SUBNET = os.environ.get("DEFAULT_SUBNET", "192.168.1.0/24")
HTTP_TIMEOUT = 3
TCP_TIMEOUT = 3


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS miners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT UNIQUE NOT NULL,
                hostname TEXT,
                kind TEXT,                   -- bitaxe | bitforge | nerd | avalon | unknown
                first_seen TEXT,
                last_seen TEXT,
                online INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                miner_id INTEGER NOT NULL,
                ts TEXT NOT NULL,
                hashrate_ghs REAL,
                temp_c REAL,
                power_w REAL,
                fan_pct INTEGER,
                shares_accepted INTEGER,
                shares_rejected INTEGER,
                best_diff TEXT,
                pool_url TEXT,
                worker TEXT,
                raw_json TEXT,
                FOREIGN KEY (miner_id) REFERENCES miners(id)
            );
            CREATE INDEX IF NOT EXISTS idx_samples_miner_ts
                ON samples(miner_id, ts DESC);
        """)
        db.commit()


def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "subnet": DEFAULT_SUBNET,
    "miners": [],          # explicit IPs to always poll, e.g. ["192.168.1.160"]
    "avalon_ips": [],      # CGMiner TCP — Avalon Q
    "scan_enabled": True,
    "btc_address": "",     # for Public Pool / SoloStrike worker cross-ref
    "pool_api_url": "",    # e.g. http://localhost/api or https://solostrike.io/api
    "alerts": {
        "temp_warn_c": 65,        # cards turn orange above this
        "temp_critical_c": 75,    # cards turn red and trigger fleet alert above this
        "reject_rate_warn_pct": 1.0,  # >1% rejected shares = warning
        "offline_grace_min": 5,   # miner offline this long counts as a fleet alert
    }
}


def load_config():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2))
        return DEFAULT_CONFIG.copy()
    try:
        cfg = json.loads(CONFIG_PATH.read_text())
        # merge defaults for any missing keys
        merged = {**DEFAULT_CONFIG, **cfg}
        merged["alerts"] = {**DEFAULT_CONFIG["alerts"], **cfg.get("alerts", {})}
        return merged
    except Exception as e:
        print(f"[config] load failed: {e}, using defaults")
        return DEFAULT_CONFIG.copy()


def save_config(cfg):
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


# ─────────────────────────────────────────────────────────────────────────────
# Protocol: AxeOS REST  (Bitaxe, BitForge, Nerd Miner)
# ─────────────────────────────────────────────────────────────────────────────

def poll_axeos(ip):
    try:
        r = requests.get(f"http://{ip}/api/system/info", timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        d = r.json()
        # Normalise to our schema
        hostname = d.get("hostname") or d.get("ssid") or ip
        kind = classify_axeos(d, hostname)
        return {
            "ip": ip,
            "hostname": hostname,
            "kind": kind,
            "hashrate_ghs": d.get("hashRate") or d.get("hashrate") or 0,
            "temp_c": d.get("temp") or d.get("temperature"),
            "power_w": d.get("power"),
            "fan_pct": d.get("fanspeed") or d.get("fanSpeed"),
            "shares_accepted": d.get("sharesAccepted"),
            "shares_rejected": d.get("sharesRejected"),
            "best_diff": str(d.get("bestSessionDiff") or d.get("bestDiff") or ""),
            "pool_url": d.get("stratumURL") or d.get("stratumUrl"),
            "worker": d.get("stratumUser"),
            "raw": d,
        }
    except (requests.RequestException, ValueError, KeyError):
        return None


def classify_axeos(d, hostname):
    """Best-effort classification from device fields."""
    asic = (d.get("ASICModel") or d.get("asicModel") or "").upper()
    board = (d.get("boardVersion") or "").lower()
    name = (hostname or "").lower()

    # Name-based wins first (Gil's hostnames like LilGuy1, BitForge-1, BitAx-1)
    if "nerd" in name or "lilguy" in name or "lilg" in name:
        return "nerd"
    if "bitforge" in name or "forge" in name:
        return "bitforge"
    if "bitax" in name or name.startswith("baxe") or "gamma" in name:
        return "bitaxe"
    # Fall back to ASIC model
    if asic in ("BM1397", "BM1366", "BM1368", "BM1370"):
        return "bitaxe"
    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Protocol: CGMiner TCP  (Avalon Q)
# ─────────────────────────────────────────────────────────────────────────────

def cgminer_send(ip, command, timeout=TCP_TIMEOUT):
    """Send a CGMiner API command and return the raw response string."""
    try:
        with socket.create_connection((ip, 4028), timeout=timeout) as s:
            s.sendall(json.dumps({"command": command}).encode())
            chunks = []
            s.settimeout(timeout)
            try:
                while True:
                    data = s.recv(8192)
                    if not data:
                        break
                    chunks.append(data)
            except socket.timeout:
                pass
            return b"".join(chunks).decode("utf-8", errors="ignore").rstrip("\x00")
    except (OSError, socket.timeout):
        return None


def parse_avalon_estats(raw):
    """Avalon estats is pipe/comma-delimited inside a JSON wrapper.
    Extracts MM ID0 line and parses key[value] pairs.
    """
    if not raw:
        return {}
    try:
        outer = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not outer.get("STATS"):
        return {}
    stats = outer["STATS"][0] if outer["STATS"] else {}
    # The MM ID line lives under various keys depending on firmware
    mm_blob = ""
    for k, v in stats.items():
        if isinstance(v, str) and "GHSspd" in v:
            mm_blob = v
            break
    fields = {}
    for m in re.finditer(r"(\w+)\[([^\]]*)\]", mm_blob):
        fields[m.group(1)] = m.group(2).strip()
    fields["_summary"] = mm_blob
    return fields


def poll_avalon(ip):
    estats_raw = cgminer_send(ip, "estats")
    fields = parse_avalon_estats(estats_raw)
    if not fields:
        return None

    summary_raw = cgminer_send(ip, "summary")
    pools_raw = cgminer_send(ip, "pools")

    pool_url = None
    worker = None
    accepted = rejected = None
    if summary_raw:
        try:
            sumj = json.loads(summary_raw)
            if sumj.get("SUMMARY"):
                s0 = sumj["SUMMARY"][0]
                accepted = s0.get("Accepted")
                rejected = s0.get("Rejected")
        except json.JSONDecodeError:
            pass
    if pools_raw:
        try:
            pj = json.loads(pools_raw)
            if pj.get("POOLS"):
                p0 = pj["POOLS"][0]
                pool_url = p0.get("URL")
                worker = p0.get("User")
        except json.JSONDecodeError:
            pass

    # GHSspd is "current speed in GH/s", GHSavg is rolling avg
    def to_float(s):
        try:
            return float(s)
        except (TypeError, ValueError):
            return None

    ghs = to_float(fields.get("GHSspd")) or to_float(fields.get("GHSavg")) or 0
    temp = to_float(fields.get("TAvg")) or to_float(fields.get("Temp"))
    power = to_float(fields.get("MPO"))  # measured power output in W

    fan_vals = [to_float(fields.get(f"Fan{i}")) for i in range(1, 5)]
    fan_vals = [f for f in fan_vals if f is not None]
    fan_avg = sum(fan_vals) / len(fan_vals) if fan_vals else None

    return {
        "ip": ip,
        "hostname": worker or f"avalon-{ip.rsplit('.', 1)[-1]}",
        "kind": "avalon",
        "hashrate_ghs": ghs,
        "temp_c": temp,
        "power_w": power,
        "fan_pct": int(fan_avg) if fan_avg else None,
        "shares_accepted": accepted,
        "shares_rejected": rejected,
        "best_diff": "",  # Avalon doesn't expose session-best the same way
        "pool_url": pool_url,
        "worker": worker,
        "raw": {"estats": fields, "fanRPMs": fan_vals},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Subnet scan
# ─────────────────────────────────────────────────────────────────────────────

def expand_subnet(cidr):
    """Tiny IPv4 /24 expansion (kept simple — no ipaddress dep needed)."""
    try:
        base, prefix = cidr.split("/")
        if prefix != "24":
            # only /24 supported in scan; explicit IPs handle the rest
            return []
        a, b, c, _ = base.split(".")
        return [f"{a}.{b}.{c}.{i}" for i in range(1, 255)]
    except Exception:
        return []


def discover_axeos(ips, max_workers=40):
    """Probe a list of IPs for AxeOS endpoint. Returns successful polls."""
    found = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for result in ex.map(poll_axeos, ips):
            if result:
                found.append(result)
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────

def upsert_miner(conn, sample):
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("SELECT id FROM miners WHERE ip = ?", (sample["ip"],))
    row = cur.fetchone()
    if row:
        conn.execute(
            "UPDATE miners SET hostname=?, kind=?, last_seen=?, online=1 WHERE id=?",
            (sample["hostname"], sample["kind"], now, row["id"]),
        )
        return row["id"]
    cur = conn.execute(
        "INSERT INTO miners (ip, hostname, kind, first_seen, last_seen, online) "
        "VALUES (?, ?, ?, ?, ?, 1)",
        (sample["ip"], sample["hostname"], sample["kind"], now, now),
    )
    return cur.lastrowid


def insert_sample(conn, miner_id, sample):
    conn.execute(
        """INSERT INTO samples
           (miner_id, ts, hashrate_ghs, temp_c, power_w, fan_pct,
            shares_accepted, shares_rejected, best_diff, pool_url, worker, raw_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            miner_id,
            datetime.now(timezone.utc).isoformat(),
            sample.get("hashrate_ghs"),
            sample.get("temp_c"),
            sample.get("power_w"),
            sample.get("fan_pct"),
            sample.get("shares_accepted"),
            sample.get("shares_rejected"),
            sample.get("best_diff"),
            sample.get("pool_url"),
            sample.get("worker"),
            json.dumps(sample.get("raw") or {})[:8000],
        ),
    )


def mark_offline(conn, ips_seen_this_round):
    placeholders = ",".join("?" for _ in ips_seen_this_round) or "''"
    if ips_seen_this_round:
        conn.execute(
            f"UPDATE miners SET online=0 WHERE ip NOT IN ({placeholders})",
            ips_seen_this_round,
        )
    else:
        conn.execute("UPDATE miners SET online=0")


def prune_old_samples(conn, keep_days=14):
    cutoff = datetime.now(timezone.utc).timestamp() - (keep_days * 86400)
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
    conn.execute("DELETE FROM samples WHERE ts < ?", (cutoff_iso,))


# ─────────────────────────────────────────────────────────────────────────────
# Main poll cycle
# ─────────────────────────────────────────────────────────────────────────────

_last_run = {"ts": None, "found": 0, "errors": []}


def run_one_cycle():
    cfg = load_config()
    found = []
    errors = []

    # 1. Explicit AxeOS IPs (fast path — these always get polled)
    explicit = list(cfg.get("miners") or [])
    for ip in explicit:
        s = poll_axeos(ip)
        if s:
            found.append(s)

    # 2. Avalon CGMiner IPs
    for ip in cfg.get("avalon_ips") or []:
        s = poll_avalon(ip)
        if s:
            found.append(s)

    # 3. Subnet scan (only AxeOS-style — Avalons must be configured explicitly)
    if cfg.get("scan_enabled"):
        candidates = expand_subnet(cfg.get("subnet", "192.168.1.0/24"))
        already = {s["ip"] for s in found}
        candidates = [ip for ip in candidates if ip not in already
                      and ip not in (cfg.get("avalon_ips") or [])]
        scanned = discover_axeos(candidates, max_workers=50)
        found.extend(scanned)

    # 4. Persist
    seen_ips = []
    with closing(db_conn()) as conn:
        for s in found:
            try:
                mid = upsert_miner(conn, s)
                insert_sample(conn, mid, s)
                seen_ips.append(s["ip"])
            except Exception as e:
                errors.append(f"{s.get('ip')}: {e}")
        mark_offline(conn, seen_ips)
        prune_old_samples(conn)
        conn.commit()

    _last_run["ts"] = datetime.now(timezone.utc).isoformat()
    _last_run["found"] = len(found)
    _last_run["errors"] = errors[-5:]
    return found, errors


def poller_loop():
    init_db()
    while True:
        try:
            run_one_cycle()
        except Exception as e:
            print(f"[poller] cycle error: {e}")
        time.sleep(POLL_INTERVAL)


def start_background():
    t = threading.Thread(target=poller_loop, daemon=True, name="poller")
    t.start()
    return t


def get_status():
    return dict(_last_run)
