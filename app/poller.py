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
from urllib.parse import quote

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
                uptime_s INTEGER,
                raw_json TEXT,
                FOREIGN KEY (miner_id) REFERENCES miners(id)
            );
            CREATE INDEX IF NOT EXISTS idx_samples_miner_ts
                ON samples(miner_id, ts DESC);

            -- Pool worker cross-reference data. Refreshed each cycle, read per-pool.
            CREATE TABLE IF NOT EXISTS pool_workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_key TEXT NOT NULL,       -- ckpool | axebch2 | unmineable
                worker_name TEXT,
                hashrate_ghs REAL,
                best_diff TEXT,
                last_seen TEXT,               -- per pool API
                fetched_at TEXT NOT NULL,     -- when WE fetched
                raw_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pool_workers_pool ON pool_workers(pool_key);
        """)

        # Migration: add uptime_s column to samples if missing (v0.2.2 upgrade)
        cols = [r[1] for r in db.execute("PRAGMA table_info(samples)").fetchall()]
        if "uptime_s" not in cols:
            db.execute("ALTER TABLE samples ADD COLUMN uptime_s INTEGER")

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

    # Per-class alert thresholds — Avalon hashboards run hot by design (Canaan
    # spec is 80–90°C normal, 113°C absolute max), Bitaxe/BitForge live around
    # 60–70°C, Nerd Miners barely break 50°C. Per-class makes more sense than
    # global because a single threshold either falsely alerts on Avalons or
    # ignores actual problems on Nerd Miners.
    "alerts_by_class": {
        "bitforge": {"temp_warn_c": 65, "temp_critical_c": 75,  "reject_rate_warn_pct": 1.0},
        "bitaxe":   {"temp_warn_c": 65, "temp_critical_c": 75,  "reject_rate_warn_pct": 1.0},
        "avalon":   {"temp_warn_c": 90, "temp_critical_c": 100, "reject_rate_warn_pct": 0.5},
        "nerd":     {"temp_warn_c": 50, "temp_critical_c": 60,  "reject_rate_warn_pct": 2.0},
        "unknown":  {"temp_warn_c": 65, "temp_critical_c": 75,  "reject_rate_warn_pct": 1.0},
    },
    "offline_grace_min": 5,   # global — applies to all classes

    # Three pool cross-reference slots
    "pools": {
        "ckpool": {
            "enabled": False,
            "label": "Public Pool / SoloStrike",
            "base_url": "",          # e.g. http://localhost/api or https://solostrike.io/api
            "btc_address": "",       # used as path arg: /api/client/<addr>
        },
        "axebch2": {
            "enabled": False,
            "label": "AxeBCH2",
            # AxeBCH2 is a single-tenant Umbrel app — endpoint is /api/pool/workers,
            # no address parameter needed. From inside Umbrel use the internal
            # Docker hostname; from outside, the LAN IP + app proxy port.
            "base_url": "http://willitmod-dev-axebch2_app_1:3000",
        },
        "unmineable": {
            "enabled": False,
            "label": "unMineable",
            "address": "",           # your unMineable address
            "coin": "KAS",           # KAS / ALPH / RVN / etc.
        },
    },
}


def load_config():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CONFIG, indent=2))
        return DEFAULT_CONFIG.copy()
    try:
        cfg = json.loads(CONFIG_PATH.read_text())
        # Merge defaults for any missing keys at every level
        merged = {**DEFAULT_CONFIG, **cfg}

        # Migration path from 0.1.x → 0.2.x:
        # Old config had a single flat "alerts" block with offline_grace_min inside it.
        # If we see that, lift offline_grace_min out and convert thresholds to per-class.
        migrated_from_v01 = False
        if "alerts" in cfg and "alerts_by_class" not in cfg:
            migrated_from_v01 = True
            old = cfg["alerts"]
            merged["offline_grace_min"] = old.get("offline_grace_min", 5)
            merged["alerts_by_class"] = {
                k: {
                    "temp_warn_c": old.get("temp_warn_c", v["temp_warn_c"]),
                    "temp_critical_c": old.get("temp_critical_c", v["temp_critical_c"]),
                    "reject_rate_warn_pct": old.get("reject_rate_warn_pct", v["reject_rate_warn_pct"]),
                }
                for k, v in DEFAULT_CONFIG["alerts_by_class"].items()
            }

        # Per-class merge for v0.2+ configs — fill missing classes with defaults
        # (skip if we just migrated, since migration already produced a complete map)
        if not migrated_from_v01:
            merged_alerts = dict(DEFAULT_CONFIG["alerts_by_class"])
            for k, v in (cfg.get("alerts_by_class") or {}).items():
                if k in merged_alerts:
                    merged_alerts[k] = {**merged_alerts[k], **v}
            merged["alerts_by_class"] = merged_alerts

        # Pools merge — preserve any user-set fields, fill in missing slots
        merged_pools = {}
        for k, v in DEFAULT_CONFIG["pools"].items():
            merged_pools[k] = {**v, **(cfg.get("pools") or {}).get(k, {})}
        merged["pools"] = merged_pools

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

        # Round fan to integer — AxeOS sometimes returns the value as a calculated
        # float (e.g. 44.8386421) which renders ugly in the UI.
        raw_fan = d.get("fanspeed") or d.get("fanSpeed")
        fan_pct = int(round(raw_fan)) if raw_fan is not None else None

        return {
            "ip": ip,
            "hostname": hostname,
            "kind": kind,
            "hashrate_ghs": d.get("hashRate") or d.get("hashrate") or 0,
            "temp_c": d.get("temp") or d.get("temperature"),
            "power_w": d.get("power"),
            "fan_pct": fan_pct,
            "shares_accepted": d.get("sharesAccepted"),
            "shares_rejected": d.get("sharesRejected"),
            "best_diff": str(d.get("bestSessionDiff") or d.get("bestDiff") or ""),
            "uptime_s": d.get("uptimeSeconds") or d.get("uptime"),
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
    best_share = None
    elapsed_s = None
    if summary_raw:
        try:
            sumj = json.loads(summary_raw)
            if sumj.get("SUMMARY"):
                s0 = sumj["SUMMARY"][0]
                accepted = s0.get("Accepted")
                rejected = s0.get("Rejected")
                # Avalon's CGMiner summary exposes Best Share as raw integer difficulty
                best_share = s0.get("Best Share")
                elapsed_s = s0.get("Elapsed")
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

    # Hostname fallback: if the worker name looks like a wallet address (bitcoincash:,
    # bc1, qrr, 1xxx, or just a long hex/base58 blob), use the IP-based name instead.
    # This avoids the dashboard showing "bitcoincashii:qrr6zd3qjndwcpqq..." as a name.
    def looks_like_address(s):
        if not s:
            return True
        if len(s) > 26:                           # most worker names are <20 chars
            return True
        if ":" in s:                              # bitcoincash:, bitcoin:
            return True
        if s.lower().startswith(("bc1", "qrr", "qrl", "qq")):
            return True
        return False

    if looks_like_address(worker):
        hostname = f"Avalon-{ip.rsplit('.', 1)[-1]}"
    else:
        hostname = worker

    return {
        "ip": ip,
        "hostname": hostname,
        "kind": "avalon",
        "hashrate_ghs": ghs,
        "temp_c": temp,
        "power_w": power,
        "fan_pct": int(fan_avg) if fan_avg else None,
        "shares_accepted": accepted,
        "shares_rejected": rejected,
        "best_diff": str(best_share) if best_share else "",
        "uptime_s": elapsed_s,
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
# Pool cross-reference fetchers
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ckpool(cfg):
    """Public Pool / SoloStrike / generic CKPool API.
    Endpoint: <base_url>/client/<btc_address>
    Returns list of {worker_name, hashrate_ghs, best_diff, last_seen}.
    """
    if not cfg.get("enabled") or not cfg.get("base_url") or not cfg.get("btc_address"):
        return []
    # URL-encode the address — BCH addresses contain colons (bitcoincash:qrr...)
    # which would otherwise be treated as port specifiers by some HTTP libraries.
    addr = quote(cfg["btc_address"], safe="")
    url = f"{cfg['base_url'].rstrip('/')}/client/{addr}"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[pool:ckpool] {url} → HTTP {r.status_code}")
            return []
        d = r.json()
        out = []
        for w in (d.get("workers") or []):
            # hashRate is in H/s — convert to GH/s
            hr_hs = w.get("hashRate") or 0
            out.append({
                "worker_name": w.get("name"),
                "hashrate_ghs": (hr_hs / 1e9) if hr_hs else 0,
                "best_diff": str(w.get("bestDifficulty") or ""),
                "last_seen": w.get("lastSeen"),
                "raw": w,
            })
        if not out:
            print(f"[pool:ckpool] {url} → 200 OK but no workers in response")
        return out
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"[pool:ckpool] {url} → error: {e}")
        return []


def fetch_axebch2(cfg):
    """AxeBCH2 self-hosted Umbrel pool.

    Despite being a Public Pool fork, AxeBCH2 exposes a different API shape than
    upstream CKPool. It serves /api/pool/workers (no address parameter) and
    returns ALL workers in a single response — perfect for a single-tenant
    Umbrel app where the pool only has one user (you).

    Schema (verified against ghcr.io/willitmod/axebch2-app:0.1.38-dev):
        {
          "workers": 9,
          "active_workers": 9,
          "total_workers": 9,
          "lastshare": 1777192916,
          "workers_details": [{
              "workername": "<address>.<label>",  // e.g. "qrr...zqfhd3.QMoney"
              "hashrate_ths": 87.3,
              "hashrate_5m_ths": 90.5,
              "hashrate_1h_ths": 90.8,
              "lastshare": 1777192916,
              "lastshare_ago_s": 1,
              "shares": 8904402,
              "bestshare": 129665135,
              "bestever": 129665135,
              "current_diff": 15814,
          }]
        }

    Note: address-based config is ignored on AxeBCH2 — the API is single-tenant.
    Only base_url is required.
    """
    if not cfg.get("enabled") or not cfg.get("base_url"):
        return []
    url = f"{cfg['base_url'].rstrip('/')}/api/pool/workers"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[pool:axebch2] {url} → HTTP {r.status_code}")
            return []
        d = r.json()
        out = []
        for w in (d.get("workers_details") or []):
            # workername is "<address>.<label>" — strip address prefix for cleaner display
            full_name = w.get("workername", "")
            display_name = full_name.split(".", 1)[1] if "." in full_name else full_name
            # Convert TH/s → GH/s for consistent display with CKPool
            hr_ths = w.get("hashrate_ths") or 0
            hr_ghs = hr_ths * 1000 if hr_ths else 0
            out.append({
                "worker_name": display_name,
                "hashrate_ghs": hr_ghs,
                "best_diff": str(w.get("bestever") or w.get("bestshare") or ""),
                "last_seen": w.get("lastshare"),
                "raw": w,
            })
        if not out:
            print(f"[pool:axebch2] {url} → 200 OK but workers_details was empty")
        return out
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"[pool:axebch2] {url} → error: {e}")
        return []


def fetch_unmineable(cfg):
    """unMineable API — alt-coin auto-exchange pool.
    Endpoint: https://api.unminable.com/v4/address/<addr>?coin=KAS
    Returns aggregated hashrate + balance + worker breakdown.
    """
    if not cfg.get("enabled") or not cfg.get("address"):
        return []
    coin = cfg.get("coin", "KAS")
    addr = quote(cfg["address"], safe="")  # kaspa:qz... has a colon
    url = f"https://api.unminable.com/v4/address/{addr}?coin={coin}"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[pool:unmineable] {url} → HTTP {r.status_code}")
            return []
        d = r.json()
        if not d.get("success"):
            print(f"[pool:unmineable] {url} → success=false")
            return []
        data = d.get("data") or {}
        out = []
        for w in (data.get("miners") or []):
            out.append({
                "worker_name": w.get("worker"),
                "hashrate_ghs": w.get("reported_hashrate"),
                "best_diff": "",
                "last_seen": w.get("last_seen"),
                "raw": {**w, "_coin": coin, "_balance": data.get("balance")},
            })
        if not out and data.get("reported_hashrate") is not None:
            out.append({
                "worker_name": f"{coin}-aggregate",
                "hashrate_ghs": data.get("reported_hashrate"),
                "best_diff": "",
                "last_seen": None,
                "raw": {"_coin": coin, "_balance": data.get("balance"), "_aggregate": True},
            })
        return out
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"[pool:unmineable] {url} → error: {e}")
        return []


def refresh_pool_workers(conn, cfg):
    """Fetch all enabled pools and replace pool_workers contents."""
    pools_cfg = cfg.get("pools") or {}
    fetchers = {
        "ckpool":     fetch_ckpool,
        "axebch2":    fetch_axebch2,
        "unmineable": fetch_unmineable,
    }
    now = datetime.now(timezone.utc).isoformat()

    # Wipe and replace — pool worker lists are inherently a snapshot
    conn.execute("DELETE FROM pool_workers")

    for key, fetcher in fetchers.items():
        pcfg = pools_cfg.get(key, {})
        if not pcfg.get("enabled"):
            continue
        try:
            workers = fetcher(pcfg)
            for w in workers:
                conn.execute(
                    """INSERT INTO pool_workers
                       (pool_key, worker_name, hashrate_ghs, best_diff, last_seen, fetched_at, raw_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (key, w.get("worker_name"), w.get("hashrate_ghs"),
                     w.get("best_diff"), w.get("last_seen"), now,
                     json.dumps(w.get("raw") or {})[:4000]),
                )
        except Exception as e:
            print(f"[pool:{key}] fetch failed: {e}")




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
            shares_accepted, shares_rejected, best_diff, pool_url, worker, uptime_s, raw_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            sample.get("uptime_s"),
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

        # 5. Refresh pool cross-references (separate from miner polling)
        try:
            refresh_pool_workers(conn, cfg)
        except Exception as e:
            errors.append(f"pools: {e}")

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
