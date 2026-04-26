"""
FleetSwarm — Flask web app.
Serves the dashboard UI and the JSON API consumed by the front-end.
"""
from contextlib import closing
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request

from poller import db_conn, get_status, init_db, load_config, save_config, run_one_cycle, start_background

app = Flask(__name__, template_folder="/app/templates", static_folder="/app/static")


def thresholds_for(kind, alerts_by_class):
    """Look up the right threshold set for a miner, falling back to 'unknown'."""
    return alerts_by_class.get(kind) or alerts_by_class.get("unknown") or {
        "temp_warn_c": 65, "temp_critical_c": 75, "reject_rate_warn_pct": 1.0,
    }


def compute_health(miners, cfg):
    """Roll up the fleet into a single health status with reasons.
    Now uses per-class thresholds — Avalon at 70°C is normal, Bitaxe at 70°C is warm.
    Returns: (status, reasons[]) where status in {ok, warning, critical}.
    """
    reasons = []
    status = "ok"

    now = datetime.now(timezone.utc)
    grace_min = cfg.get("offline_grace_min", 5)
    grace = timedelta(minutes=grace_min)
    alerts_by_class = cfg.get("alerts_by_class") or {}

    offline_long = 0
    hot = 0
    overheated = 0
    rejecting = 0

    for m in miners:
        # Offline check
        if not m["online"]:
            try:
                last = datetime.fromisoformat(m["last_seen"]) if m["last_seen"] else None
                if last and (now - last) > grace:
                    offline_long += 1
            except (TypeError, ValueError):
                pass
            continue

        # Per-class thresholds for this miner
        t_cfg = thresholds_for(m.get("kind"), alerts_by_class)

        # Temperature check
        t = m.get("temp_c")
        if t is not None:
            if t >= t_cfg["temp_critical_c"]:
                overheated += 1
            elif t >= t_cfg["temp_warn_c"]:
                hot += 1

        # Reject rate check (per-class threshold)
        acc = m.get("shares_accepted") or 0
        rej = m.get("shares_rejected") or 0
        total = acc + rej
        if total > 100:
            reject_pct = (rej / total) * 100
            if reject_pct >= t_cfg["reject_rate_warn_pct"]:
                rejecting += 1

    if overheated:
        status = "critical"
        reasons.append(f"{overheated} miner{'s' if overheated != 1 else ''} overheating")
    if offline_long:
        status = "critical"
        reasons.append(f"{offline_long} offline >{grace_min}m")
    if hot and status != "critical":
        status = "warning"
        reasons.append(f"{hot} running warm")
    if rejecting and status != "critical":
        status = "warning"
        reasons.append(f"{rejecting} rejecting shares")

    if not reasons:
        reasons.append("All miners healthy")

    return status, reasons


# ─── API ─────────────────────────────────────────────────────────────────────

@app.route("/api/fleet")
def api_fleet():
    """Latest sample per miner + fleet health summary."""
    cfg = load_config()

    with closing(db_conn()) as conn:
        miners = conn.execute(
            """SELECT m.id, m.ip, m.hostname, m.kind, m.online, m.last_seen,
                      s.ts, s.hashrate_ghs, s.temp_c, s.power_w, s.fan_pct,
                      s.shares_accepted, s.shares_rejected, s.best_diff,
                      s.pool_url, s.worker
               FROM miners m
               LEFT JOIN samples s ON s.id = (
                   SELECT id FROM samples WHERE miner_id = m.id
                   ORDER BY ts DESC LIMIT 1
               )
               ORDER BY m.kind, m.hostname"""
        ).fetchall()

    rows = [dict(r) for r in miners]

    # Totals — only count online miners
    total_hashrate = sum(r["hashrate_ghs"] or 0 for r in rows if r["online"])
    total_power = sum(r["power_w"] or 0 for r in rows if r["online"])
    online_count = sum(1 for r in rows if r["online"])
    efficiency = (total_power / (total_hashrate / 1000)) if total_hashrate > 0 else None

    total_accepted = sum(r["shares_accepted"] or 0 for r in rows if r["online"])
    total_rejected = sum(r["shares_rejected"] or 0 for r in rows if r["online"])
    fleet_reject_pct = None
    if total_accepted + total_rejected > 0:
        fleet_reject_pct = (total_rejected / (total_accepted + total_rejected)) * 100

    health_status, health_reasons = compute_health(rows, cfg)

    return jsonify({
        "miners": rows,
        "totals": {
            "online": online_count,
            "total": len(rows),
            "hashrate_ghs": total_hashrate,
            "hashrate_ths": total_hashrate / 1000,
            "power_w": total_power,
            "efficiency_w_per_th": efficiency,
            "shares_accepted": total_accepted,
            "shares_rejected": total_rejected,
            "reject_rate_pct": fleet_reject_pct,
        },
        "health": {
            "status": health_status,
            "reasons": health_reasons,
        },
        "alerts_by_class": cfg.get("alerts_by_class") or {},
        "status": get_status(),
        "now": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/pools")
def api_pools():
    """Pool worker cross-reference data."""
    cfg = load_config()

    with closing(db_conn()) as conn:
        rows = conn.execute(
            """SELECT pool_key, worker_name, hashrate_ghs, best_diff, last_seen, fetched_at
               FROM pool_workers
               ORDER BY pool_key, hashrate_ghs DESC"""
        ).fetchall()

    # Group by pool
    by_pool = {}
    for r in rows:
        by_pool.setdefault(r["pool_key"], []).append(dict(r))

    # Reflect config so the front-end knows labels and which slots are configured
    pools_cfg = cfg.get("pools") or {}
    out = []
    for key in ("ckpool", "axebch2", "unmineable"):
        pcfg = pools_cfg.get(key, {})
        out.append({
            "key": key,
            "label": pcfg.get("label", key),
            "enabled": pcfg.get("enabled", False),
            "configured": bool(pcfg.get("base_url") or pcfg.get("address")),
            "coin": pcfg.get("coin"),
            "workers": by_pool.get(key, []),
        })
    return jsonify({"pools": out})


@app.route("/api/miner/<int:miner_id>/history")
def api_miner_history(miner_id):
    hours = int(request.args.get("hours", 24))
    with closing(db_conn()) as conn:
        rows = conn.execute(
            """SELECT ts, hashrate_ghs, temp_c, power_w
               FROM samples
               WHERE miner_id = ?
                 AND ts >= datetime('now', ?)
               ORDER BY ts ASC""",
            (miner_id, f"-{hours} hours"),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        new_cfg = request.get_json(force=True)
        save_config(new_cfg)
        return jsonify({"ok": True})
    return jsonify(load_config())


@app.route("/api/poll-now", methods=["POST"])
def api_poll_now():
    found, errors = run_one_cycle()
    return jsonify({"found": len(found), "errors": errors})


@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "status": get_status()})


# ─── UI ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/config")
def config_page():
    return render_template("config.html")


# ─── Startup ─────────────────────────────────────────────────────────────────

def main():
    init_db()
    start_background()
    app.run(host="0.0.0.0", port=8888, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
