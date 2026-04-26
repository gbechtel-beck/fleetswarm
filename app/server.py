"""
FleetSwarm — Flask web app.
Serves the dashboard UI and the JSON API consumed by the front-end.
"""
from contextlib import closing
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request

from poller import db_conn, get_status, init_db, load_config, save_config, run_one_cycle, start_background

app = Flask(__name__, template_folder="/app/templates", static_folder="/app/static")


def compute_health(miners, alerts):
    """Roll up the fleet into a single health status with reasons.
    Returns: (status, reasons[]) where status in {ok, warning, critical}.
    """
    reasons = []
    status = "ok"

    now = datetime.now(timezone.utc)
    grace = timedelta(minutes=alerts["offline_grace_min"])

    offline_long = 0
    hot = 0
    overheated = 0
    rejecting = 0

    for m in miners:
        # Offline check — only count as a fleet alert if offline > grace period
        if not m["online"]:
            try:
                last = datetime.fromisoformat(m["last_seen"]) if m["last_seen"] else None
                if last and (now - last) > grace:
                    offline_long += 1
            except (TypeError, ValueError):
                pass
            continue

        # Temperature checks (only for online miners)
        t = m.get("temp_c")
        if t is not None:
            if t >= alerts["temp_critical_c"]:
                overheated += 1
            elif t >= alerts["temp_warn_c"]:
                hot += 1

        # Reject rate check — needs minimum sample size to avoid false positives
        acc = m.get("shares_accepted") or 0
        rej = m.get("shares_rejected") or 0
        total = acc + rej
        if total > 100:
            reject_pct = (rej / total) * 100
            if reject_pct >= alerts["reject_rate_warn_pct"]:
                rejecting += 1

    if overheated:
        status = "critical"
        reasons.append(f"{overheated} miner{'s' if overheated != 1 else ''} overheating")
    if offline_long:
        status = "critical"
        reasons.append(f"{offline_long} offline >{alerts['offline_grace_min']}m")
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
    efficiency = (total_power / (total_hashrate / 1000)) if total_hashrate > 0 else None  # W/TH

    # Aggregate share stats
    total_accepted = sum(r["shares_accepted"] or 0 for r in rows if r["online"])
    total_rejected = sum(r["shares_rejected"] or 0 for r in rows if r["online"])
    fleet_reject_pct = None
    if total_accepted + total_rejected > 0:
        fleet_reject_pct = (total_rejected / (total_accepted + total_rejected)) * 100

    # Health roll-up
    health_status, health_reasons = compute_health(rows, cfg["alerts"])

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
        "thresholds": cfg["alerts"],   # so front-end can color cards consistently
        "status": get_status(),
        "now": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/miner/<int:miner_id>/history")
def api_miner_history(miner_id):
    """Hashrate + temp history for charting."""
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
