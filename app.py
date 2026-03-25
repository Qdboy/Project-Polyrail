"""
Polymarket MAS — Flask API server for Railway.
Serves market data, agent analysis, and health endpoints.
"""
import time
import threading
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

from config import PORT
from services.polymarket import fetch_markets, get_cached_markets, get_stats, get_signals
from agents.orchestrator import LeadResearcher

app = Flask(__name__)
CORS(app)  # Allow Lovable frontend to call

orchestrator = LeadResearcher()
_last_analysis = {"result": None, "timestamp": None}
_analysis_lock = threading.Lock()


# ─── Scheduled Jobs ───────────────────────────────────────────────

def refresh_markets():
    """Fetch fresh market data every 60s."""
    print(f"[scheduler] Refreshing markets...")
    markets = fetch_markets(limit=100)
    print(f"[scheduler] Fetched {len(markets)} markets")


def run_analysis():
    """Run the full MAS analysis pipeline every 5 min."""
    global _last_analysis
    markets = get_cached_markets()
    if not markets:
        return
    print(f"[scheduler] Running MAS analysis on {len(markets)} markets...")
    try:
        context = {
            "markets": markets,
            "portfolio": {"balance": 10000, "positions": []},  # placeholder
        }
        result = orchestrator.execute(context)
        with _analysis_lock:
            _last_analysis = {
                "result": result,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        print("[scheduler] MAS analysis complete")
    except Exception as e:
        print(f"[scheduler] MAS analysis error: {e}")


scheduler = BackgroundScheduler()
scheduler.add_job(refresh_markets, "interval", seconds=60, next_run_time=None)
scheduler.add_job(run_analysis, "interval", minutes=5, next_run_time=None)


# ─── API Routes ───────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


@app.route("/api/markets")
def markets():
    limit = request.args.get("limit", 50, type=int)
    data = get_cached_markets()[:limit]
    return jsonify({"markets": data, "count": len(data)})


@app.route("/api/markets/<market_id>/history")
def market_history(market_id):
    # Placeholder — would query time-series DB in production
    return jsonify({"market_id": market_id, "history": []})


@app.route("/api/stats")
def stats():
    return jsonify(get_stats())


@app.route("/api/signals")
def signals():
    return jsonify({"signals": get_signals()})


@app.route("/api/analysis")
def analysis():
    """Return the latest MAS orchestrator analysis."""
    with _analysis_lock:
        if _last_analysis["result"] is None:
            return jsonify({"status": "pending", "message": "No analysis run yet. Trigger via POST /api/analysis/run"}), 202
        return jsonify(_last_analysis)


@app.route("/api/analysis/run", methods=["POST"])
def trigger_analysis():
    """Manually trigger an MAS analysis run."""
    markets = get_cached_markets()
    if not markets:
        return jsonify({"error": "No market data cached. Wait for next refresh."}), 400

    context = {
        "markets": markets,
        "portfolio": request.json or {"balance": 10000, "positions": []},
    }

    try:
        result = orchestrator.execute(context)
        global _last_analysis
        with _analysis_lock:
            _last_analysis = {
                "result": result,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        return jsonify(_last_analysis)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agents")
def agent_list():
    """List all registered agents and their last status."""
    agents_info = []
    for name, agent in orchestrator.agents.items():
        agents_info.append({
            "name": agent.name,
            "description": agent.description,
            "has_result": agent.last_result is not None,
            "last_status": agent.last_result.get("status") if agent.last_result else None,
        })
    return jsonify({"orchestrator": orchestrator.name, "agents": agents_info})


# ─── Startup ──────────────────────────────────────────────────────

if __name__ == "__main__":
    # Initial data fetch
    print("[startup] Fetching initial market data...")
    fetch_markets(limit=100)

    scheduler.start()
    print(f"[startup] Server starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
else:
    # Gunicorn: fetch data + start scheduler on import
    fetch_markets(limit=100)
    scheduler.start()
