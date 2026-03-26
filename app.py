"""
app.py — Flask application with background workers for market discovery, ingestion, and finalization.

Workers:
  1. Discovery worker — runs every 5 minutes
  2. Ingest worker — runs every 30 seconds
  3. Finalize worker — runs every 2 minutes
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify

from market_discovery import run_discovery, ensure_tracked_table
from market_ingest import run_ingest, ensure_snapshots_table
from market_finalize import run_finalize, ensure_outcomes_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

DISCOVERY_INTERVAL = int(os.environ.get("DISCOVERY_INTERVAL_SECONDS", "300"))
INGEST_INTERVAL = int(os.environ.get("INGEST_INTERVAL_SECONDS", "30"))
FINALIZE_INTERVAL = int(os.environ.get("FINALIZE_INTERVAL_SECONDS", "120"))


def _get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/stats")
def stats():
    """Quick stats endpoint."""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tracked_markets")
            total_tracked = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM tracked_markets WHERE is_active = TRUE")
            active = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM market_snapshots")
            snapshots = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM market_outcomes")
            outcomes = cur.fetchone()[0]
        conn.close()
        return jsonify({
            "tracked_markets": total_tracked,
            "active_markets": active,
            "total_snapshots": snapshots,
            "finalized_markets": outcomes,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/export")
def export_data():
    """Trigger a dataset export and return the path."""
    from data_cleaner import export_clean_dataset
    try:
        conn = _get_conn()
        output_path = "/tmp/clean_dataset.csv"
        count = export_clean_dataset(conn, output_path)
        conn.close()
        return jsonify({"rows_exported": count, "path": output_path})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _worker_loop(name, func, interval):
    """Generic worker loop with reconnection."""
    conn = _get_conn()
    while True:
        try:
            func(conn)
        except Exception as e:
            logger.error("[%s] Error: %s", name, e)
            try:
                conn.close()
            except Exception:
                pass
            conn = _get_conn()
        time.sleep(interval)


DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # Initialize tables on startup
    init_conn = _get_conn()
    ensure_tracked_table(init_conn)
    ensure_snapshots_table(init_conn)
    ensure_outcomes_table(init_conn)
    init_conn.close()

    for name, func, interval in [
        ("discovery", run_discovery, DISCOVERY_INTERVAL),
        ("ingest", run_ingest, INGEST_INTERVAL),
        ("finalize", run_finalize, FINALIZE_INTERVAL),
    ]:
        t = threading.Thread(target=_worker_loop, args=(name, func, interval), daemon=True, name=name)
        t.start()
        logger.info("Started %s worker (interval=%ds)", name, interval)
else:
    logger.warning("DATABASE_URL not set — workers not started")


if __name__ == "__main__":
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set")
        sys.exit(1)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
