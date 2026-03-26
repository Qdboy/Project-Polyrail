"""
app.py — Flask application with background workers.

Workers:
  1. Discovery — every 5 minutes (includes parsing)
  2. Ingest — every 30 seconds
  3. Finalize — every 2 minutes
  4. BTC Price — every 30 seconds
  5. Resolution — every 2 minutes (after finalize)
  6. Parser backfill — once on startup
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify, Response

from market_discovery import run_discovery, ensure_tracked_table
from market_ingest import run_ingest, ensure_snapshots_table
from market_finalize import run_finalize, ensure_outcomes_table
from btc_price_ingest import run_btc_price_ingest, ensure_btc_prices_table
from market_resolution import run_resolution, ensure_resolution_columns
from market_parser import ensure_parsed_columns, backfill_unparsed
from data_exporter import export_csv_to_string

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

DISCOVERY_INTERVAL = int(os.environ.get("DISCOVERY_INTERVAL_SECONDS", "300"))
INGEST_INTERVAL = int(os.environ.get("INGEST_INTERVAL_SECONDS", "30"))
FINALIZE_INTERVAL = int(os.environ.get("FINALIZE_INTERVAL_SECONDS", "120"))
BTC_PRICE_INTERVAL = int(os.environ.get("BTC_PRICE_INTERVAL_SECONDS", "30"))
RESOLUTION_INTERVAL = int(os.environ.get("RESOLUTION_INTERVAL_SECONDS", "120"))


def _get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/stats")
def stats():
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
            cur.execute("SELECT COUNT(*) FROM btc_prices")
            btc_prices = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM market_outcomes WHERE true_outcome IS NOT NULL")
            resolved = cur.fetchone()[0]
            cur.execute("SELECT price, timestamp FROM btc_prices ORDER BY timestamp DESC LIMIT 1")
            latest_btc = cur.fetchone()
        conn.close()
        return jsonify({
            "tracked_markets": total_tracked,
            "active_markets": active,
            "total_snapshots": snapshots,
            "finalized_markets": outcomes,
            "resolved_with_btc": resolved,
            "btc_price_records": btc_prices,
            "latest_btc_price": latest_btc[0] if latest_btc else None,
            "latest_btc_time": str(latest_btc[1]) if latest_btc else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/export/csv")
def export_csv():
    """Download the full training dataset as CSV."""
    try:
        conn = _get_conn()
        csv_string, count = export_csv_to_string(conn)
        conn.close()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"btc_markets_dataset_{timestamp}.csv"

        return Response(
            csv_string,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/export")
def export_data():
    """Legacy export endpoint."""
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
    init_conn = _get_conn()
    ensure_tracked_table(init_conn)
    ensure_snapshots_table(init_conn)
    ensure_outcomes_table(init_conn)
    ensure_btc_prices_table(init_conn)
    ensure_parsed_columns(init_conn)
    ensure_resolution_columns(init_conn)

    # Backfill parsing for any existing unparsed markets
    backfill_unparsed(init_conn)
    init_conn.close()

    for name, func, interval in [
        ("discovery", run_discovery, DISCOVERY_INTERVAL),
        ("ingest", run_ingest, INGEST_INTERVAL),
        ("finalize", run_finalize, FINALIZE_INTERVAL),
        ("btc_price", run_btc_price_ingest, BTC_PRICE_INTERVAL),
        ("resolution", run_resolution, RESOLUTION_INTERVAL),
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
