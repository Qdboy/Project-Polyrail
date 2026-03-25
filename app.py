import os
import sys
import time
import threading
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify

from polymarket_db_ingest import run_agent, ingest, ensure_table

app = Flask(__name__)

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


def ingestion_worker(DATABASE_URL):
    """Background thread: connects to Postgres and polls Polymarket on a schedule."""
    conn = psycopg2.connect(DATABASE_URL)
    ensure_table(conn)

    while True:
        try:
            data = run_agent()
            if data:
                ingest(conn, data)
            else:
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] "
                    "No data returned from agent, skipping cycle",
                    file=sys.stderr,
                )
        except Exception as e:
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] Error during cycle: {e}",
                file=sys.stderr,
            )
            # Reconnect on DB errors
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg2.connect(DATABASE_URL)

        time.sleep(POLL_INTERVAL_SECONDS)


# Start the ingestion worker when the module loads (works with gunicorn)
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    worker = threading.Thread(
        target=ingestion_worker, args=(DATABASE_URL,), daemon=True, name="ingestion-worker"
    )
    worker.start()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Ingestion worker started")
else:
    print("WARNING: DATABASE_URL not set, ingestion worker not started", file=sys.stderr)


if __name__ == "__main__":
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable not set", file=sys.stderr)
        sys.exit(1)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
