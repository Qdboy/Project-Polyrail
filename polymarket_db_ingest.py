import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone

import psycopg2

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS btc_markets (
    id SERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL,
    market_id TEXT,
    question TEXT,
    slug TEXT,
    condition_id TEXT,
    outcomes TEXT,
    outcome_prices TEXT,
    start_date TEXT,
    end_date TEXT,
    active BOOLEAN,
    closed BOOLEAN,
    liquidity DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    volume_24hr DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    last_trade_price DOUBLE PRECISION,
    spread DOUBLE PRECISION
);
"""

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()

def safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def ingest(conn, data):
    fetched_at = data["fetchedAt"]
    markets = data["markets"]
    with conn.cursor() as cur:
        for m in markets:
            cur.execute(
                """
                INSERT INTO btc_markets (
                    fetched_at, market_id, question, slug, condition_id,
                    outcomes, outcome_prices, start_date, end_date,
                    active, closed, liquidity, volume, volume_24hr,
                    best_ask, best_bid, last_trade_price, spread
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    fetched_at,
                    m.get("id"),
                    m.get("question"),
                    m.get("slug"),
                    m.get("conditionId"),
                    m.get("outcomes"),
                    m.get("outcomePrices"),
                    m.get("startDate"),
                    m.get("endDate"),
                    m.get("active"),
                    m.get("closed"),
                    safe_float(m.get("liquidity")),
                    safe_float(m.get("volume")),
                    safe_float(m.get("volume24hr")),
                    safe_float(m.get("bestAsk")),
                    safe_float(m.get("bestBid")),
                    safe_float(m.get("lastTradePrice")),
                    safe_float(m.get("spread")),
                ),
            )
    conn.commit()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Inserted {len(markets)} rows")

def run_agent():
    """Run polymarket_btc_agent.py and capture its JSON output."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    agent_path = os.path.join(script_dir, "polymarket_btc_agent.py")
    result = subprocess.run(
        [sys.executable, agent_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Agent error: {result.stderr}", file=sys.stderr)
        return None
    return json.loads(result.stdout)

def main():
    DATABASE_URL = os.environ.get("DATABASE_URL")
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    ensure_table(conn)

    poll_interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))

    while True:
        try:
            data = run_agent()
            if data:
                ingest(conn, data)
            else:
                print("No data returned from agent, skipping cycle")
        except Exception as e:
            print(f"Error during cycle: {e}", file=sys.stderr)
            # Reconnect on DB errors
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg2.connect(DATABASE_URL)
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
