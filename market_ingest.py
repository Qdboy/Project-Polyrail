"""
market_ingest.py — Collect time-series snapshots for all tracked markets.

Updated: replaces 'spread' with 'confidence' and 'imbalance' metrics.

  confidence = max(price_yes, price_no)  — 0.5 = uncertain, 1.0 = certain
  imbalance  = price_yes - price_no      — directional bias (-1 to +1)
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

CREATE_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS market_snapshots (
    id          SERIAL PRIMARY KEY,
    market_id   TEXT NOT NULL REFERENCES tracked_markets(market_id),
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    price_yes   DOUBLE PRECISION,
    price_no    DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    liquidity   DOUBLE PRECISION,
    spread      DOUBLE PRECISION,
    confidence  DOUBLE PRECISION,
    imbalance   DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_snapshots_market_ts
    ON market_snapshots (market_id, timestamp);
"""

ALTER_SNAPSHOTS_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_snapshots' AND column_name='confidence') THEN
        ALTER TABLE market_snapshots ADD COLUMN confidence DOUBLE PRECISION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_snapshots' AND column_name='imbalance') THEN
        ALTER TABLE market_snapshots ADD COLUMN imbalance DOUBLE PRECISION;
    END IF;
END
$$;
"""


def ensure_snapshots_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_SNAPSHOTS_SQL)
        cur.execute(ALTER_SNAPSHOTS_SQL)
    conn.commit()


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_prices(outcome_prices_str):
    if not outcome_prices_str:
        return None, None
    try:
        import json
        prices = json.loads(outcome_prices_str)
        if len(prices) >= 2:
            return _safe_float(prices[0]), _safe_float(prices[1])
        elif len(prices) == 1:
            return _safe_float(prices[0]), None
    except Exception:
        pass
    return None, None


def _compute_spread(price_yes, price_no):
    if price_yes is None or price_no is None:
        return None
    return abs(price_yes - (1.0 - price_no))


def _compute_confidence(price_yes, price_no):
    if price_yes is None or price_no is None:
        return None
    return max(price_yes, price_no)


def _compute_imbalance(price_yes, price_no):
    if price_yes is None or price_no is None:
        return None
    return price_yes - price_no


def run_ingest(conn):
    """
    Fetch snapshots for all tracked markets and insert into market_snapshots.
    Returns the number of rows inserted.
    """
    ensure_snapshots_table(conn)

    with conn.cursor() as cur:
        cur.execute("SELECT market_id FROM tracked_markets")
        tracked_ids = {r[0] for r in cur.fetchall()}

    if not tracked_ids:
        logger.info("No tracked markets, skipping ingest")
        return 0

    fetched = {}
    offset = 0
    limit = 100
    while True:
        params = {"limit": limit, "offset": offset}
        resp = requests.get(GAMMA_URL, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for m in batch:
            mid = m.get("id")
            if mid in tracked_ids:
                fetched[mid] = m
        offset += limit
        if len(fetched) >= len(tracked_ids):
            break

    missing = tracked_ids - set(fetched.keys())
    if missing:
        logger.warning("Missing %d tracked markets from API: %s", len(missing), list(missing)[:5])

    now = datetime.now(timezone.utc)
    inserted = 0
    with conn.cursor() as cur:
        for mid, m in fetched.items():
            price_yes, price_no = _parse_prices(m.get("outcomePrices"))
            if price_yes is None:
                continue

            volume = _safe_float(m.get("volume"))
            liquidity = _safe_float(m.get("liquidity"))
            spread = _compute_spread(price_yes, price_no)
            confidence = _compute_confidence(price_yes, price_no)
            imbalance = _compute_imbalance(price_yes, price_no)

            cur.execute(
                """
                INSERT INTO market_snapshots
                    (market_id, timestamp, price_yes, price_no, volume, liquidity, spread, confidence, imbalance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (mid, now, price_yes, price_no, volume, liquidity, spread, confidence, imbalance),
            )
            inserted += 1

    conn.commit()
    logger.info("Ingested %d snapshots (%d missing from API)", inserted, len(missing))
    return inserted


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    count = run_ingest(conn)
    print(f"Inserted {count} snapshots")
    conn.close()
