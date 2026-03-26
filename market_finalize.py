"""
market_finalize.py — Capture final outcomes for expired markets.

Updated: determines resolved_outcome from final prices (never NULL when possible),
adds columns for resolution via market_resolution.py.
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

CREATE_OUTCOMES_SQL = """
CREATE TABLE IF NOT EXISTS market_outcomes (
    market_id              TEXT PRIMARY KEY REFERENCES tracked_markets(market_id),
    final_price_yes        DOUBLE PRECISION,
    final_price_no         DOUBLE PRECISION,
    resolved_outcome       TEXT,
    resolution_time        TIMESTAMPTZ,
    btc_price_at_resolution DOUBLE PRECISION,
    btc_price_start        DOUBLE PRECISION,
    btc_price_end          DOUBLE PRECISION,
    true_outcome           TEXT,
    polymarket_outcome     TEXT,
    confidence_gap         DOUBLE PRECISION
);
"""

ALTER_OUTCOMES_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='btc_price_at_resolution') THEN
        ALTER TABLE market_outcomes ADD COLUMN btc_price_at_resolution DOUBLE PRECISION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='btc_price_start') THEN
        ALTER TABLE market_outcomes ADD COLUMN btc_price_start DOUBLE PRECISION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='btc_price_end') THEN
        ALTER TABLE market_outcomes ADD COLUMN btc_price_end DOUBLE PRECISION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='true_outcome') THEN
        ALTER TABLE market_outcomes ADD COLUMN true_outcome TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='polymarket_outcome') THEN
        ALTER TABLE market_outcomes ADD COLUMN polymarket_outcome TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='market_outcomes' AND column_name='confidence_gap') THEN
        ALTER TABLE market_outcomes ADD COLUMN confidence_gap DOUBLE PRECISION;
    END IF;
END
$$;
"""


def ensure_outcomes_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_OUTCOMES_SQL)
        cur.execute(ALTER_OUTCOMES_SQL)
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
    except Exception:
        pass
    return None, None


def _determine_outcome(price_yes, price_no):
    """
    Determine resolved outcome. Uses thresholds to ensure non-NULL:
      >= 0.90 → YES
      <= 0.10 → NO
      Otherwise: YES if price_yes > price_no, else NO
    NEVER returns None — always makes a determination.
    """
    if price_yes is None:
        return "UNKNOWN"
    if price_yes >= 0.90:
        return "YES"
    if price_yes <= 0.10:
        return "NO"
    # For ambiguous cases, pick the higher side
    if price_no is not None:
        return "YES" if price_yes > price_no else "NO"
    return "YES" if price_yes > 0.5 else "NO"


def run_finalize(conn):
    """Finalize expired active markets. Returns count finalized."""
    ensure_outcomes_table(conn)
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT market_id FROM tracked_markets WHERE is_active = TRUE AND end_date < %s",
            (now,),
        )
        expired_ids = [r[0] for r in cur.fetchall()]

    if not expired_ids:
        logger.info("No expired active markets to finalize")
        return 0

    with conn.cursor() as cur:
        cur.execute("SELECT market_id FROM market_outcomes WHERE market_id = ANY(%s)", (expired_ids,))
        already_finalized = {r[0] for r in cur.fetchall()}

    to_finalize = [mid for mid in expired_ids if mid not in already_finalized]
    if not to_finalize:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tracked_markets SET is_active = FALSE WHERE market_id = ANY(%s)",
                (expired_ids,),
            )
        conn.commit()
        logger.info("All %d expired markets already finalized, deactivated", len(expired_ids))
        return 0

    finalized = 0
    with conn.cursor() as cur:
        for mid in to_finalize:
            try:
                resp = requests.get(f"{GAMMA_URL}/{mid}", timeout=15)
                if resp.status_code == 404:
                    logger.warning("Market %s not found on API", mid)
                    continue
                resp.raise_for_status()
                m = resp.json()
            except Exception as e:
                logger.error("Error fetching market %s: %s", mid, e)
                continue

            price_yes, price_no = _parse_prices(m.get("outcomePrices"))
            outcome = _determine_outcome(price_yes, price_no)

            cur.execute(
                """
                INSERT INTO market_outcomes (market_id, final_price_yes, final_price_no, resolved_outcome, resolution_time)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO NOTHING
                """,
                (mid, price_yes, price_no, outcome, now),
            )
            cur.execute(
                "UPDATE tracked_markets SET is_active = FALSE WHERE market_id = %s",
                (mid,),
            )
            finalized += 1

    conn.commit()
    logger.info("Finalized %d markets", finalized)
    return finalized


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    count = run_finalize(conn)
    print(f"Finalized {count} markets")
    conn.close()
