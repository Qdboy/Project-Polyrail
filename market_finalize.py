"""
market_finalize.py — Capture final outcomes for expired markets.

Responsibilities:
  - Find tracked markets where end_date has passed and is_active = TRUE
  - Fetch final data from Polymarket
  - Insert into market_outcomes (once per market)
  - Set is_active = FALSE in tracked_markets
  - Never delete history

Usage:
  from market_finalize import run_finalize
  count = run_finalize(conn)
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

CREATE_OUTCOMES_SQL = """
CREATE TABLE IF NOT EXISTS market_outcomes (
    market_id         TEXT PRIMARY KEY REFERENCES tracked_markets(market_id),
    final_price_yes   DOUBLE PRECISION,
    final_price_no    DOUBLE PRECISION,
    resolved_outcome  TEXT,
    resolution_time   TIMESTAMPTZ
);
"""


def ensure_outcomes_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_OUTCOMES_SQL)
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
    """Infer resolved outcome from final prices (near 1.0 = YES, near 0.0 = NO)."""
    if price_yes is None:
        return None
    if price_yes >= 0.95:
        return "YES"
    elif price_yes <= 0.05:
        return "NO"
    return None  # unresolved or ambiguous


def run_finalize(conn):
    """
    Finalize expired active markets.
    Returns the number of markets finalized.
    """
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

    # Check which ones are already in market_outcomes
    with conn.cursor() as cur:
        cur.execute("SELECT market_id FROM market_outcomes WHERE market_id = ANY(%s)", (expired_ids,))
        already_finalized = {r[0] for r in cur.fetchall()}

    to_finalize = [mid for mid in expired_ids if mid not in already_finalized]
    if not to_finalize:
        # Still deactivate them
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tracked_markets SET is_active = FALSE WHERE market_id = ANY(%s)",
                (expired_ids,),
            )
        conn.commit()
        logger.info("All %d expired markets already finalized, deactivated", len(expired_ids))
        return 0

    # Fetch market data individually (they may be closed/delisted from bulk)
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

            if outcome is None:
                logger.warning("Market %s: could not determine resolution (yes=%.3f)",
                               mid, price_yes or 0)

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
