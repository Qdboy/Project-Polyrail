"""
market_resolution.py — Determine TRUE outcomes using real BTC price data.

For each expired market:
  A. Strike markets: compare BTC price at end_time vs strike_price
  B. Up/Down markets: compare BTC price at start_time vs end_time

Compares true_outcome with polymarket_outcome and stores confidence_gap.

Updates market_outcomes with:
  btc_price_at_resolution, btc_price_start, btc_price_end,
  true_outcome, polymarket_outcome, confidence_gap
"""

import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# SQL to add resolution columns to market_outcomes
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


def ensure_resolution_columns(conn):
    with conn.cursor() as cur:
        cur.execute(ALTER_OUTCOMES_SQL)
    conn.commit()


def _get_nearest_btc_price(conn, target_time, tolerance_seconds=60):
    """
    Get the BTC price closest to target_time within ±tolerance.
    Returns (price, timestamp) or (None, None).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT price, timestamp
            FROM btc_prices
            WHERE timestamp BETWEEN %s AND %s
            ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - %s)))
            LIMIT 1
            """,
            (
                target_time - timedelta(seconds=tolerance_seconds),
                target_time + timedelta(seconds=tolerance_seconds),
                target_time,
            ),
        )
        row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def _determine_polymarket_outcome(final_price_yes, final_price_no):
    """Determine Polymarket's outcome from final prices."""
    if final_price_yes is None:
        return None
    if final_price_yes > 0.5:
        return "YES"
    elif final_price_no is not None and final_price_no > 0.5:
        return "NO"
    elif final_price_yes >= 0.95:
        return "YES"
    elif final_price_yes <= 0.05:
        return "NO"
    return "UNCERTAIN"


def run_resolution(conn):
    """
    Resolve all finalized markets that haven't been resolved with BTC price yet.
    Returns (resolved_count, error_count).
    """
    ensure_resolution_columns(conn)

    with conn.cursor() as cur:
        # Get markets that have outcomes but no true_outcome yet
        cur.execute(
            """
            SELECT o.market_id, o.final_price_yes, o.final_price_no,
                   t.market_type, t.strike_price, t.comparison_type,
                   t.parsed_start_time, t.parsed_end_time, t.end_date
            FROM market_outcomes o
            JOIN tracked_markets t ON o.market_id = t.market_id
            WHERE o.true_outcome IS NULL
            """
        )
        pending = cur.fetchall()

    if not pending:
        logger.info("No markets pending resolution")
        return 0, 0

    resolved = 0
    errors = 0

    for row in pending:
        (market_id, final_yes, final_no, market_type, strike_price,
         comparison_type, parsed_start, parsed_end, end_date) = row

        btc_at_resolution = None
        btc_start = None
        btc_end = None
        true_outcome = None

        # Use parsed_end_time if available, otherwise fall back to end_date
        resolution_time = parsed_end or end_date

        if market_type == "strike" and strike_price is not None:
            # Strike market: compare BTC price at end time vs strike
            if resolution_time:
                btc_at_resolution, _ = _get_nearest_btc_price(conn, resolution_time)

            if btc_at_resolution is not None:
                if comparison_type == "above":
                    true_outcome = "YES" if btc_at_resolution > strike_price else "NO"
                elif comparison_type == "below":
                    true_outcome = "YES" if btc_at_resolution < strike_price else "NO"
                else:
                    true_outcome = "UNKNOWN"
            else:
                logger.warning("Market %s: no BTC price near %s", market_id, resolution_time)
                errors += 1
                continue

        elif market_type == "up_down":
            # Up/Down market: compare BTC at start vs end
            if parsed_start:
                btc_start, _ = _get_nearest_btc_price(conn, parsed_start)
            if resolution_time:
                btc_end, _ = _get_nearest_btc_price(conn, resolution_time)
                btc_at_resolution = btc_end

            if btc_start is not None and btc_end is not None:
                true_outcome = "UP" if btc_end > btc_start else "DOWN"
            else:
                missing = []
                if btc_start is None:
                    missing.append("start")
                if btc_end is None:
                    missing.append("end")
                logger.warning("Market %s: missing BTC price at %s", market_id, ", ".join(missing))
                errors += 1
                continue

        else:
            # Unknown market type — try to resolve just from Polymarket
            logger.warning("Market %s: unknown type '%s', skipping true resolution", market_id, market_type)
            errors += 1
            continue

        # Determine Polymarket's outcome
        polymarket_outcome = _determine_polymarket_outcome(final_yes, final_no)

        # For up/down: map polymarket YES→UP, NO→DOWN for comparison
        if market_type == "up_down" and polymarket_outcome in ("YES", "NO"):
            # YES = first outcome (Up), NO = second outcome (Down)
            polymarket_outcome = "UP" if polymarket_outcome == "YES" else "DOWN"

        # Confidence gap
        confidence_gap = None
        if true_outcome and polymarket_outcome:
            # 1.0 if they agree, gap = how wrong the market price was
            true_binary = 1.0 if true_outcome in ("YES", "UP") else 0.0
            market_price = final_yes if final_yes is not None else 0.5
            confidence_gap = abs(market_price - true_binary)

        # Log mismatches
        if true_outcome and polymarket_outcome and true_outcome != polymarket_outcome:
            logger.warning(
                "MISMATCH market %s: true=%s polymarket=%s gap=%.3f",
                market_id, true_outcome, polymarket_outcome, confidence_gap or 0,
            )

        # Update market_outcomes
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE market_outcomes
                SET btc_price_at_resolution = %s,
                    btc_price_start = %s,
                    btc_price_end = %s,
                    true_outcome = %s,
                    polymarket_outcome = %s,
                    confidence_gap = %s
                WHERE market_id = %s
                """,
                (btc_at_resolution, btc_start, btc_end,
                 true_outcome, polymarket_outcome, confidence_gap, market_id),
            )
        conn.commit()
        resolved += 1

    logger.info("Resolved %d markets, %d errors", resolved, errors)
    return resolved, errors


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    resolved, errors = run_resolution(conn)
    print(f"Resolved: {resolved}, Errors: {errors}")
    conn.close()
