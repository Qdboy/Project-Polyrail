"""
data_exporter.py — Export clean, training-ready datasets via HTTP endpoint.

Joins: market_snapshots + tracked_markets + market_outcomes + btc_prices

Output: CSV with all relevant fields including BTC price, strike, outcomes.
"""

import csv
import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

EXPORT_QUERY = """
SELECT
    s.market_id,
    t.question,
    t.market_type,
    t.strike_price,
    t.comparison_type,
    t.end_date,
    t.parsed_start_time,
    t.parsed_end_time,
    s.timestamp,
    s.price_yes,
    s.price_no,
    s.volume,
    s.liquidity,
    s.confidence,
    s.imbalance,
    o.final_price_yes,
    o.final_price_no,
    o.btc_price_at_resolution,
    o.btc_price_start,
    o.btc_price_end,
    o.true_outcome,
    o.polymarket_outcome,
    o.confidence_gap,
    o.resolved_outcome,
    o.resolution_time,
    (SELECT bp.price FROM btc_prices bp
     WHERE bp.timestamp <= s.timestamp
     ORDER BY bp.timestamp DESC LIMIT 1) AS btc_price_at_snapshot
FROM market_snapshots s
JOIN tracked_markets t ON s.market_id = t.market_id
LEFT JOIN market_outcomes o ON s.market_id = o.market_id
WHERE s.price_yes IS NOT NULL
  AND s.price_no IS NOT NULL
ORDER BY s.market_id, s.timestamp
"""

CSV_COLUMNS = [
    "market_id", "question", "market_type", "strike_price", "comparison_type",
    "end_date", "parsed_start_time", "parsed_end_time",
    "timestamp", "price_yes", "price_no", "volume", "liquidity",
    "confidence", "imbalance", "btc_price_at_snapshot",
    "final_price_yes", "final_price_no",
    "btc_price_at_resolution", "btc_price_start", "btc_price_end",
    "true_outcome", "polymarket_outcome", "confidence_gap",
    "resolved_outcome", "resolution_time",
]


def export_csv_to_string(conn):
    """
    Run export query and return CSV as a string (for HTTP response).
    Returns (csv_string, row_count).
    """
    with conn.cursor() as cur:
        cur.execute(EXPORT_QUERY)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
    writer.writeheader()

    for row in rows:
        row_dict = dict(zip(columns, row))
        # Clean: replace None with empty string
        clean_row = {col: (row_dict.get(col, "") if row_dict.get(col) is not None else "")
                     for col in CSV_COLUMNS}
        writer.writerow(clean_row)

    csv_string = output.getvalue()
    logger.info("Exported %d rows to CSV", len(rows))
    return csv_string, len(rows)


def export_csv_to_file(conn, path):
    """Write CSV to a file path."""
    csv_string, count = export_csv_to_string(conn)
    with open(path, "w") as f:
        f.write(csv_string)
    logger.info("Wrote %d rows to %s", count, path)
    return count


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    export_csv_to_file(conn, "btc_markets_dataset.csv")
    conn.close()
