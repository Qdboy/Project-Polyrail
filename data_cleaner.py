"""
data_cleaner.py — Prepare clean datasets from market data for training/backtesting.

Responsibilities:
  - Join market_snapshots + tracked_markets + market_outcomes
  - Remove rows with missing values
  - Normalize timestamps
  - Feature engineering: price_change, volatility, time_to_expiry, liquidity trends
  - Output clean CSV

Usage:
  from data_cleaner import export_clean_dataset
  export_clean_dataset(conn, "/path/to/output.csv")
"""

import csv
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

QUERY = """
SELECT
    s.market_id,
    t.question,
    t.end_date,
    s.timestamp,
    s.price_yes,
    s.price_no,
    s.volume,
    s.liquidity,
    s.spread,
    o.final_price_yes,
    o.final_price_no,
    o.resolved_outcome,
    o.resolution_time
FROM market_snapshots s
JOIN tracked_markets t ON s.market_id = t.market_id
LEFT JOIN market_outcomes o ON s.market_id = o.market_id
WHERE s.price_yes IS NOT NULL
  AND s.price_no IS NOT NULL
  AND s.volume IS NOT NULL
  AND s.liquidity IS NOT NULL
ORDER BY s.market_id, s.timestamp
"""


def export_clean_dataset(conn, output_path):
    """
    Query, compute features, and write a clean CSV.
    Returns the number of rows written.
    """
    with conn.cursor() as cur:
        cur.execute(QUERY)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        logger.warning("No data to export")
        return 0

    # Group by market_id for feature engineering
    from collections import defaultdict
    market_rows = defaultdict(list)
    for row in rows:
        row_dict = dict(zip(columns, row))
        market_rows[row_dict["market_id"]].append(row_dict)

    output_columns = [
        "market_id", "question", "end_date", "timestamp",
        "price_yes", "price_no", "volume", "liquidity", "spread",
        "price_change", "volatility_5", "time_to_expiry_minutes",
        "liquidity_change",
        "final_price_yes", "final_price_no", "resolved_outcome",
    ]

    total_written = 0
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_columns)
        writer.writeheader()

        for mid, snapshots in market_rows.items():
            # Sort by timestamp
            snapshots.sort(key=lambda r: r["timestamp"])

            for i, snap in enumerate(snapshots):
                # price_change from previous snapshot
                if i > 0:
                    prev_yes = snapshots[i - 1]["price_yes"]
                    price_change = snap["price_yes"] - prev_yes
                else:
                    price_change = 0.0

                # Rolling volatility (std of last 5 price_yes values)
                window = [s["price_yes"] for s in snapshots[max(0, i - 4):i + 1]]
                if len(window) >= 2:
                    mean = sum(window) / len(window)
                    variance = sum((x - mean) ** 2 for x in window) / len(window)
                    volatility_5 = variance ** 0.5
                else:
                    volatility_5 = 0.0

                # Time to expiry
                end_dt = snap["end_date"]
                if hasattr(end_dt, "timestamp"):
                    ts = snap["timestamp"]
                    tte_seconds = (end_dt - ts).total_seconds()
                    tte_minutes = max(0, tte_seconds / 60.0)
                else:
                    tte_minutes = None

                # Liquidity change
                if i > 0 and snapshots[i - 1]["liquidity"]:
                    liq_change = snap["liquidity"] - snapshots[i - 1]["liquidity"]
                else:
                    liq_change = 0.0

                writer.writerow({
                    "market_id": mid,
                    "question": snap["question"],
                    "end_date": snap["end_date"],
                    "timestamp": snap["timestamp"],
                    "price_yes": snap["price_yes"],
                    "price_no": snap["price_no"],
                    "volume": snap["volume"],
                    "liquidity": snap["liquidity"],
                    "spread": snap["spread"],
                    "price_change": round(price_change, 6),
                    "volatility_5": round(volatility_5, 6),
                    "time_to_expiry_minutes": round(tte_minutes, 2) if tte_minutes is not None else "",
                    "liquidity_change": round(liq_change, 4),
                    "final_price_yes": snap.get("final_price_yes", ""),
                    "final_price_no": snap.get("final_price_no", ""),
                    "resolved_outcome": snap.get("resolved_outcome", ""),
                })
                total_written += 1

    logger.info("Exported %d rows to %s", total_written, output_path)
    return total_written


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    export_clean_dataset(conn, "clean_dataset.csv")
    conn.close()
