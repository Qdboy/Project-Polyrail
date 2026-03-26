"""
market_discovery.py — Discover and track Bitcoin-related Polymarket markets.

Responsibilities:
  - Fetch markets from Polymarket Gamma API
  - Filter by BTC/Bitcoin keywords, not closed, ending within 24h, liquidity > threshold
  - INSERT new markets into tracked_markets
  - UPDATE last_seen for existing markets
  - Never delete markets once added

Usage:
  from market_discovery import run_discovery
  new_ids, all_ids = run_discovery(conn)
"""

import logging
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
MIN_LIQUIDITY = 1000  # configurable threshold

CREATE_TRACKED_MARKETS_SQL = """
CREATE TABLE IF NOT EXISTS tracked_markets (
    market_id   TEXT PRIMARY KEY,
    question    TEXT,
    end_date    TIMESTAMPTZ,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE
);
"""


def ensure_tracked_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TRACKED_MARKETS_SQL)
    conn.commit()


def _fetch_candidate_markets():
    """Paginate through Polymarket API and return BTC markets ending within 24h."""
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=24)
    candidates = []
    offset = 0
    limit = 100

    while True:
        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
            "order": "endDate",
            "ascending": "true",
        }
        resp = requests.get(GAMMA_URL, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        for m in batch:
            end_str = m.get("endDate")
            if not end_str:
                continue
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except ValueError:
                continue

            # Stop paginating once we pass the 24h window
            if end_dt > cutoff:
                return candidates

            question = (m.get("question") or "").lower()
            group_title = (m.get("groupItemTitle") or "").lower()
            liq = 0
            try:
                liq = float(m.get("liquidity") or 0)
            except (ValueError, TypeError):
                pass

            is_btc = "bitcoin" in question or "btc" in question or \
                     "bitcoin" in group_title or "btc" in group_title

            if now < end_dt <= cutoff and is_btc and liq > MIN_LIQUIDITY:
                candidates.append({
                    "market_id": m.get("id"),
                    "question": m.get("question"),
                    "end_date": end_str,
                })

        offset += limit

    return candidates


def run_discovery(conn):
    """
    Discover BTC markets and upsert into tracked_markets.

    Returns:
        (new_ids, all_tracked_ids) — lists of market_id strings
    """
    ensure_tracked_table(conn)
    candidates = _fetch_candidate_markets()
    logger.info("Discovered %d candidate markets", len(candidates))

    new_ids = []
    with conn.cursor() as cur:
        for c in candidates:
            # Try INSERT, on conflict just update last_seen
            cur.execute(
                """
                INSERT INTO tracked_markets (market_id, question, end_date, first_seen, last_seen, is_active)
                VALUES (%s, %s, %s, now(), now(), TRUE)
                ON CONFLICT (market_id) DO UPDATE
                    SET last_seen = now()
                RETURNING (xmax = 0) AS is_new
                """,
                (c["market_id"], c["question"], c["end_date"]),
            )
            row = cur.fetchone()
            if row and row[0]:  # xmax=0 means INSERT happened
                new_ids.append(c["market_id"])

        # Fetch all tracked ids
        cur.execute("SELECT market_id FROM tracked_markets")
        all_ids = [r[0] for r in cur.fetchall()]

    conn.commit()
    logger.info("New markets added: %d | Total tracked: %d", len(new_ids), len(all_ids))
    return new_ids, all_ids


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    new_ids, all_ids = run_discovery(conn)
    print(f"New: {len(new_ids)}, Total tracked: {len(all_ids)}")
    conn.close()
