"""
btc_price_ingest.py — Continuously collect real Bitcoin price data.

Data Sources:
  Primary: CoinGecko API (free, no key required)
  Fallback: Coinbase API

Table: btc_prices
  id, timestamp, price, source

Polling: every 30 seconds
"""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINBASE_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

CREATE_BTC_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS btc_prices (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    price       DOUBLE PRECISION NOT NULL,
    source      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_btc_prices_ts ON btc_prices (timestamp);
"""


def ensure_btc_prices_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_BTC_PRICES_SQL)
    conn.commit()


def _fetch_coingecko():
    """Fetch BTC/USD from CoinGecko. Returns (price, 'coingecko') or None."""
    try:
        resp = requests.get(
            COINGECKO_URL,
            params={"ids": "bitcoin", "vs_currencies": "usd"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        price = float(data["bitcoin"]["usd"])
        return price, "coingecko"
    except Exception as e:
        logger.warning("CoinGecko failed: %s", e)
        return None


def _fetch_coinbase():
    """Fetch BTC/USD from Coinbase. Returns (price, 'coinbase') or None."""
    try:
        resp = requests.get(COINBASE_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        price = float(data["data"]["amount"])
        return price, "coinbase"
    except Exception as e:
        logger.warning("Coinbase failed: %s", e)
        return None


def run_btc_price_ingest(conn):
    """
    Fetch BTC price from primary (CoinGecko) or fallback (Coinbase).
    Insert a new row into btc_prices. Never overwrites.
    Returns (price, source) or None if both fail.
    """
    ensure_btc_prices_table(conn)

    result = _fetch_coingecko()
    fallback_used = False

    if result is None:
        result = _fetch_coinbase()
        fallback_used = True

    if result is None:
        logger.error("All BTC price sources failed")
        return None

    price, source = result
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO btc_prices (timestamp, price, source) VALUES (%s, %s, %s)",
            (now, price, source),
        )
    conn.commit()

    if fallback_used:
        logger.info("BTC price: $%.2f (source: %s, FALLBACK)", price, source)
    else:
        logger.info("BTC price: $%.2f (source: %s)", price, source)

    return price, source


if __name__ == "__main__":
    import os, psycopg2
    logging.basicConfig(level=logging.INFO)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    run_btc_price_ingest(conn)
    conn.close()
