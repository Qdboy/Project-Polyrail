"""
Polymarket CLOB API client — fetches live market data.
"""
import requests
import time
from config import POLYMARKET_API_URL

# In-memory cache
_cache = {"markets": [], "last_fetch": None, "history": {}}


def fetch_markets(limit: int = 100) -> list[dict]:
    """Fetch active markets from the Polymarket CLOB API."""
    try:
        resp = requests.get(
            f"{POLYMARKET_API_URL}/markets",
            params={"limit": limit, "active": True},
            timeout=15,
        )
        resp.raise_for_status()
        raw = resp.json()

        markets = []
        for m in raw if isinstance(raw, list) else raw.get("data", raw.get("markets", [])):
            outcomes = m.get("outcomes", "")
            prices = m.get("outcomePrices", m.get("outcome_prices", ""))

            # Parse prices for spread calc
            try:
                if isinstance(prices, str):
                    import json as _json
                    price_list = _json.loads(prices)
                else:
                    price_list = prices or []
                price_floats = [float(p) for p in price_list]
            except Exception:
                price_floats = []

            best_bid = price_floats[0] if price_floats else 0
            best_ask = 1 - best_bid if best_bid else 0
            spread = round(best_ask - best_bid, 4) if best_bid else 0

            markets.append({
                "market_id": m.get("condition_id", m.get("id", "")),
                "question": m.get("question", ""),
                "slug": m.get("slug", m.get("market_slug", "")),
                "condition_id": m.get("condition_id", ""),
                "outcomes": outcomes if isinstance(outcomes, str) else str(outcomes),
                "outcome_prices": prices if isinstance(prices, str) else str(prices),
                "start_date": m.get("startDate", m.get("start_date", "")),
                "end_date": m.get("endDate", m.get("end_date", "")),
                "active": m.get("active", True),
                "closed": m.get("closed", False),
                "liquidity": float(m.get("liquidity", 0)),
                "volume": float(m.get("volume", 0)),
                "volume_24hr": float(m.get("volume24hr", m.get("volume_24hr", 0))),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "last_trade_price": float(m.get("lastTradePrice", m.get("last_trade_price", 0))),
                "spread": spread,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

        _cache["markets"] = markets
        _cache["last_fetch"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return markets

    except Exception as e:
        print(f"[polymarket] fetch error: {e}")
        return _cache.get("markets", [])


def get_cached_markets() -> list[dict]:
    return _cache.get("markets", [])


def get_stats() -> dict:
    markets = _cache.get("markets", [])
    if not markets:
        return {
            "total_records": 0, "unique_markets": 0,
            "last_fetch": None, "first_fetch": None,
            "avg_liquidity": 0, "total_volume_24hr": 0,
        }
    ids = set(m["market_id"] for m in markets)
    liq = [m["liquidity"] for m in markets if m["liquidity"]]
    return {
        "total_records": len(markets),
        "unique_markets": len(ids),
        "last_fetch": _cache.get("last_fetch"),
        "first_fetch": markets[-1]["fetched_at"] if markets else None,
        "avg_liquidity": round(sum(liq) / len(liq), 2) if liq else 0,
        "total_volume_24hr": round(sum(m["volume_24hr"] for m in markets), 2),
    }


def get_signals() -> list[dict]:
    """Aggregate market snapshots into signal-feed entries."""
    markets = _cache.get("markets", [])
    if not markets:
        return []
    spreads = [m["spread"] for m in markets if m["spread"]]
    top_vol = max((m["volume_24hr"] for m in markets), default=0)
    return [{
        "fetched_at": _cache.get("last_fetch", ""),
        "markets_found": len(markets),
        "avg_spread": round(sum(spreads) / len(spreads), 4) if spreads else 0,
        "top_volume_24hr": round(top_vol, 2),
    }]
