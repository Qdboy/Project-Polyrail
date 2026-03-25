import requests
import json
from datetime import datetime, timezone, timedelta

GAMMA_URL = "https://gamma-api.polymarket.com/markets"

def fetch_bitcoin_markets():
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=24)

    all_markets = []
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
        response = requests.get(GAMMA_URL, params=params)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        for market in batch:
            end_date_str = market.get("endDate")
            if not end_date_str:
                continue
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            # Stop paginating if we've passed the cutoff
            if end_date > cutoff:
                return all_markets

            question = (market.get("question") or "").lower()
            group_title = (market.get("groupItemTitle") or "").lower()
            if now < end_date <= cutoff and ("bitcoin" in question or "btc" in question or "bitcoin" in group_title or "btc" in group_title):
                all_markets.append({
                    "id": market.get("id"),
                    "question": market.get("question"),
                    "slug": market.get("slug"),
                    "conditionId": market.get("conditionId"),
                    "outcomes": market.get("outcomes"),
                    "outcomePrices": market.get("outcomePrices"),
                    "startDate": market.get("startDate"),
                    "endDate": end_date_str,
                    "active": market.get("active"),
                    "closed": market.get("closed"),
                    "liquidity": market.get("liquidity"),
                    "volume": market.get("volume"),
                    "volume24hr": market.get("volume24hr"),
                    "bestAsk": market.get("bestAsk"),
                    "bestBid": market.get("bestBid"),
                    "lastTradePrice": market.get("lastTradePrice"),
                    "spread": market.get("spread"),
                })

        offset += limit

    return all_markets

if __name__ == "__main__":
    markets = fetch_bitcoin_markets()
    output = {
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "cutoff": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
        "totalMarkets": len(markets),
        "markets": markets,
    }
    print(json.dumps(output, indent=2))
