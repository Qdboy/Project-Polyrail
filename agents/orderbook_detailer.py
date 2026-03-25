"""
Orderbook Detailer — monitors depth, spreads, and price impact.
"""
from agents.base import BaseAgent


class OrderbookDetailer(BaseAgent):
    name = "OrderbookDetailer"
    description = "Monitors orderbook depth, spreads, and detects thin liquidity."

    def execute(self, context: dict) -> dict:
        markets = context.get("markets", [])
        if not markets:
            return {"status": "no_data", "alerts": []}

        alerts = []
        for m in markets:
            spread = m.get("spread", 0)
            liquidity = m.get("liquidity", 0)

            if spread > 0.05:
                alerts.append({
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "alert_type": "wide_spread",
                    "spread": spread,
                    "liquidity": liquidity,
                })
            if 0 < liquidity < 5000:
                alerts.append({
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "alert_type": "thin_liquidity",
                    "spread": spread,
                    "liquidity": liquidity,
                })

        self.last_result = {
            "status": "ok",
            "markets_analyzed": len(markets),
            "alerts": alerts[:20],
        }
        return self.last_result
