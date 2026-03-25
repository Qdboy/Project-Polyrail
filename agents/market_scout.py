"""
Market Scout — identifies high-opportunity markets from raw Polymarket data.
"""
from agents.base import BaseAgent


class MarketScout(BaseAgent):
    name = "MarketScout"
    description = "Scans markets for high-liquidity, high-volume opportunities."

    def execute(self, context: dict) -> dict:
        markets = context.get("markets", [])
        if not markets:
            return {"status": "no_data", "opportunities": []}

        # Filter: liquidity > $10k and 24h volume > $1k
        opps = [
            m for m in markets
            if m.get("liquidity", 0) > 10000 and m.get("volume_24hr", 0) > 1000
        ]
        opps.sort(key=lambda m: m["volume_24hr"], reverse=True)

        # Optional: use AI for deeper analysis on top picks
        top = opps[:5]
        ai_summary = None
        if top:
            try:
                prompt = f"Briefly rank these prediction markets by trading opportunity. Return a 2-sentence summary per market.\n\n"
                for m in top:
                    prompt += f"- {m['question']} | Liquidity: ${m['liquidity']:,.0f} | 24h Vol: ${m['volume_24hr']:,.0f} | Spread: {m['spread']}\n"
                ai_summary = self.ask_ai(
                    prompt,
                    system="You are a quantitative prediction-market analyst. Be concise and data-driven.",
                )
            except Exception as e:
                ai_summary = f"AI analysis unavailable: {e}"

        self.last_result = {
            "status": "ok",
            "total_scanned": len(markets),
            "opportunities_found": len(opps),
            "top_opportunities": [
                {"market_id": m["market_id"], "question": m["question"],
                 "liquidity": m["liquidity"], "volume_24hr": m["volume_24hr"],
                 "spread": m["spread"]}
                for m in top
            ],
            "ai_summary": ai_summary,
        }
        return self.last_result
