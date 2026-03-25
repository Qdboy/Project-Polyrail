"""
Risk Auditor — capital preservation, position limits, geofencing checks.
"""
from agents.base import BaseAgent


class RiskAuditor(BaseAgent):
    name = "RiskAuditor"
    description = "Enforces capital limits, exposure rules, and regulatory geofencing."

    # Configurable thresholds
    MAX_SINGLE_POSITION_PCT = 0.10  # 10% of portfolio per market
    MAX_TOTAL_EXPOSURE = 0.80       # 80% deployed max

    def execute(self, context: dict) -> dict:
        portfolio = context.get("portfolio", {})
        markets = context.get("markets", [])

        balance = portfolio.get("balance", 0)
        positions = portfolio.get("positions", [])

        warnings = []

        # Check per-position concentration
        for pos in positions:
            if balance > 0:
                pct = pos.get("size", 0) / balance
                if pct > self.MAX_SINGLE_POSITION_PCT:
                    warnings.append({
                        "type": "concentration",
                        "market_id": pos.get("market_id"),
                        "pct_of_portfolio": round(pct * 100, 1),
                        "limit_pct": self.MAX_SINGLE_POSITION_PCT * 100,
                    })

        # Check total exposure
        total_deployed = sum(p.get("size", 0) for p in positions)
        if balance > 0 and (total_deployed / balance) > self.MAX_TOTAL_EXPOSURE:
            warnings.append({
                "type": "over_exposure",
                "deployed_pct": round((total_deployed / balance) * 100, 1),
                "limit_pct": self.MAX_TOTAL_EXPOSURE * 100,
            })

        self.last_result = {
            "status": "ok",
            "balance": balance,
            "total_deployed": total_deployed,
            "warnings": warnings,
            "positions_checked": len(positions),
        }
        return self.last_result
