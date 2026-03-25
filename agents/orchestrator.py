"""
LeadResearcher (Orchestrator) — coordinates all subagents and synthesizes findings.
"""
from agents.base import BaseAgent
from agents.market_scout import MarketScout
from agents.orderbook_detailer import OrderbookDetailer
from agents.risk_auditor import RiskAuditor
from agents.integrity_monitor import IntegrityMonitor


class LeadResearcher(BaseAgent):
    name = "LeadResearcher"
    description = "Orchestrator that coordinates all subagents and produces a unified research brief."

    def __init__(self):
        super().__init__()
        self.agents = {
            "market_scout": MarketScout(),
            "orderbook_detailer": OrderbookDetailer(),
            "risk_auditor": RiskAuditor(),
            "integrity_monitor": IntegrityMonitor(),
        }

    def execute(self, context: dict) -> dict:
        """Run all subagents and synthesize their outputs."""
        results = {}
        errors = {}

        for name, agent in self.agents.items():
            try:
                results[name] = agent.execute(context)
            except Exception as e:
                errors[name] = str(e)
                results[name] = {"status": "error", "error": str(e)}

        # AI-powered synthesis of all agent findings
        synthesis = None
        try:
            findings_text = ""
            for name, result in results.items():
                findings_text += f"\n## {name}\n{_summarize(result)}\n"

            synthesis = self.ask_ai(
                f"Synthesize these multi-agent findings into a concise trading brief with actionable recommendations:\n{findings_text}",
                system="You are the Lead Researcher of a quantitative prediction-market trading desk. Provide a structured brief: 1) Key Opportunities, 2) Risk Alerts, 3) Integrity Flags, 4) Recommended Actions. Be specific and data-driven.",
            )
        except Exception as e:
            synthesis = f"Synthesis unavailable: {e}"

        self.last_result = {
            "status": "ok",
            "agent_results": results,
            "errors": errors,
            "synthesis": synthesis,
        }
        return self.last_result


def _summarize(result: dict) -> str:
    """Create a short text summary of an agent result for the AI."""
    lines = []
    for k, v in result.items():
        if k == "status":
            continue
        if isinstance(v, list):
            lines.append(f"  {k}: {len(v)} items")
            for item in v[:3]:
                lines.append(f"    - {item}")
        else:
            lines.append(f"  {k}: {v}")
    return "\n".join(lines) if lines else "  (empty)"
