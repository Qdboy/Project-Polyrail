"""
BaseAgent — abstract base class for all MAS subagents.
Each agent implements `execute()` and returns a structured dict.
"""
from abc import ABC, abstractmethod
from services.ai_client import call_ai


class BaseAgent(ABC):
    """Abstract base for every subagent in the MAS."""

    name: str = "BaseAgent"
    description: str = ""

    def __init__(self):
        self.last_result = None

    @abstractmethod
    def execute(self, context: dict) -> dict:
        """Run the agent's core logic. Must return a result dict."""
        ...

    def ask_ai(self, prompt: str, system: str = None, model: str = None) -> str:
        """Convenience wrapper around the AI proxy."""
        return call_ai(
            messages=[{"role": "user", "content": prompt}],
            system=system,
            model=model,
        )

    def __repr__(self):
        return f"<{self.name}>"
