"""
AI Client — calls the Lovable AI proxy edge function.
No API keys needed here; the edge function handles auth via LOVABLE_API_KEY.
"""
import requests
from config import AI_PROXY_URL, SUPABASE_ANON_KEY, DEFAULT_AI_MODEL


def call_ai(messages: list[dict], model: str = None, system: str = None) -> str:
    """Send messages to the Lovable AI proxy and return the assistant reply."""
    if not AI_PROXY_URL:
        raise RuntimeError("SUPABASE_URL not configured — cannot reach AI proxy")

    payload = {
        "messages": messages,
        "model": model or DEFAULT_AI_MODEL,
    }
    if system:
        payload["system"] = system

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    }

    resp = requests.post(AI_PROXY_URL, json=payload, headers=headers, timeout=60)

    if resp.status_code == 429:
        raise RuntimeError("AI rate-limited — retry after a short delay")
    if resp.status_code == 402:
        raise RuntimeError("AI credits exhausted — add funds in Lovable Settings")
    resp.raise_for_status()

    data = resp.json()
    return data["choices"][0]["message"]["content"]
