import os
from dotenv import load_dotenv

load_dotenv()

PORT = int(os.getenv("PORT", 8080))
POLYMARKET_API_URL = os.getenv("POLYMARKET_API_URL", "https://clob.polymarket.com")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
AI_PROXY_URL = f"{SUPABASE_URL}/functions/v1/ai-proxy" if SUPABASE_URL else ""
DEFAULT_AI_MODEL = "google/gemini-3-flash-preview"
