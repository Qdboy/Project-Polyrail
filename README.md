# Polymarket MAS — Railway Backend

Multi-Agent System for prediction market analysis.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your values
python app.py
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/markets?limit=50` | GET | Cached market data |
| `/api/markets/<id>/history` | GET | Price history (placeholder) |
| `/api/stats` | GET | Aggregate statistics |
| `/api/signals` | GET | Signal feed |
| `/api/agents` | GET | List all agents + status |
| `/api/analysis` | GET | Latest MAS analysis |
| `/api/analysis/run` | POST | Trigger analysis manually |

## Architecture

```
LeadResearcher (Orchestrator)
├── MarketScout        — opportunity identification
├── OrderbookDetailer  — spread/depth monitoring
├── RiskAuditor        — capital & exposure limits
└── IntegrityMonitor   — wash trading detection
```

## Environment Variables (Railway)

| Variable | Required | Description |
|----------|----------|-------------|
| `PORT` | Auto | Set by Railway |
| `SUPABASE_URL` | Yes | Lovable Cloud URL for AI proxy |
| `SUPABASE_ANON_KEY` | Yes | Anon key for edge function calls |
| `POLYMARKET_API_URL` | No | Defaults to CLOB API |
