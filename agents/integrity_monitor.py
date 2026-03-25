"""
Integrity Monitor — volume validation, wash-trading detection, anomaly flags.
"""
from agents.base import BaseAgent


class IntegrityMonitor(BaseAgent):
    name = "IntegrityMonitor"
    description = "Scans for wash trading, volume anomalies, and suspicious patterns."

    VOLUME_SPIKE_THRESHOLD = 5.0  # 5x average is suspicious

    def execute(self, context: dict) -> dict:
        markets = context.get("markets", [])
        if not markets:
            return {"status": "no_data", "flags": []}

        volumes = [m["volume_24hr"] for m in markets if m.get("volume_24hr", 0) > 0]
        avg_vol = sum(volumes) / len(volumes) if volumes else 0

        flags = []
        for m in markets:
            v24 = m.get("volume_24hr", 0)
            liq = m.get("liquidity", 0)

            # Volume spike detection
            if avg_vol > 0 and v24 > avg_vol * self.VOLUME_SPIKE_THRESHOLD:
                flags.append({
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "flag_type": "volume_spike",
                    "volume_24hr": v24,
                    "avg_volume": round(avg_vol, 2),
                    "multiple": round(v24 / avg_vol, 1),
                })

            # Volume-to-liquidity ratio anomaly (possible wash trading)
            if liq > 0 and v24 / liq > 10:
                flags.append({
                    "market_id": m["market_id"],
                    "question": m["question"],
                    "flag_type": "wash_trading_suspect",
                    "vol_liq_ratio": round(v24 / liq, 2),
                })

        self.last_result = {
            "status": "ok",
            "markets_scanned": len(markets),
            "flags": flags[:20],
        }
        return self.last_result
