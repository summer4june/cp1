from core.stateengine import StateEngine
import uuid
from datetime import datetime, timezone

state = StateEngine("test_bot.db")
signal = {
    "signal_id": str(uuid.uuid4()),
    "pair": "GBPJPYm",
    "session": "London",
    "killzone": "London KZ",
    "entry_leg": "C",
    "entry_mode": "FILTER",
    "timeframe_bias": "D1",
    "timeframe_entry": "M15",
    "direction": "SELL",
    "bias_summary": "Test",
    "entry_price": 1.0,
    "sl_price": 1.1,
    "tp1_price": 0.9,
    "tp2_price": 0.8,
    "tp3_price": 0.7,
    "sl_pips": 10.0,
    "tp_pips": 20.0,
    "tp3_pips": 30.0,
    "spread_pips": 1.0,
    "effective_rr": 2.0,
    "score": 80.0,
    "detected_time": datetime.now(timezone.utc).isoformat(),
    "strategy": "ZGMT-C",
    "setup_type": "ZGMT-C",
    "fixed_lot_size": 0.0,
    "skip_rr_check": True,
    "position_fraction": 1.0,
}
state.insert_signal(signal)
print("Has recent:", state.has_recent_signal("GBPJPYm", "SELL", 1440))
