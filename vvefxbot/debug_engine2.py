from modules.accumulationengine import AccumulationEngine, State
import pandas as pd
from datetime import datetime, timezone

df = pd.read_csv("backtest/data/US500m_M1.csv", sep="\t", encoding="utf-16")
df.columns = [c.strip().lstrip("<").rstrip(">").lower() for c in df.columns]
if "date" in df.columns and "time" in df.columns:
    df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
elif "time" in df.columns:
    df["datetime"] = pd.to_datetime(df["time"], utc=True)

df = df.head(100000)

engine = AccumulationEngine("US500m")
engine.MIN_CANDLES = 7
engine.config = {"max_acc_pips_us500": 500.0}

invalid_reasons = {}

for i, row in df.iterrows():
    prev_state = engine.state_data.state
    engine.process_candle(
        float(row['open']), float(row['high']), float(row['low']), float(row['close']), 
        row['datetime'], 
        is_in_macro=True, window_name="Macro 3"
    )
    if prev_state != engine.state_data.state:
        if engine.state_data.state == State.INVALID:
            reason = engine.state_data.invalid_reason or "unknown"
            invalid_reasons[reason] = invalid_reasons.get(reason, 0) + 1
            engine._transition(State.RESET)
        elif engine.state_data.state == State.DISTRIBUTION:
            print(f"DISTRIBUTION reached at {row['datetime']}")
            engine._transition(State.RESET)
            
print("Invalid Reasons:")
for k, v in sorted(invalid_reasons.items(), key=lambda x: x[1], reverse=True):
    print(f"{k}: {v}")
