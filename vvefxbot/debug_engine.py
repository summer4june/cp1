import pandas as pd
from datetime import datetime, timezone
from modules.accumulationengine import AccumulationEngine, State
import logging

logging.basicConfig(level=logging.INFO)

df = pd.read_csv("backtest/data/US500m_M1.csv", sep="\t", encoding="utf-16")
df.columns = [c.strip().lstrip("<").rstrip(">").lower() for c in df.columns]

# Check columns
print(df.columns)
print(df.head())

if "date" in df.columns and "time" in df.columns:
    df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
elif "time" in df.columns:
    df["datetime"] = pd.to_datetime(df["time"], utc=True)

# Select a subset, say 10000 candles
df = df.head(10000)

engine = AccumulationEngine("US500m")
print(f"Feeding {len(df)} candles...")

signals_emitted = 0
invalid_count = 0
for i, row in df.iterrows():
    prev_state = engine.state_data.state
    engine.process_candle(
        float(row['open']), float(row['high']), float(row['low']), float(row['close']), 
        row['datetime'], 
        is_in_macro=True, window_name="Macro 3"
    )
    if prev_state != engine.state_data.state:
        if engine.state_data.state == State.INVALID:
            invalid_count += 1
        elif engine.state_data.state in [State.MANIPULATION_CONFIRMED, State.MSS, State.DISTRIBUTION]:
            print(f"State transition: {prev_state.name} -> {engine.state_data.state.name} at {row['datetime']}")
    if engine.state_data.state == State.DISTRIBUTION:
        signals_emitted += 1
        engine._transition(State.RESET)

print(f"Total AMD cycles reached DISTRIBUTION: {signals_emitted}")
print(f"Total AMD cycles marked INVALID: {invalid_count}")
