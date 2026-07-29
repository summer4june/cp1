import os
import json
from datetime import datetime, timezone
import pandas as pd
from core.configengine import ConfigEngine
from backtest.connector import BacktestConnector
from modules.scannermacro import ScannerMacro
from core.stateengine import StateEngine
import backtest

config_engine = ConfigEngine("config.json")
config = config_engine.get_config()

config.macro_strategy["enabled"] = True
config.macro_strategy["pairs"] = ["US500m"]
config.macro_strategy["max_acc_pips_us500"] = 5000.0
config.macro_strategy["min_accumulation_candles"] = 5

data = backtest.load_from_csv("backtest/data", "US500m", datetime(2026, 6, 1, tzinfo=timezone.utc), datetime(2026, 6, 30, tzinfo=timezone.utc))
df = data["M1"]
df = df.head(10000)

bt_state = StateEngine(":memory:")
connector = BacktestConnector(config, {"M1": df}, "US500m")
scanner = ScannerMacro(config, connector, bt_state)

print(f"Starting scan over {len(df)} candles...")
signals = 0
for i in range(150, len(df), 1):
    current_time = df.iloc[i]["time"]
    connector._current_time = current_time
    
    sig = scanner.scan("US500m")
    if sig:
        print(f"Signal: {sig['direction']} at {sig['timestamp']}")
        signals += 1

print(f"Total valid signals: {signals}")
