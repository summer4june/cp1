import sys
import pandas as pd
from core.configengine import ConfigEngine
from backtest.engine import ExecutionEngine
from backtest.connector import MT5ConnectorBacktest

config_engine = ConfigEngine("config.json")
config = config_engine.get_config()
config.backtest_mode = True

connector = MT5ConnectorBacktest("NZDJPY", config)
connector.load_data("2024-05-01", "2024-05-10", "D1")

candles = connector.get_candles("NZDJPY", "D1", 6)
print(candles)

completed = candles.iloc[-6:-1]
highest_high = float(completed['high'].max())
lowest_low = float(completed['low'].min())
adr = (highest_high - lowest_low) / 5
sl_dist = adr / 2
print(f"High: {highest_high}")
print(f"Low: {lowest_low}")
print(f"Diff: {highest_high - lowest_low}")
print(f"ADR dist: {adr}")
print(f"SL dist: {sl_dist}")
print(f"SL pips: {sl_dist / 0.01}")

