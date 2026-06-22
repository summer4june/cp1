import pandas as pd
from core.configengine import ConfigEngine
from core.mt5connector import MT5Connector

config_engine = ConfigEngine()
config = config_engine.get_config()
connector = MT5Connector(config)
connector.connect()

df = connector.get_candles("EURUSDm", "H1", count=10)
if df is not None and not df.empty:
    for _, row in df.iterrows():
        print(f"Candle Time (raw df): {row['time']}")

connector.disconnect()
