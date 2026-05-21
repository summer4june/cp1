import os
import sys
import json
from datetime import datetime, timezone
import MetaTrader5 as mt5

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.configengine import ConfigEngine
from backtest.connector import BacktestConnector

def main():
    if not mt5.initialize():
        print(f"MT5 init failed: {mt5.last_error()}")
        return
    
    config_engine = ConfigEngine("config.json")
    config = config_engine.get_config()
    
    authorized = mt5.login(
        login=config.mt5_login,
        password=config.mt5_password,
        server=config.mt5_server,
    )
    if not authorized:
        print(f"MT5 login failed: {mt5.last_error()}")
        mt5.shutdown()
        return
        
    print("MT5 connected successfully!")
    
    # Let's fetch one day of XAUUSD history: 2026-04-01
    symbol = "XAUUSD"
    date_from = datetime(2026, 4, 1, tzinfo=timezone.utc)
    date_to = datetime(2026, 4, 2, tzinfo=timezone.utc)
    
    dt_from = date_from.replace(tzinfo=None)
    dt_to = date_to.replace(tzinfo=None)
    
    # H1
    rates_h1 = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_H1, dt_from, dt_to)
    # M1
    rates_m1 = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, dt_from, dt_to)
    
    mt5.shutdown()
    
    if rates_h1 is None or len(rates_h1) == 0:
        print("Failed to fetch H1 rates.")
        return
    if rates_m1 is None or len(rates_m1) == 0:
        print("Failed to fetch M1 rates.")
        return
        
    import pandas as pd
    df_h1 = pd.DataFrame(rates_h1)
    df_h1["time_raw"] = df_h1["time"]
    df_h1["time_utc"] = pd.to_datetime(df_h1["time"], unit="s", utc=True)
    
    df_m1 = pd.DataFrame(rates_m1)
    df_m1["time_raw"] = df_m1["time"]
    df_m1["time_utc"] = pd.to_datetime(df_m1["time"], unit="s", utc=True)
    
    print("\n--- H1 First 5 candles ---")
    print(df_h1[["time_raw", "time_utc", "open", "high", "low", "close"]].head(5).to_string())
    
    print("\n--- M1 First 5 candles ---")
    print(df_m1[["time_raw", "time_utc", "open", "high", "low", "close"]].head(5).to_string())

if __name__ == "__main__":
    main()
