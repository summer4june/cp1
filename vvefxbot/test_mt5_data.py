import sys
import pandas as pd
from datetime import datetime, timezone
import MetaTrader5 as mt5

if not mt5.initialize():
    print("MT5 init failed")
    sys.exit(1)

start_dt = datetime(2025, 1, 15, tzinfo=timezone.utc)
end_dt = datetime(2025, 1, 16, tzinfo=timezone.utc)
rates = mt5.copy_rates_range("USDJPY", mt5.TIMEFRAME_M1, start_dt, end_dt)
if rates is not None and len(rates) > 0:
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    
    # print the first 20 minutes of the day
    print(df.head(20)[['time', 'open', 'high', 'low', 'close']])
else:
    print("No data")
mt5.shutdown()
