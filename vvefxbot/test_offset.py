import MetaTrader5 as mt5
from datetime import datetime, timezone

mt5.initialize()
tick = mt5.symbol_info_tick("EURUSD")
if tick:
    broker_time = tick.time
    real_utc = int(datetime.now(timezone.utc).timestamp())
    offset = broker_time - real_utc
    offset_hours = offset / 3600
    print(f"Broker offset: {offset_hours} hours")
else:
    print("Could not get tick")
mt5.shutdown()
