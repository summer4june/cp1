import MetaTrader5 as mt5
from datetime import datetime, timezone, timedelta
import time
import json

with open("config.json", "r") as f:
    config = json.load(f)

mt5.initialize(
    login=config["mt5"]["login"],
    server=config["mt5"]["server"],
    password=config["mt5"]["password"]
)

symbol = "US30m"
mt5.symbol_select(symbol, True)
info = mt5.symbol_info(symbol)

if not info:
    print("Symbol not found")
    mt5.shutdown()
    exit()

# Get current UTC time
now_utc = datetime.now(timezone.utc)
# Set expiration to 10 minutes from now (UTC)
expire_utc = now_utc + timedelta(minutes=10)
# Get POSIX timestamp (UTC)
expire_ts = int(expire_utc.timestamp())

print(f"Now UTC: {now_utc}")
print(f"Expire UTC: {expire_utc}")
print(f"Expire TS: {expire_ts}")

price = info.ask - 50.0  # Limit order way below current price
sl = price - 20.0
tp = price + 20.0

request = {
    "action": mt5.TRADE_ACTION_PENDING,
    "symbol": symbol,
    "volume": 0.1,
    "type": mt5.ORDER_TYPE_BUY_LIMIT,
    "price": round(price, 2),
    "sl": round(sl, 2),
    "tp": round(tp, 2),
    "type_time": mt5.ORDER_TIME_SPECIFIED,
    "expiration": expire_ts,
    "type_filling": mt5.ORDER_FILLING_IOC,
}

result = mt5.order_send(request)
if result.retcode != mt5.TRADE_RETCODE_DONE:
    print(f"Order failed: {result.retcode}")
else:
    print(f"Order placed: {result.order}")
    
    # Fetch the order and check its expiration time
    orders = mt5.orders_get(ticket=result.order)
    if orders:
        order = orders[0]
        print(f"Order expiration (from MT5): {order.time_expiration}")
        dt = datetime.fromtimestamp(order.time_expiration, tz=timezone.utc)
        print(f"Parsed MT5 expiration as UTC: {dt}")
        
    # Cancel the order
    mt5.order_send({
        "action": mt5.TRADE_ACTION_REMOVE,
        "order": result.order
    })

mt5.shutdown()
