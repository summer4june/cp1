import MetaTrader5 as mt5
from datetime import datetime, timedelta

mt5.initialize()
from_date = datetime.now() - timedelta(days=2)
all_deals = mt5.history_deals_get(from_date, datetime.now())
if all_deals:
    print(f"Found {len(all_deals)} deals")
    for d in all_deals[-5:]:
        print(f"Deal: {d.ticket}, Order: {d.order}, Position: {getattr(d, 'position_id', 'N/A')}, Profit: {d.profit}")
mt5.shutdown()
