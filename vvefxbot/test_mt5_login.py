import MetaTrader5 as mt5

print("Starting MT5 test...")
res1 = mt5.initialize()
print(f"init() alone: {res1}, error: {mt5.last_error()}")

if res1:
    res2 = mt5.login(login=463520050, password="Tradingbot@2026", server="Exness-MT5Trial17")
    print(f"login(): {res2}, error: {mt5.last_error()}")
    
    mt5.shutdown()

print("Now testing initialize with credentials...")
res3 = mt5.initialize(login=463520050, password="Tradingbot@2026", server="Exness-MT5Trial17")
print(f"init(creds): {res3}, error: {mt5.last_error()}")
mt5.shutdown()
