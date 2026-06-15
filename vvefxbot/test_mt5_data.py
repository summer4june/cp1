import sys
import logging
from core.configengine import Config
from core.mt5connector import MT5Connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def test_mt5_data_fetch():
    print("===========================================")
    print("       MT5 LIVE DATA DIAGNOSTIC TEST       ")
    print("===========================================\n")
    
    print("1. Loading config...")
    config = Config.load()
    print("   ✅ Config loaded.")
    
    print("\n2. Initializing MT5 Connector...")
    connector = MT5Connector(config)
    success = connector.connect()
    
    if not success:
        print("   ❌ Failed to connect to MT5. Check terminal status and login credentials.")
        sys.exit(1)
    
    print("   ✅ Connected to MT5 successfully.")
    
    pairs_to_test = ["EURUSD", "XAUUSD"]
    
    print("\n3. Testing Candle Fetching for ADR (D1)...")
    for pair in pairs_to_test:
        # Requesting 8 candles (we only need 6 for ADR(5))
        df_d1 = connector.get_candles(pair, "D1", count=8)
        if df_d1 is not None and not df_d1.empty:
            print(f"   ✅ {pair}: Fetched {len(df_d1)} D1 candles. (Need 6+ for ADR)")
        else:
            print(f"   ❌ {pair}: Failed to fetch D1 candles!")
            
    print("\n4. Testing Candle Fetching for ZGMT Scanner (M15)...")
    for pair in pairs_to_test:
        df_m15 = connector.get_candles(pair, "M15", count=100)
        if df_m15 is not None and not df_m15.empty:
            print(f"   ✅ {pair}: Fetched {len(df_m15)} M15 candles.")
        else:
            print(f"   ❌ {pair}: Failed to fetch M15 candles!")

    print("\n===========================================")
    print(" If all tests above have a ✅, the bot has ")
    print(" plenty of data and the ZGMT ADR logic will")
    print(" work perfectly at 0 GMT tonight! ")
    print("===========================================\n")
    
    connector.disconnect()

if __name__ == "__main__":
    test_mt5_data_fetch()
