import os
import sys
from datetime import datetime, timezone

# Add project root to python path so it can be run easily
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.configengine import ConfigEngine
from modules.telegrambridge import TelegramBridge

def main():
    print("Loading config...")
    config_engine = ConfigEngine()
    config = config_engine.config

    print("Initializing TelegramBridge...")
    # Initialize with None for things we don't need in this test
    bridge = TelegramBridge(config=config, state_engine=None, execution_callback=lambda x: print(f"Execute callback for {x}"))
    
    print("Sending fake trade signal...")
    fake_signal = {
        "signal_id": "test_sig_001",
        "pair": "XAUUSD",
        "session": "London",
        "direction": "BUY",
        "timeframe_entry": "M1",
        "bias_summary": "Bullish structure | Discount array",
        "score": 95.5,
        "entry_price": 2400.00,
        "sl_price": 2398.00,
        "tp1_price": 2402.00,
        "tp2_price": 2406.00,
        "tp3_price": 2410.00,
        "sl_pips": 20.0,
        "tp_pips": 60.0,
        "spread_pips": 1.5,
        "effective_rr": 3.0,
        "detected_time": datetime.now(timezone.utc).isoformat()
    }
    
    fake_usd_metrics = {
        "sl_usd": 20.00,
        "tp1_usd": 20.00,
        "tp2_usd": 60.00,
        "tp3_usd": 100.00,
        "margin_usd": 24.00
    }
    
    success = bridge.send_signal(fake_signal, lot_size=0.01, usd_metrics=fake_usd_metrics)
    
    if success:
        print("✅ Fake trade signal successfully sent to Telegram!")
        print("Check your Telegram group. You can Ctrl+C to exit.")
        # Start listener so you can click the buttons to see if callbacks work
        bridge.start_listener()
        import time
        time.sleep(10)
    else:
        print("❌ Failed to send signal. Check your TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in .env")

if __name__ == "__main__":
    main()
