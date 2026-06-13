import os
import sys
from datetime import datetime, timezone
import uuid

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.configengine import ConfigEngine
from modules.reportgoogle import GoogleSheetReporter

def main():
    print("Loading config...")
    config_engine = ConfigEngine()
    config = config_engine.get_config()

    print("Initializing GoogleSheetReporter...")
    reporter = GoogleSheetReporter(config)
    
    if not reporter.connect():
        print("❌ Failed to connect to Google Sheets. Check your google_creds_path and google_sheet_id in .env")
        return

    print("✅ Connected to Google Sheets!")
    print("Logging a fake executed trade to the 'Trades' sheet...")

    fake_signal = {
        "score": 85.0,
        "entry_mode": "DIRECT",
        "strategy": "ZGMT",
        "entry_leg": "Leg A",
        "setup_type": "ZGMT-A"
    }

    fake_trade = {
        "ticket_id": 999999,
        "execution_time": datetime.now(timezone.utc).isoformat(),
        "pair": "GBPUSD",
        "direction": "SELL",
        "executed_price": 1.3000,
        "sl": 1.3020,
        "tp1": 1.2980,
        "tp2": 1.2960,
        "tp3": 1.2940,
        "lot_total": 0.05,
        "risk_amount": 5.00
    }

    try:
        reporter.log_trade(fake_trade, fake_signal)
        print("✅ Fake trade successfully logged to the 'Trades' sheet!")
    except Exception as e:
        print(f"❌ Failed to log trade: {e}")

if __name__ == "__main__":
    main()
