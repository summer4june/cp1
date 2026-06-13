import os
import sys
from datetime import datetime, timezone

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
    print("Logging a fake Vault End-of-Day summary to the 'Vault' sheet...")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_profit = 25.50
    transferred_to_vault = 12.75
    new_vault_balance = 112.75
    new_trading_balance = 212.75
    new_lot_margin = 14.18

    try:
        reporter.log_vault_eod(today, daily_profit, transferred_to_vault, new_vault_balance, new_trading_balance, new_lot_margin)
        print("✅ Fake Vault EOD summary successfully logged to the 'Vault' sheet!")
    except Exception as e:
        print(f"❌ Failed to log Vault summary: {e}")

if __name__ == "__main__":
    main()
