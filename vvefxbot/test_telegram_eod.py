import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.configengine import ConfigEngine
from modules.telegrambridge import TelegramBridge

def main():
    print("Loading config...")
    config_engine = ConfigEngine()
    config = config_engine.get_config()

    print("Initializing TelegramBridge...")
    bridge = TelegramBridge(config=config, state_engine=None, execution_callback=None)
    
    msg = (
        "🌙 *End of Day Vault Summary*\n\n"
        f"• Start Balance: `$100.00`\n"
        f"• End Balance: `$110.00`\n"
        f"• Daily Profit: `$10.00`\n"
        f"• Trading Balance: `$105.00`\n"
        f"• Vault Balance: `$5.00`\n"
        f"• Total Wealth: `$110.00`\n"
        f"• Current Lot Size (Margin): `$7.00`\n"
        f"• Win Rate: `66.6%`\n"
        f"• Daily Drawdown: `0.00%`\n"
        f"• Number of Trades: `3`\n\n"
        "_This is a simulated End of Day test message._"
    )
    
    print("Broadcasting EOD summary to Telegram...")
    # Because broadcast_message doesn't return anything, we assume success if no exception
    try:
        bridge.broadcast_message(msg)
        print("✅ Fake EOD summary successfully broadcasted to Telegram!")
    except Exception as e:
        print(f"❌ Failed to broadcast EOD summary: {e}")

if __name__ == "__main__":
    main()
