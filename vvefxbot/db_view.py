import sqlite3
import pandas as pd
import os

def view_trades():
    db_path = 'db/fxbot.db'
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found. Start the bot first!")
        return

    conn = sqlite3.connect(db_path)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    print("\n" + "="*80)
    print("                VvE FxBOT: EXECUTED TRADES REPORT")
    print("="*80)

    try:
        # Query all executed trades
        query = """
        SELECT 
            ticket_id as Ticket,
            pair as Symbol,
            direction as Type,
            executed_price as Entry,
            sl as SL,
            tp1 as TP1,
            tp2 as TP2,
            lot_total as Lots,
            status as Status,
            result as Result,
            profit_usd as 'Profit ($)',
            execution_time as Time
        FROM trades_executed 
        ORDER BY execution_time DESC
        """
        trades = pd.read_sql_query(query, conn)
        
        if trades.empty:
            print("\n>>> No trades have been executed yet.")
            print(">>> The bot is scanning, but hasn't hit an A+ setup yet.")
        else:
            print(trades.to_string(index=False))
            
            # Show summary
            total_profit = trades['Profit ($)'].sum()
            print("\n" + "-"*40)
            print(f"TOTAL TRADES: {len(trades)}")
            print(f"TOTAL NET PROFIT: ${total_profit:,.2f}")
            print("-"*40)

    except Exception as e:
        print(f"Error reading database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    view_trades()
