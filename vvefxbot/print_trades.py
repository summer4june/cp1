import sqlite3
import pandas as pd

DB_PATH = 'db/fxbot.db'

def main():
    conn = sqlite3.connect(DB_PATH)
    
    print("\n" + "="*80)
    print("📈 TAKEN TRADES (EXECUTED)")
    print("="*80)
    
    query_executed = """
        SELECT t.execution_time, t.pair, t.direction, t.lot_total, t.status, t.profit_usd, s.strategy, s.entry_leg
        FROM trades_executed t
        LEFT JOIN signals_detected s ON t.signal_id = s.signal_id
        ORDER BY t.execution_time DESC
    """
    
    df_exec = pd.read_sql_query(query_executed, conn)
    if df_exec.empty:
        print("No trades have been executed yet.")
    else:
        print(df_exec.to_string(index=False))
        print(f"\nTotal Taken Trades: {len(df_exec)}")
        print(f"Total Profit/Loss: ${df_exec['profit_usd'].sum():.2f}")


    print("\n" + "="*80)
    print("🚫 REJECTED TRADES (SKIPPED)")
    print("="*80)
    
    query_skipped = """
        SELECT ts.skip_time, s.pair, s.direction, s.strategy, s.entry_leg, ts.reason
        FROM trades_skipped ts
        LEFT JOIN signals_detected s ON ts.signal_id = s.signal_id
        ORDER BY ts.skip_time DESC
    """
    
    df_skip = pd.read_sql_query(query_skipped, conn)
    if df_skip.empty:
        print("No trades have been rejected/skipped yet.")
    else:
        print(df_skip.to_string(index=False))
        print(f"\nTotal Rejected Trades: {len(df_skip)}")
        
    conn.close()

if __name__ == '__main__':
    main()
