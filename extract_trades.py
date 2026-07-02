import sqlite3
import csv
import json

conn = sqlite3.connect('fxbot.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Join trades and signals
cursor.execute('''
    SELECT 
        t.trade_id, t.ticket_id, t.pair, t.direction, t.executed_price, t.sl, t.tp1, t.tp2, t.tp3,
        t.lot_total, t.execution_time, t.status, t.result, t.profit_usd, t.tp1_hit, t.tp2_hit, t.be_moved,
        t.current_sl, t.sl_usd, t.tp1_usd, t.tp2_usd, t.tp3_usd, t.margin_used,
        s.signal_id, s.session, s.killzone, s.timeframe_bias, s.timeframe_entry, s.bias_summary,
        s.entry_price, s.sl_price, s.tp1_price, s.tp2_price, s.tp3_price as sig_tp3_price,
        s.sl_pips, s.tp_pips, s.tp3_pips, s.spread_pips, s.effective_rr, s.score,
        s.detected_time, s.strategy, s.entry_mode, s.entry_leg, s.setup_type, s.killzone
    FROM trades_executed t
    LEFT JOIN signals_detected s ON t.signal_id = s.signal_id
    ORDER BY t.execution_time DESC
''')

rows = cursor.fetchall()

# Let's print the headers and first row to analyze, and also write to a CSV
if rows:
    headers = rows[0].keys()
    print("Columns available:", len(headers))
    print(headers)
    
    with open('today_trades.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
            print(dict(row))
else:
    print("No trades found in db!")

conn.close()
