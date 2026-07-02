import sqlite3
import csv
from datetime import datetime, timezone, timedelta
import pytz

def build_row_data(trade, signal):
    _IST = pytz.timezone('Asia/Kolkata')
    now_utc = datetime.now(pytz.utc)
    if "execution_time" in trade and trade["execution_time"]:
        try:
            now_utc = datetime.fromisoformat(trade["execution_time"].replace('Z', '+00:00'))
        except ValueError:
            pass

    now_ist = now_utc.astimezone(_IST)
    
    close_time_ist = ""
    status = trade.get("status", "OPEN")
    if status == "CLOSED":
        close_time_ist = datetime.now(pytz.utc).astimezone(_IST).strftime("%Y-%m-%d %H:%M:%S IST")

    pair = trade.get("pair") or (signal.get("pair", "") if signal else "")
    direction = trade.get("direction") or (signal.get("direction", "") if signal else "")
    entry_price = trade.get("executed_price", 0.0)
    sl_price = trade.get("sl", 0.0)
    tp1_price = trade.get("tp1", 0.0)
    tp2_price = trade.get("tp2", 0.0)
    tp3_price = trade.get("tp3", 0.0) or (signal.get("tp3_price", 0.0) if signal else 0.0)

    point = 0.01 if pair and ("JPY" in pair or "XAU" in pair or "XAG" in pair) else 0.0001
    sl_usd = float(trade.get("sl_usd") or trade.get("risk_amount") or 0.0)
    tp1_usd = float(trade.get("tp1_usd") or sl_usd)
    tp2_usd = float(trade.get("tp2_usd") or sl_usd * 2.0)
    tp3_usd = float(trade.get("tp3_usd") or sl_usd * 3.0)
    
    sl_pips = float(trade.get("sl_pips") or (signal.get("sl_pips", 0.0) if signal else 0.0))

    pip_size = point
    
    if trade.get("tp1") and trade.get("executed_price"):
        tp1_pips = round(abs(trade.get("tp1") - trade.get("executed_price")) / pip_size, 2)
    else:
        tp1_pips = float(trade.get("tp1_pips") or sl_pips)
        
    if trade.get("tp2") and trade.get("executed_price"):
        tp2_pips = round(abs(trade.get("tp2") - trade.get("executed_price")) / pip_size, 2)
    else:
        tp2_pips = float(trade.get("tp2_pips") or sl_pips * 2.0)
        
    if trade.get("tp3") and trade.get("executed_price"):
        tp3_pips = round(abs(trade.get("tp3") - trade.get("executed_price")) / pip_size, 2)
    else:
        tp3_pips = float(trade.get("tp3_pips") or sl_pips * 3.0)
    margin_used = float(trade.get("margin_used") or 0.0)

    result = trade.get("result", "") or ""
    max_level = ""
    if result == "TP1_HIT":
        max_level = "tp1"
    elif result == "TP2_HIT":
        max_level = "tp2"
    elif result == "TP3_HIT":
        max_level = "tp3"

    exec_time_str = trade.get("execution_time")
    if exec_time_str:
        try:
            exec_dt_utc = datetime.fromisoformat(exec_time_str.replace('Z', '+00:00'))
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            exec_dt_ist = exec_dt_utc.astimezone(ist_tz)
            open_time_ist = exec_dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
        except Exception:
            open_time_ist = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    else:
        open_time_ist = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

    return [
        trade.get("trade_id", ""),
        pair,
        direction,
        now_ist.strftime("%Y"),
        signal.get("session", "") if signal else "",
        signal.get("entry_leg", "") if signal else "",
        entry_price,
        sl_price,
        tp1_price,
        tp2_price,
        tp3_price,
        entry_price,
        sl_usd,
        tp1_usd,
        tp2_usd,
        tp3_usd,
        trade.get("lot_total", 0.0),
        "",  # open_bar
        open_time_ist,
        "",  # close_bar
        close_time_ist,
        status,
        result,
        trade.get("profit_usd", 0.0) or 0.0,
        "",  # exit_price
        result,  # exit_reason
        max_level,
        sl_pips,
        tp1_pips,
        tp2_pips,
        tp3_pips,
        now_ist.strftime("%m"),
        now_ist.isocalendar()[1],
        round(margin_used, 2) if margin_used > 0 else "",
        signal.get("score", 0.0) if signal else 0.0
    ]

headers = [
    "trade_id", "pair", "direction", "year", "session", "entry_leg", "entry_price", "sl_price",
    "tp1_price", "tp2_price", "tp3_price", "entry", "sl_usd", "tp1_usd", "tp2_usd", "tp3_usd",
    "lot", "open_bar", "open_time", "close_bar", "close_time", "status", "result", "profit_usd",
    "exit_price", "exit_reason", "max_level_reached", "sl_pips", "tp1_pips", "tp2_pips",
    "tp3_pips", "month", "week_no", "margin_used", "score"
]

conn = sqlite3.connect('fxbot.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT * FROM trades_executed ORDER BY execution_time DESC")
trades = cursor.fetchall()

with open('today_trades_35.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    for trade_row in trades:
        trade = dict(trade_row)
        cursor.execute("SELECT * FROM signals_detected WHERE signal_id = ?", (trade.get("signal_id"),))
        sig_row = cursor.fetchone()
        signal = dict(sig_row) if sig_row else {}
        row = build_row_data(trade, signal)
        writer.writerow(row)

conn.close()
