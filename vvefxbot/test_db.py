from datetime import datetime, timezone, timedelta
import sqlite3

conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE signals_detected (pair TEXT, direction TEXT, detected_time TEXT)")

dtime = datetime.now(timezone.utc).isoformat()
conn.execute("INSERT INTO signals_detected VALUES (?, ?, ?)", ("AUDUSDm", "SELL", dtime))

cutoff_time = (datetime.now(timezone.utc) - timedelta(minutes=1440)).isoformat()
cursor = conn.execute("SELECT 1 FROM signals_detected WHERE pair = ? AND direction = ? AND detected_time >= ?", ("AUDUSDm", "SELL", cutoff_time))
print(cursor.fetchone())
