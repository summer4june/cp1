import sqlite3
conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE test (col1 TEXT)")
try:
    conn.execute("INSERT INTO test (col1) VALUES (?)", [{"name": "London"}])
    print("Inserted successfully")
except Exception as e:
    print(f"Failed: {type(e).__name__} - {e}")
