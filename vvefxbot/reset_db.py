import sqlite3
import os

def clear_db(db_path, tables):
    if not os.path.exists(db_path):
        print(f"{db_path} does not exist. Skipping.")
        return
        
    print(f"Connecting to {db_path}...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table};")
                print(f"Cleared table: {table}")
            except sqlite3.OperationalError as e:
                print(f"Skipping table {table} (not found or error: {e})")
        conn.commit()
        conn.close()
        print(f"Successfully cleared data in {db_path}\n")
    except Exception as e:
        print(f"Error accessing {db_path}: {e}\n")

if __name__ == "__main__":
    print("Resetting Databases (Data Only)...\n")
    
    # State Engine DB
    state_db = os.path.join("db", "fxbot.db")
    state_tables = [
        "trades_executed", 
        "trades_skipped", 
        "trade_management_events", 
        "daily_state", 
        "pair_state", 
        "errors", 
        "signals_detected"
    ]
    clear_db(state_db, state_tables)
    
    # Vault DB
    vault_db = "vault.sqlite"
    vault_tables = [
        "vault_ledger",
        "daily_ledger"
    ]
    clear_db(vault_db, vault_tables)
    
    # In case there are other DB names used historically
    clear_db("vvefxbot.db", state_tables)
    clear_db("state.db", state_tables)
    clear_db("vvefxbot_vault.db", vault_tables)
    clear_db("vault.db", vault_tables)
    
    print("Reset complete!")
