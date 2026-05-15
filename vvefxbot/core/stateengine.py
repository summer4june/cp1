import sqlite3
import threading
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from core.logger import get_logger

logger = get_logger("StateEngine")

class StateEngine:
    """Persistence engine for the trading bot using SQLite."""
    
    def __init__(self, db_path: str = "db/fxbot.db"):
        """
        Initializes the StateEngine and creates tables if they don't exist.
        """
        self.db_path = db_path
        self.lock = threading.Lock()
        self._memory_conn = None

        if self.db_path == ":memory:":
            self._memory_conn = sqlite3.connect(":memory:", check_same_thread=False)
        else:
            # Create db directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
        self._create_tables()

    def _get_connection(self):
        """Returns a thread-safe sqlite3 connection."""
        if self._memory_conn:
            return self._memory_conn
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _close_connection(self, conn):
        """Safely closes connection unless it is a persistent in-memory connection."""
        if self._memory_conn and conn == self._memory_conn:
            return
        conn.close()

    def _create_tables(self):
        """Creates all required database tables if they do not exist."""
        with self.lock:
            conn = self._get_connection()
            # For persistent file-based DB, we close after use. 
            # For :memory:, we keep using the same conn.
            cursor = conn.cursor()
            try:
                # 1. signals_detected
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals_detected (
                    signal_id TEXT PRIMARY KEY, pair TEXT, session TEXT,
                    timeframe_bias TEXT, timeframe_entry TEXT, direction TEXT,
                    bias_summary TEXT, entry_price REAL, sl_price REAL,
                    tp1_price REAL, tp2_price REAL, sl_pips REAL, tp_pips REAL,
                    spread_pips REAL, effective_rr REAL, score REAL,
                    detected_time TEXT
                )
                """)

                # 2. trades_executed
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades_executed (
                    trade_id TEXT PRIMARY KEY, signal_id TEXT, ticket_id INTEGER,
                    pair TEXT, direction TEXT,
                    executed_price REAL, sl REAL, tp1 REAL, tp2 REAL,
                    lot_total REAL, risk_amount REAL, execution_time TEXT,
                    status TEXT, result TEXT, profit_usd REAL,
                    tp1_hit INTEGER DEFAULT 0, be_moved INTEGER DEFAULT 0
                )
                """)

                # 3. trades_skipped
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades_skipped (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT, reason TEXT, spread REAL, score REAL, skip_time TEXT
                )
                """)

                # 4. trade_management_events
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_management_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT, event_type TEXT, event_time TEXT, price REAL
                )
                """)

                # 5. errors
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, module TEXT, error_message TEXT, stacktrace TEXT
                )
                """)

                # 6. daily_state
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_state (
                    date TEXT PRIMARY KEY, total_trades INTEGER DEFAULT 0,
                    total_loss_usd REAL DEFAULT 0.0, consecutive_losses INTEGER DEFAULT 0,
                    daily_profit_usd REAL DEFAULT 0.0, bot_disabled INTEGER DEFAULT 0
                )
                """)

                # 7. pair_state
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS pair_state (
                    pair TEXT PRIMARY KEY, trades_today INTEGER DEFAULT 0,
                    last_signal_id TEXT, last_trade_time TEXT, cooldown_until TEXT
                )
                """)

                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"DB Initialization Error: {e}")
            finally:
                self._close_connection(conn)

    # --- SIGNALS ---

    def insert_signal(self, signal: Dict[str, Any]) -> None:
        """Inserts a detected signal into the database."""
        with self.lock:
            conn = self._get_connection()
            try:
                columns = ', '.join(signal.keys())
                placeholders = ', '.join(['?'] * len(signal))
                sql = f"INSERT INTO signals_detected ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(signal.values()))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error inserting signal: {e}")
            finally:
                self._close_connection(conn)

    def signal_exists(self, signal_id: str) -> bool:
        """Checks if a signal with the given ID already exists."""
        with self.lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT 1 FROM signals_detected WHERE signal_id = ?", (signal_id,))
                return cursor.fetchone() is not None
            except sqlite3.Error as e:
                logger.error(f"Error checking signal existence: {e}")
                return False
            finally:
                self._close_connection(conn)

    def has_recent_signal(self, pair: str, direction: str, cooldown_minutes: int) -> bool:
        """Checks if a signal for the same pair and direction was generated within the cooldown window."""
        with self.lock:
            conn = self._get_connection()
            try:
                # Calculate the cutoff time
                cutoff_time = (datetime.now(timezone.utc) - __import__('datetime').timedelta(minutes=cooldown_minutes)).isoformat()
                cursor = conn.execute(
                    "SELECT 1 FROM signals_detected WHERE pair = ? AND direction = ? AND detected_time >= ?",
                    (pair, direction, cutoff_time)
                )
                return cursor.fetchone() is not None
            except sqlite3.Error as e:
                logger.error(f"Error checking recent signal: {e}")
                return False
            finally:
                self._close_connection(conn)

    def get_signal(self, signal_id: str) -> Optional[Dict[str, Any]]:
        """Fetches a full signal row from signals_detected by signal_id."""
        with self.lock:
            conn = self._get_connection()
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(
                    "SELECT * FROM signals_detected WHERE signal_id = ?", (signal_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
            except sqlite3.Error as e:
                logger.error(f"Error fetching signal {signal_id}: {e}")
                return None
            finally:
                self._close_connection(conn)

    # --- TRADES ---

    def insert_trade(self, trade: Dict[str, Any]) -> None:
        """Inserts an executed trade into the database."""
        with self.lock:
            conn = self._get_connection()
            try:
                columns = ', '.join(trade.keys())
                placeholders = ', '.join(['?'] * len(trade))
                sql = f"INSERT INTO trades_executed ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(trade.values()))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error inserting trade: {e}")
            finally:
                self._close_connection(conn)

    def get_open_trades(self) -> List[Dict[str, Any]]:
        """Returns all trades with status 'OPEN'."""
        with self.lock:
            conn = self._get_connection()
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute("SELECT * FROM trades_executed WHERE status = 'OPEN'")
                return [dict(row) for row in cursor.fetchall()]
            except sqlite3.Error as e:
                logger.error(f"Error getting open trades: {e}")
                return []
            finally:
                self._close_connection(conn)

    def update_trade_status(self, trade_id: str, status: str, result: str, profit_usd: float) -> None:
        """Updates the status, result, and profit of a trade."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    UPDATE trades_executed 
                    SET status = ?, result = ?, profit_usd = ?
                    WHERE trade_id = ?
                """, (status, result, profit_usd, trade_id))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error updating trade status: {e}")
            finally:
                self._close_connection(conn)

    def update_trade_tp1_hit(self, trade_id: str) -> None:
        """Marks TP1 as hit for a trade."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE trades_executed SET tp1_hit = 1 WHERE trade_id = ?", (trade_id,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error updating TP1 hit: {e}")
            finally:
                self._close_connection(conn)

    def update_trade_be_moved(self, trade_id: str) -> None:
        """Marks BE as moved for a trade."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE trades_executed SET be_moved = 1 WHERE trade_id = ?", (trade_id,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error updating BE moved: {e}")
            finally:
                self._close_connection(conn)

    def get_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """Fetches a full trade row from trades_executed by trade_id."""
        with self.lock:
            conn = self._get_connection()
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute(
                    "SELECT * FROM trades_executed WHERE trade_id = ?", (trade_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
            except sqlite3.Error as e:
                logger.error(f"Error fetching trade {trade_id}: {e}")
                return None
            finally:
                self._close_connection(conn)

    # --- DAILY STATE ---

    def _ensure_daily_state(self, date: str) -> None:
        """Ensures a row exists for the given date in daily_state."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("INSERT OR IGNORE INTO daily_state (date) VALUES (?)", (date,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error ensuring daily state: {e}")
            finally:
                self._close_connection(conn)

    def get_daily_state(self, date: str) -> Dict[str, Any]:
        """Returns the daily state for a given date."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute("SELECT * FROM daily_state WHERE date = ?", (date,))
                return dict(cursor.fetchone())
            except sqlite3.Error as e:
                logger.error(f"Error getting daily state: {e}")
                return {}
            finally:
                self._close_connection(conn)

    def increment_daily_trades(self, date: str) -> None:
        """Increments the total trades count for the day."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE daily_state SET total_trades = total_trades + 1 WHERE date = ?", (date,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error incrementing daily trades: {e}")
            finally:
                self._close_connection(conn)

    def add_daily_loss(self, date: str, loss_usd: float) -> None:
        """Adds to the daily loss and updates total profit."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    UPDATE daily_state 
                    SET total_loss_usd = total_loss_usd + ?,
                        daily_profit_usd = daily_profit_usd - ?
                    WHERE date = ?
                """, (loss_usd, loss_usd, date))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error adding daily loss: {e}")
            finally:
                self._close_connection(conn)

    def add_daily_profit(self, date: str, profit_usd: float) -> None:
        """Adds to the daily profit."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE daily_state SET daily_profit_usd = daily_profit_usd + ? WHERE date = ?", (profit_usd, date))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error adding daily profit: {e}")
            finally:
                self._close_connection(conn)

    def increment_consecutive_losses(self, date: str) -> None:
        """Increments the consecutive losses count for the day."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE daily_state SET consecutive_losses = consecutive_losses + 1 WHERE date = ?", (date,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error incrementing consecutive losses: {e}")
            finally:
                self._close_connection(conn)

    def reset_consecutive_losses(self, date: str) -> None:
        """Resets the consecutive losses count to zero."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE daily_state SET consecutive_losses = 0 WHERE date = ?", (date,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error resetting consecutive losses: {e}")
            finally:
                self._close_connection(conn)

    def disable_bot_today(self, date: str) -> None:
        """Disables the bot for the current day."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE daily_state SET bot_disabled = 1 WHERE date = ?", (date,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error disabling bot today: {e}")
            finally:
                self._close_connection(conn)

    def is_bot_disabled_today(self, date: str) -> bool:
        """Checks if the bot is disabled for the given date."""
        self._ensure_daily_state(date)
        with self.lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT bot_disabled FROM daily_state WHERE date = ?", (date,))
                row = cursor.fetchone()
                return bool(row[0]) if row else False
            except sqlite3.Error as e:
                logger.error(f"Error checking bot disabled: {e}")
                return False
            finally:
                self._close_connection(conn)

    # --- PAIR STATE ---

    def _ensure_pair_state(self, pair: str) -> None:
        """Ensures a row exists for the given pair in pair_state."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("INSERT OR IGNORE INTO pair_state (pair) VALUES (?)", (pair,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error ensuring pair state: {e}")
            finally:
                self._close_connection(conn)

    def get_pair_state(self, pair: str) -> Dict[str, Any]:
        """Returns the state for a given trading pair."""
        self._ensure_pair_state(pair)
        with self.lock:
            conn = self._get_connection()
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.execute("SELECT * FROM pair_state WHERE pair = ?", (pair,))
                return dict(cursor.fetchone())
            except sqlite3.Error as e:
                logger.error(f"Error getting pair state: {e}")
                return {}
            finally:
                self._close_connection(conn)

    def increment_pair_trades(self, pair: str) -> None:
        """Increments the trade count for a pair today."""
        self._ensure_pair_state(pair)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE pair_state SET trades_today = trades_today + 1 WHERE pair = ?", (pair,))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error incrementing pair trades: {e}")
            finally:
                self._close_connection(conn)

    def set_pair_cooldown(self, pair: str, until: datetime) -> None:
        """Sets a cooldown period for a pair."""
        self._ensure_pair_state(pair)
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("UPDATE pair_state SET cooldown_until = ? WHERE pair = ?", (until.isoformat(), pair))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error setting pair cooldown: {e}")
            finally:
                self._close_connection(conn)

    def is_pair_on_cooldown(self, pair: str) -> bool:
        """Checks if a pair is currently on cooldown."""
        state = self.get_pair_state(pair)
        cooldown_str = state.get("cooldown_until")
        if not cooldown_str:
            return False
        
        try:
            from datetime import timezone
            until = datetime.fromisoformat(cooldown_str)
            if until.tzinfo is not None:
                return datetime.now(timezone.utc) < until
            else:
                return datetime.now() < until
        except Exception as e:
            logger.error(f"Error parsing cooldown time: {e}")
            return False

    def get_pair_trades_today(self, pair: str) -> int:
        """Returns the number of trades executed for a pair today."""
        state = self.get_pair_state(pair)
        return state.get("trades_today", 0)

    # --- SKIPPED / EVENTS / ERRORS ---

    def insert_skip(self, signal_id: str, reason: str, spread: float, score: float) -> None:
        """Logs a skipped signal."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT INTO trades_skipped (signal_id, reason, spread, score, skip_time)
                    VALUES (?, ?, ?, ?, ?)
                """, (signal_id, reason, spread, score, datetime.now().isoformat()))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error inserting skip: {e}")
            finally:
                self._close_connection(conn)

    def insert_event(self, trade_id: str, event_type: str, price: float) -> None:
        """Logs a trade management event."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT INTO trade_management_events (trade_id, event_type, event_time, price)
                    VALUES (?, ?, ?, ?)
                """, (trade_id, event_type, datetime.now().isoformat(), price))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Error inserting event: {e}")
            finally:
                self._close_connection(conn)

    def log_error(self, module: str, error_message: str, stacktrace: str) -> None:
        """Logs an application error to the database."""
        with self.lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT INTO errors (timestamp, module, error_message, stacktrace)
                    VALUES (?, ?, ?, ?)
                """, (datetime.now().isoformat(), module, error_message, stacktrace))
                conn.commit()
            except sqlite3.Error as e:
                # Log to logger as fallback
                logger.critical(f"FATAL DB ERROR when logging error: {e}")
            finally:
                conn.close()
