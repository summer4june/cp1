import os
import time
from typing import Dict, Any, List
from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from core.logger import get_logger
from core.configengine import Config

logger = get_logger("GoogleSheetReporter")

_IST = pytz.timezone("Asia/Kolkata")
_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

_HEADERS = [
    "trade_id", "pair", "direction", "year", "session", "entry_leg", 
    "entry_price", "sl_price", "tp1_price", "tp2_price", "tp3_price",
    "entry", "sl_usd", "tp1_usd", "tp2_usd", "tp3_usd", "lot", 
    "open_bar", "open_time", "close_bar", "close_time", "status", 
    "result", "profit_usd", "exit_price", "exit_reason", "max_level_reached", "sl_pips", 
    "tp1_pips", "tp2_pips", "tp3_pips", "month", "week_no", "margin_used", "score"
]

class GoogleSheetReporter:
    """Reporter to push trade data to a Google Sheet using gspread."""

    def __init__(self, config: Config):
        """
        Initializes the GoogleSheetReporter.

        Args:
            config (Config): Validated configuration dataclass.
        """
        self.config = config
        self.client = None
        self.sheet = None
        self.denied_sheet = None
        self.vault_sheet = None

    def connect(self) -> bool:
        """
        Authenticate and connect to the Google Sheet.

        Returns:
            bool: True on success, False on failure.
        """
        try:
            creds_path = self.config.google_creds_path
            sheet_id = self.config.google_sheet_id

            if not creds_path or not os.path.exists(creds_path):
                logger.error(f"Google credentials file not found: {creds_path}")
                return False

            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, _SCOPES)
            self.client = gspread.authorize(creds)
            
            # Open the first worksheet of the specified spreadsheet
            spreadsheet = self.client.open_by_key(sheet_id)
            self.sheet = spreadsheet.sheet1

            # Check if header exists, if not auto-add it
            first_row = self.sheet.row_values(1)
            if not first_row or first_row[0] not in ("trade_id", "TradeID"):
                self.sheet.insert_row(_HEADERS, 1)
                logger.info("Added header row to main Google Sheet.")

            logger.info("Successfully connected to main Google Sheet.")
            
            # Connect to Denied/Rejected sheet in the SAME spreadsheet
            try:
                self.denied_sheet = spreadsheet.worksheet("Rejected")
            except gspread.exceptions.WorksheetNotFound:
                logger.info("Rejected sheet not found, creating it now.")
                self.denied_sheet = spreadsheet.add_worksheet(title="Rejected", rows=1000, cols=15)
                
            denied_headers = ["Date", "Time", "Pair", "Strategy", "Direction", "Reason", "Spread", "Signal Score", "Signal ID"]
            first_row_denied = self.denied_sheet.row_values(1)
            if not first_row_denied or first_row_denied[0] != "Date":
                self.denied_sheet.insert_row(denied_headers, 1)
                logger.info("Added header row to Rejected Google Sheet tab.")

            # Connect to Vault sheet in the SAME spreadsheet
            try:
                self.vault_sheet = spreadsheet.worksheet("Vault")
            except gspread.exceptions.WorksheetNotFound:
                logger.info("Vault sheet not found, creating it now.")
                self.vault_sheet = spreadsheet.add_worksheet(title="Vault", rows=1000, cols=10)
                
            vault_headers = ["Date", "Time", "Daily Profit", "Transferred to Vault", "New Vault Balance", "New Trading Balance", "New Lot Margin"]
            first_row_vault = self.vault_sheet.row_values(1)
            if not first_row_vault or first_row_vault[0] != "Date":
                self.vault_sheet.insert_row(vault_headers, 1)
                logger.info("Added header row to Vault Google Sheet tab.")

            return True

        except Exception as e:
            logger.error(f"Failed to connect to Google Sheet: {e}")
            return False

    def log_trade(self, trade: Dict[str, Any], signal: Dict[str, Any]) -> bool:
        """
        Smart insert-or-update trade record in the Google Sheet.

        - If the trade is OPEN: appends a new row.
        - If the trade is CLOSED: finds the existing row by TradeID in Notes
          column and updates Result + Profit in place. Falls back to append
          if the row is not found.

        Args:
            trade (dict): Executed trade data.
            signal (dict): Original signal data.

        Returns:
            bool: True on success, False on failure.
        """
        return self._log_trade_with_retry(trade, signal, retry_count=1)

    def _build_row_data(self, trade: Dict[str, Any], signal: Dict[str, Any]) -> list:
        """Build the full row data list for a trade (35 columns)."""
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

        # Pip distances
        point = 0.01 if ("JPY" in pair or "XAU" in pair or "XAG" in pair) else 0.0001
        # USD values (using exact metrics from MT5 if available)
        sl_usd = float(trade.get("sl_usd") or trade.get("risk_amount", 0.0))
        tp1_usd = float(trade.get("tp1_usd") or sl_usd)
        tp2_usd = float(trade.get("tp2_usd") or sl_usd * 2.0)
        tp3_usd = float(trade.get("tp3_usd") or sl_usd * 3.0)
        
        sl_pips = float(trade.get("sl_pips") or signal.get("sl_pips", 0.0))

        # Calculate exact pips from prices if available
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

        # Calculate proper open time
        exec_time_str = trade.get("execution_time")
        if exec_time_str:
            try:
                exec_dt_utc = datetime.fromisoformat(exec_time_str)
                # Convert UTC to IST
                ist_tz = timezone(timedelta(hours=5, minutes=30))
                exec_dt_ist = exec_dt_utc.astimezone(ist_tz)
                open_time_ist = exec_dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
            except Exception:
                open_time_ist = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")
        else:
            open_time_ist = now_ist.strftime("%Y-%m-%d %H:%M:%S IST")

        return [
            trade.get("trade_id", ""),                                      # 1. trade_id
            pair,                                                           # 2. pair
            direction,                                                      # 3. direction
            now_ist.strftime("%Y"),                                         # 4. year
            signal.get("session", "") if signal else "",                    # 5. session
            signal.get("entry_leg", "") if signal else "",                  # 6. entry_leg
            entry_price,                                                    # 7. entry_price
            sl_price,                                                       # 8. sl_price
            tp1_price,                                                      # 9. tp1_price
            tp2_price,                                                      # 10. tp2_price
            tp3_price,                                                      # 11. tp3_price
            entry_price,                                                    # 12. entry
            sl_usd,                                                         # 13. sl_usd
            tp1_usd,                                                        # 14. tp1_usd
            tp2_usd,                                                        # 15. tp2_usd
            tp3_usd,                                                        # 16. tp3_usd
            trade.get("lot_total", 0.0),                                    # 17. lot
            "",  # open_bar                                                 # 18. open_bar
            open_time_ist,                                                  # 19. open_time
            "",  # close_bar                                                # 20. close_bar
            close_time_ist,                                                 # 21. close_time
            status,                                                         # 22. status
            result,                                                         # 23. result
            trade.get("profit_usd", 0.0) or 0.0,                            # 24. profit_usd
            "",  # exit_price                                               # 25. exit_price
            result,  # exit_reason                                          # 26. exit_reason
            max_level,  # max_level_reached                                 # 27. max_level_reached
            sl_pips,                                                        # 28. sl_pips
            tp1_pips,                                                       # 29. tp1_pips
            tp2_pips,                                                       # 30. tp2_pips
            tp3_pips,                                                       # 31. tp3_pips
            now_ist.strftime("%m"),                                         # 32. month
            now_ist.isocalendar()[1],                                       # 33. week_no
            round(margin_used, 2) if margin_used > 0 else "",               # 34. margin_used
            signal.get("score", 0.0) if signal else 0.0                     # 35. score
        ]

    def _find_row_by_trade_id(self, trade_id: str) -> int:
        """
        Find the 1-indexed row number in the sheet whose trade_id cell (col 1) matches exactly.
        Returns -1 if not found.
        """
        try:
            # trade_id is the 1st column (index 0, 1-indexed col 1)
            id_col = self.sheet.col_values(1)
            for i, cell_val in enumerate(id_col):
                if str(cell_val).strip() == trade_id:
                    return i + 1  # 1-indexed
        except Exception as e:
            logger.warning(f"Could not search trade_id column: {e}")
        return -1

    def _log_trade_with_retry(self, trade: Dict[str, Any], signal: Dict[str, Any], retry_count: int) -> bool:
        """Internal method with retry logic."""
        if self.sheet is None:
            if not self.connect():
                return False

        try:
            trade_id = trade.get("trade_id", "")
            status = trade.get("status", "OPEN")
            row_data = self._build_row_data(trade, signal)

            # Always try to find the row first
            row_num = self._find_row_by_trade_id(trade_id)

            if row_num > 0:
                if status == "CLOSED":
                    # Update close_time (21), status (22), result (23), profit_usd (24), exit_reason (26), max_level (27)
                    updates = [
                        {"range": f"U{row_num}", "values": [[row_data[20]]]}, # 21. close_time
                        {"range": f"V{row_num}", "values": [[row_data[21]]]}, # 22. status
                        {"range": f"W{row_num}", "values": [[row_data[22]]]}, # 23. result
                        {"range": f"X{row_num}", "values": [[row_data[23]]]}, # 24. profit_usd
                        {"range": f"Z{row_num}", "values": [[row_data[25]]]}, # 26. exit_reason
                        {"range": f"AA{row_num}", "values": [[row_data[26]]]}, # 27. max_level_reached
                    ]
                else:
                    # Update open_time (19), status (22), result (23) when transitioning PENDING -> OPEN or TP hits
                    updates = [
                        {"range": f"S{row_num}", "values": [[row_data[18]]]}, # 19. open_time
                        {"range": f"V{row_num}", "values": [[row_data[21]]]}, # 22. status
                        {"range": f"W{row_num}", "values": [[row_data[22]]]}, # 23. result
                    ]
                self.sheet.batch_update(updates)
                logger.info(f"Trade {trade_id} {status} — updated row {row_num} in Google Sheet.")
                return True
            else:
                # Row not found, append full row
                self.sheet.append_row(row_data)
                logger.info(f"Trade {trade_id} {status} — appended new row to Google Sheet.")
                return True

        except Exception as e:
            logger.error(f"Error logging trade to Google Sheet: {e}")
            if retry_count > 0:
                logger.info("Retrying Google Sheet log_trade...")
                time.sleep(2)
                self.connect()
                return self._log_trade_with_retry(trade, signal, retry_count - 1)
            return False

    def sync_all_closed_trades(self, state_engine: Any) -> int:
        """
        Fetch all CLOSED trades and ensure they are in the sheet.

        Args:
            state_engine: The StateEngine instance.

        Returns:
            int: Number of rows added.
        """
        if self.sheet is None:
            if not self.connect():
                return 0

        try:
            # Get existing notes to find already synced trade IDs
            all_trade_ids = self.sheet.col_values(1)
            synced_trade_ids = set([str(tid).strip() for tid in all_trade_ids if tid])

            # Fetch closed trades from state engine
            closed_trades = []
            if hasattr(state_engine, "_get_connection") and hasattr(state_engine, "lock"):
                with state_engine.lock:
                    conn = state_engine._get_connection()
                    conn.row_factory = __import__('sqlite3').Row
                    try:
                        cursor = conn.execute("SELECT * FROM trades_executed WHERE status = 'CLOSED'")
                        closed_trades = [dict(row) for row in cursor.fetchall()]
                    except Exception as e:
                        logger.error(f"DB error fetching closed trades for sync: {e}")
                    finally:
                        conn.close()

            added_count = 0
            for trade in closed_trades:
                trade_id = trade.get("trade_id", "")
                if trade_id and trade_id not in synced_trade_ids:
                    # Need the corresponding signal to properly fill the row
                    signal_id = trade.get("signal_id", "")
                    signal = state_engine.get_signal(signal_id) if hasattr(state_engine, "get_signal") else {}
                    if not signal:
                        signal = {}
                    
                    if self.log_trade(trade, signal):
                        added_count += 1
                        # Throttle slightly to avoid API rate limits
                        time.sleep(1)

            if added_count > 0:
                logger.info(f"Synced {added_count} missing closed trades to Google Sheet.")
            return added_count

        except Exception as e:
            logger.error(f"Error syncing closed trades to Google Sheet: {e}")
            return 0

    def log_denied_trade(self, signal: Dict[str, Any], reason: str) -> bool:
        """
        Log a denied/skipped signal to the 'Rejected' tab in the Google Sheet.
        """
            
        if self.denied_sheet is None:
            if not self.connect():
                return False
                
        try:
            now_utc = datetime.now(pytz.utc)
            now_ist = now_utc.astimezone(_IST)
            
            row_data = [
                now_ist.strftime("%Y-%m-%d"),
                now_ist.strftime("%H:%M:%S"),
                signal.get("pair", ""),
                signal.get("strategy", ""),
                signal.get("direction", ""),
                reason,
                signal.get("spread_pips", 0.0),
                signal.get("score", 0.0),
                signal.get("signal_id", ""),
            ]
            
            self.denied_sheet.append_row(row_data)
            logger.info(f"Signal {signal.get('signal_id')} (Denied: {reason}) logged to Denied Google Sheet.")
            return True
        except Exception as e:
            logger.error(f"Error logging denied trade to Google Sheet: {e}")
            return False

    def log_vault_eod(self, date_str: str, profit: float, transferred: float, vault_bal: float, trading_bal: float, risk_amt: float) -> bool:
        """
        Logs the End of Day Vault summary to the Vault sheet.
        """
        if not self.vault_sheet:
            if not self.connect():
                return False

        now_ist = datetime.now(pytz.utc).astimezone(_IST).strftime("%H:%M:%S")

        row_data = [
            date_str,
            now_ist,
            round(profit, 2),
            round(transferred, 2),
            round(vault_bal, 2),
            round(trading_bal, 2),
            round(risk_amt, 2)
        ]

        try:
            self.vault_sheet.append_row(row_data)
            logger.info(f"Successfully logged EOD Vault data for {date_str} to Google Sheet.")
            return True
        except Exception as e:
            logger.error(f"Error logging EOD Vault to Google Sheet: {e}")
            return False
