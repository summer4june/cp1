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
    "Date", "Time", "Pair", "Session", "Direction", "Entry", "SL", "TP1", "TP2",
    "Lot", "Risk%", "Spread", "Effective RR", "Result", "Profit/Loss", "Notes", "Signal Score"
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
            if not first_row or first_row[0] != "Date":
                self.sheet.insert_row(_HEADERS, 1)
                logger.info("Added header row to Google Sheet.")

            logger.info("Successfully connected to Google Sheet.")
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
        """Build the full row data list for a trade."""
        now_utc = datetime.now(pytz.utc)
        if "execution_time" in trade and trade["execution_time"]:
            try:
                now_utc = datetime.fromisoformat(trade["execution_time"].replace('Z', '+00:00'))
            except ValueError:
                pass

        now_ist = now_utc.astimezone(_IST)
        trade_id = trade.get("trade_id", "")
        bias_summary = signal.get("bias_summary", "") if signal else ""
        notes = f"TradeID:{trade_id} | {bias_summary}"

        return [
            now_ist.strftime("%Y-%m-%d"),
            now_ist.strftime("%H:%M:%S"),
            trade.get("pair") or signal.get("pair", "") if signal else trade.get("pair", ""),
            signal.get("session", "") if signal else "",
            trade.get("direction") or (signal.get("direction", "") if signal else ""),
            trade.get("executed_price", 0.0),
            trade.get("sl", 0.0),
            trade.get("tp1", 0.0),
            trade.get("tp2", 0.0),
            trade.get("lot_total", 0.0),
            self.config.risk_percent,
            signal.get("spread_pips", 0.0) if signal else 0.0,
            signal.get("effective_rr", 0.0) if signal else 0.0,
            trade.get("result", "") or "",
            trade.get("profit_usd", 0.0) or 0.0,
            notes,
            signal.get("score", 0.0) if signal else 0.0,
        ]

    def _find_row_by_trade_id(self, trade_id: str) -> int:
        """
        Find the 1-indexed row number in the sheet whose Notes cell contains
        'TradeID:<trade_id>'. Returns -1 if not found.
        """
        try:
            # Notes is the 16th column (index 15, 1-indexed col 16)
            notes_col = self.sheet.col_values(16)
            for i, cell_val in enumerate(notes_col):
                if f"TradeID:{trade_id}" in str(cell_val):
                    return i + 1  # 1-indexed
        except Exception as e:
            logger.warning(f"Could not search Notes column: {e}")
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

            if status == "CLOSED":
                # Try to find and update existing row
                row_num = self._find_row_by_trade_id(trade_id)
                if row_num > 0:
                    # Update Result (col 14) and Profit (col 15) in the existing row
                    self.sheet.update_cell(row_num, 14, trade.get("result", ""))
                    self.sheet.update_cell(row_num, 15, trade.get("profit_usd", 0.0))
                    logger.info(f"Trade {trade_id} CLOSED — updated row {row_num} in Google Sheet.")
                    return True
                else:
                    # Row not found (was never opened), append full row
                    logger.warning(f"Trade {trade_id} row not found in Sheet — appending as new row.")
                    self.sheet.append_row(row_data)
                    logger.info(f"Trade {trade_id} appended to Google Sheet (fallback).")
            else:
                # OPEN trade — always append new row
                self.sheet.append_row(row_data)
                logger.info(f"Trade {trade_id} OPEN — appended new row to Google Sheet.")

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
            # Assuming Notes is the 16th column (index 15)
            # To be safe and handle empty sheets, fetch all records or column values
            all_records = self.sheet.get_all_records()
            synced_trade_ids = set()
            for record in all_records:
                notes = str(record.get("Notes", ""))
                if "TradeID:" in notes:
                    # Extract the TradeID substring
                    parts = notes.split("TradeID:")
                    if len(parts) > 1:
                        tid = parts[1].split(" |")[0].strip()
                        synced_trade_ids.add(tid)

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
