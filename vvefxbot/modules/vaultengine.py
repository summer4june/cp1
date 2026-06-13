import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any

from core.logger import get_logger
from core.stateengine import StateEngine
from core.mt5connector import MT5Connector

logger = get_logger("VaultEngine")

class VaultEngine:
    """Manages the Trading Balance, Vault splits, and -20% Drawdown kill-switch."""

    def __init__(self, config_path: str = "core/vault_config.json", db_path: str = "vault.sqlite"):
        self.config_path = config_path
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initializes the vault history database."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vault_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    daily_profit REAL,
                    transferred_to_vault REAL,
                    trading_balance REAL,
                    vault_balance REAL,
                    risk_amount_set REAL,
                    timestamp TEXT
                )
            """)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing vault DB: {e}")
        finally:
            conn.close()

    def get_vault_config(self) -> Dict[str, Any]:
        """Loads the current vault configuration."""
        if not os.path.exists(self.config_path):
            raise ValueError(f"Vault config {self.config_path} not found.")
        with open(self.config_path, "r") as f:
            return json.load(f)

    def save_vault_config(self, data: Dict[str, Any]) -> None:
        """Saves the vault configuration."""
        with open(self.config_path, "w") as f:
            json.dump(data, f, indent=4)

    def get_current_risk_amount(self) -> float:
        """
        Calculates the amount to risk per trade.
        Formula: Trading Balance / Divide Factor
        """
        config = self.get_vault_config()
        trading_balance = config.get("trading_balance", 70.0)
        divide_factor = config.get("divide_factor", 15)
        
        if divide_factor <= 0:
            divide_factor = 15

        return trading_balance / divide_factor

    def check_drawdown(self, state: StateEngine, mt5: MT5Connector, unrealized_pnl: float) -> bool:
        """
        Checks if Realized + Unrealized PnL <= -20% of Trading Balance.
        If yes, instantly closes all MT5 positions and disables the bot.
        
        Returns True if panic was triggered.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_state = state.get_daily_state(today)
        realized_pnl = daily_state.get("daily_profit_usd", 0.0)

        config = self.get_vault_config()
        trading_balance = config.get("trading_balance", 100.0)
        max_drawdown_pct = config.get("max_daily_drawdown_pct", 0.20)

        total_pnl = realized_pnl + unrealized_pnl
        max_loss_allowed = trading_balance * max_drawdown_pct

        if total_pnl <= -max_loss_allowed:
            # PANIC SWITCH
            logger.critical(f"🛑 DRAWDOWN PANIC: Total PnL ({total_pnl:.2f}) hit limit (-{max_loss_allowed:.2f}). CLOSING ALL POSITIONS!")
            
            # 1. Close all MT5 positions
            mt5.close_all_positions()
            
            # 2. Disable bot for the day
            state.disable_bot_today(today)
            return True
            
        return False

    def process_end_of_day(self, state: StateEngine, reporter=None) -> None:
        """
        Calculates EOD profit split, updates balances, saves history, and logs to Google Sheets.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_state = state.get_daily_state(today)
        
        # We assume daily_profit_usd accurately reflects all closed trades today
        daily_profit = daily_state.get("daily_profit_usd", 0.0)
        
        config = self.get_vault_config()
        trading_balance = config.get("trading_balance", 100.0)
        vault_balance = config.get("vault_balance", 0.0)
        min_trading_balance = config.get("min_trading_balance", 70.0)
        vault_pct = config.get("vault_percentage", 0.50)
        
        transferred_to_vault = 0.0
        
        if daily_profit > 0:
            transferred_to_vault = daily_profit * vault_pct
            remaining_profit = daily_profit - transferred_to_vault
            trading_balance += remaining_profit
            vault_balance += transferred_to_vault
        else:
            # Loss scenario
            trading_balance += daily_profit  # daily_profit is negative
            
            if trading_balance < min_trading_balance:
                deficit = min_trading_balance - trading_balance
                if vault_balance >= deficit:
                    vault_balance -= deficit
                    trading_balance = min_trading_balance
                    logger.info(f"Vault injected ${deficit:.2f} to restore minimum Trading Balance.")
                else:
                    # Vault depleted, take whatever is left
                    trading_balance += vault_balance
                    vault_balance = 0.0
                    logger.warning("Vault depleted! Trading Balance below minimum.")

        # Update Config
        config["trading_balance"] = round(trading_balance, 2)
        config["vault_balance"] = round(vault_balance, 2)
        self.save_vault_config(config)

        # Log to DB
        risk_amount = self.get_current_risk_amount()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO vault_history 
                (date, daily_profit, transferred_to_vault, trading_balance, vault_balance, risk_amount_set, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                today,
                round(daily_profit, 2),
                round(transferred_to_vault, 2),
                round(trading_balance, 2),
                round(vault_balance, 2),
                round(risk_amount, 2),
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()
            logger.info(f"EOD Processed: Profit=${daily_profit:.2f}, Vault=${transferred_to_vault:.2f}, New TB=${trading_balance:.2f}")
            
            # Log to Google Sheets
            if reporter:
                reporter.log_vault_eod(today, daily_profit, transferred_to_vault, vault_balance, trading_balance, risk_amount)
                
        except sqlite3.Error as e:
            logger.error(f"Error logging EOD vault history: {e}")
        finally:
            conn.close()
