"""
BacktestConnector — Drop-in replacement for MT5Connector during backtesting.

Implements the same public interface as MT5Connector so that ScannerMMXM,
RiskEngine, and all other modules run without any modification.
"""

import pandas as pd
from types import SimpleNamespace
from datetime import datetime
from typing import Dict, Any, Optional
from core.logger import get_logger
from core.configengine import Config

logger = get_logger("BacktestConnector")


class BacktestConnector:
    """
    Simulates MT5Connector using historical OHLC DataFrames.

    Data format expected (for each timeframe):
        DataFrame with columns: time (datetime), open, high, low, close, tick_volume

    Usage:
        connector = BacktestConnector(config, {
            'M1':  m1_df,
            'M15': m15_df,
            'H1':  h1_df,
        }, symbol='EURUSD')
        connector.set_bar_index(idx)   # advance replay cursor
    """

    def __init__(self, config: Config, data: Dict[str, pd.DataFrame], symbol: str):
        self.config = config
        self.data = data          # {'M1': df, 'M15': df, 'H1': df}
        self.symbol = symbol
        self.current_bar_idx = 0  # Index in M1 timeline

        # Simulated MT5 state
        self._open_positions: Dict[int, dict] = {}
        self._closed_records: Dict[int, dict] = {}  # ticket -> {profit, commission, swap}
        self._next_ticket = 1_000_001

    # ------------------------------------------------------------------
    # REPLAY CURSOR
    # ------------------------------------------------------------------

    def set_bar_index(self, idx: int) -> None:
        """Advance the replay cursor to M1 bar index idx."""
        self.current_bar_idx = idx

    def current_time(self) -> datetime:
        """Return the timestamp of the current M1 replay bar."""
        return self.data["M1"].iloc[self.current_bar_idx]["time"]

    # ------------------------------------------------------------------
    # MT5Connector INTERFACE — used by ScannerMMXM
    # ------------------------------------------------------------------

    def get_candles(self, symbol: str, timeframe: str, count: int = 100) -> pd.DataFrame:
        """Return up to `count` historical bars ending at the current replay bar."""
        df = self.data.get(timeframe, pd.DataFrame())
        if df.empty:
            logger.warning(f"No data for timeframe {timeframe}")
            return df

        now = self.current_time()
        available = df[df["time"] <= now].tail(count).reset_index(drop=True)
        return available

    def get_current_spread(self, symbol: str) -> float:
        """Return configured spread limit as a fixed spread for simulation."""
        return self.config.spread_limits.get(symbol, 1.5)

    def is_connected(self) -> bool:
        return True

    def get_account_balance(self) -> float:
        return self.config.trading_pool_size

    # ------------------------------------------------------------------
    # MT5Connector INTERFACE — used by ExecutionEngine
    # ------------------------------------------------------------------

    def place_order(
        self, symbol: str, order_type: str, lot: float,
        sl: float, tp: float, comment: str = "BT"
    ) -> Dict[str, Any]:
        """Simulate a market order fill at the current bar's close price."""
        bar = self.data["M1"].iloc[self.current_bar_idx]
        fill_price = bar["close"]

        ticket = self._next_ticket
        self._next_ticket += 1

        self._open_positions[ticket] = {
            "ticket": ticket,
            "symbol": symbol,
            "type": 0 if order_type.upper() == "BUY" else 1,
            "volume": lot,
            "price_open": fill_price,
            "sl": sl,
            "tp": tp,
            "profit": 0.0,
        }
        logger.info(f"[BT] Order placed: {symbol} {order_type} {lot} @ {fill_price:.5f} | Ticket: {ticket}")
        return {"success": True, "ticket": ticket, "error": ""}

    def positions_get(self, ticket: int = None):
        """Return simulated open position(s)."""
        if ticket is not None:
            pos = self._open_positions.get(ticket)
            return [SimpleNamespace(**pos)] if pos else []
        return [SimpleNamespace(**p) for p in self._open_positions.values()]

    def close_partial(self, ticket: int, lot: float) -> Dict[str, Any]:
        """Simulate a partial close at the current bar's close price."""
        pos = self._open_positions.get(ticket)
        if not pos:
            return {"success": False, "error": f"Ticket {ticket} not found"}

        bar = self.data["M1"].iloc[self.current_bar_idx]
        close_price = bar["close"]
        pip_size = 0.01 if "JPY" in self.symbol else 0.0001
        pip_value = self._pip_value_per_lot()

        if pos["type"] == 0:  # BUY
            pips = (close_price - pos["price_open"]) / pip_size
        else:  # SELL
            pips = (pos["price_open"] - close_price) / pip_size

        raw_profit = pips * pip_value * lot
        commission = -0.07 * lot  # Approximate $0.07 per 0.01 lot

        # Accumulate realized profit for this ticket
        if ticket not in self._closed_records:
            self._closed_records[ticket] = {"profit": 0.0, "commission": 0.0, "swap": 0.0}
        self._closed_records[ticket]["profit"] += raw_profit
        self._closed_records[ticket]["commission"] += commission

        # Reduce remaining volume
        remaining = round(pos["volume"] - lot, 2)
        if remaining <= 0.0:
            del self._open_positions[ticket]
        else:
            self._open_positions[ticket]["volume"] = remaining

        logger.info(
            f"[BT] Partial close: Ticket {ticket} | {lot} lot @ {close_price:.5f} | "
            f"P&L: {raw_profit:+.2f}"
        )
        return {"success": True, "error": ""}

    def modify_sl(self, ticket: int, new_sl: float) -> Dict[str, Any]:
        """Simulate an SL modification."""
        if ticket in self._open_positions:
            self._open_positions[ticket]["sl"] = new_sl
            logger.info(f"[BT] SL modified: Ticket {ticket} → {new_sl:.5f}")
            return {"success": True, "error": ""}
        return {"success": False, "error": "Position not found"}

    def get_historical_profit(self, ticket: int) -> float:
        """Return total realized P&L (profit + commission + swap) for a ticket."""
        rec = self._closed_records.get(ticket, {})
        return round(
            rec.get("profit", 0.0) + rec.get("commission", 0.0) + rec.get("swap", 0.0),
            2,
        )

    # ------------------------------------------------------------------
    # INTERNAL HELPERS
    # ------------------------------------------------------------------

    def _pip_value_per_lot(self) -> float:
        """Approximate pip value in USD for a 1-lot position."""
        pair = self.symbol.upper()
        if pair in ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"]:
            return 10.0
        elif "JPY" in pair:
            bar = self.data["M1"].iloc[self.current_bar_idx]
            price = bar["close"]
            return round((1000.0 / price), 4) if price else 10.0
        elif pair in ["USDCAD", "USDCHF"]:
            bar = self.data["M1"].iloc[self.current_bar_idx]
            price = bar["close"]
            return round(10.0 / price, 4) if price else 10.0
        return 10.0
