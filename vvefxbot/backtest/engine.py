"""
BacktestEngine — Bar-by-bar MMXM signal replay and trade simulation.

Replays M1 bars chronologically. At each bar:
  1. Calls ScannerMMXM.scan() using data up to current bar.
  2. If an A+ signal is detected, simulates trade entry.
  3. Checks all open simulated trades against bar OHLC for TP1/TP2/SL hits.
  4. Collects all results and produces a performance report.
"""

import uuid
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from core.logger import get_logger
from core.configengine import Config
from core.stateengine import StateEngine
from modules.scannermmxm import ScannerMMXM
from modules.riskengine import RiskEngine
from modules.correlationfilter import CorrelationFilter
from modules.sessionengine import SessionEngine
from backtest.connector import BacktestConnector

logger = get_logger("BacktestEngine")

# Minimum number of M1 bars needed before scanning starts (warm-up period)
_WARMUP_BARS = 120


class SimulatedTrade:
    """Tracks the lifecycle of one simulated trade during backtesting."""

    def __init__(
        self, trade_id: str, signal_id: str, pair: str, direction: str,
        entry: float, sl: float, tp1: float, tp2: float,
        lot: float, bar_index: int, bar_time: datetime
    ):
        self.trade_id = trade_id
        self.signal_id = signal_id
        self.pair = pair
        self.direction = direction
        self.entry = entry
        self.sl = sl
        self.tp1 = tp1
        self.tp2 = tp2
        self.lot = lot
        self.open_bar = bar_index
        self.open_time = bar_time
        self.close_bar: Optional[int] = None
        self.close_time: Optional[datetime] = None

        # State
        self.tp1_hit = False
        self.be_moved = False
        self.current_sl = sl
        self.remaining_lot = lot
        self.status = "OPEN"    # OPEN | CLOSED
        self.result = None      # WIN | LOSS | BREAKEVEN
        self.profit_usd = 0.0
        self.partial_profit = 0.0   # Locked at TP1
        self.exit_price = 0.0
        self.exit_reason = ""   # TP1, TP2, SL, BE_SL

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "signal_id": self.signal_id,
            "pair": self.pair,
            "direction": self.direction,
            "entry": round(self.entry, 5),
            "sl": round(self.sl, 5),
            "tp1": round(self.tp1, 5),
            "tp2": round(self.tp2, 5),
            "lot": self.lot,
            "open_bar": self.open_bar,
            "open_time": str(self.open_time),
            "close_bar": self.close_bar,
            "close_time": str(self.close_time) if self.close_time else "",
            "status": self.status,
            "result": self.result or "",
            "profit_usd": round(self.profit_usd, 2),
            "exit_price": round(self.exit_price, 5),
            "exit_reason": self.exit_reason,
        }


class BacktestEngine:
    """
    Runs the full MMXM strategy on historical M1/M15/H1 data.

    Args:
        config (Config): Bot configuration.
        connector (BacktestConnector): Historical data connector.
        pair (str): Trading symbol to backtest.
        pip_value (float): USD value per pip per standard lot.
    """

    def __init__(
        self,
        config: Config,
        connector: BacktestConnector,
        pair: str,
        pip_value: float = 10.0,
    ):
        self.config = config
        self.connector = connector
        self.pair = pair
        self.pip_value = pip_value

        self.pip_size = 0.01 if "JPY" in pair.upper() else 0.0001

        # Create a minimal in-memory StateEngine for the scanner cooldown tracking
        self.state = StateEngine(":memory:")

        # Plug BacktestConnector into scanner
        self.scanner = ScannerMMXM(config, connector, self.state)
        self.risk_engine = RiskEngine(config, connector)
        self.corr_filter = CorrelationFilter(config)
        self.session_engine = SessionEngine(config)

        self._open_trades: List[SimulatedTrade] = []
        self._closed_trades: List[SimulatedTrade] = []
        self._signals_fired: int = 0

    # ------------------------------------------------------------------
    # MAIN RUN
    # ------------------------------------------------------------------

    def run(self) -> List[dict]:
        """
        Execute the full backtest replay.

        Returns:
            List[dict]: One dict per closed trade.
        """
        m1_data = self.connector.data["M1"]
        total_bars = len(m1_data)
        logger.info(f"[BT] Starting replay: {self.pair} | {total_bars} M1 bars")

        # Deduplicate signals: don't re-enter a trade on the very next bar
        bars_since_signal = 999

        for idx in range(_WARMUP_BARS, total_bars):
            self.connector.set_bar_index(idx)
            current_bar = m1_data.iloc[idx]
            current_time = current_bar["time"]

            # ── Check existing open trades first ───────────────────────
            still_open = []
            for trade in self._open_trades:
                closed = self._check_exits(trade, current_bar, idx, current_time)
                if not closed:
                    still_open.append(trade)
            self._open_trades = still_open

            bars_since_signal += 1

            # ── Skip scanning if outside session / killzone ────────────
            session = self.session_engine.get_active_session()
            killzone = self.session_engine.get_active_killzone()
            if not killzone:
                continue

            # ── Skip if max open trades reached ───────────────────────
            if len(self._open_trades) >= self.config.max_open_trades:
                continue

            # ── Signal cooldown: skip scanning for 15 bars after entry ─
            if bars_since_signal < 15:
                continue

            # ── Run scanner ────────────────────────────────────────────
            signal = self.scanner.scan(self.pair, session or "London", killzone)
            if signal is None:
                continue

            self._signals_fired += 1
            signal_id = signal["signal_id"]
            spread_pips = signal.get("spread_pips", 0.0)

            # Risk checks
            open_trade_dicts = [
                {"pair": t.pair, "direction": t.direction,
                 "lot_total": t.lot, "risk_amount": self.config.trading_pool_size * self.config.risk_percent / 100}
                for t in self._open_trades
            ]
            risk_result = self.risk_engine.run_all_checks(signal, open_trade_dicts)
            if not risk_result["pass"]:
                logger.debug(f"[BT] Signal blocked by risk: {risk_result['failed_check']}")
                continue

            # Correlation check
            allowed, reason = self.corr_filter.can_trade(
                self.pair, open_trade_dicts, signal["direction"]
            )
            if not allowed:
                logger.debug(f"[BT] Signal blocked by correlation: {reason}")
                continue

            # Simulate trade entry
            lot = risk_result["lot_size"]
            entry_price = current_bar["close"]   # Market order at bar close
            trade = SimulatedTrade(
                trade_id=str(uuid.uuid4()),
                signal_id=signal_id,
                pair=self.pair,
                direction=signal["direction"],
                entry=entry_price,
                sl=signal["sl_price"],
                tp1=signal["tp1_price"],
                tp2=signal["tp2_price"],
                lot=lot,
                bar_index=idx,
                bar_time=current_time,
            )
            self._open_trades.append(trade)
            bars_since_signal = 0

            logger.info(
                f"[BT] Trade OPENED | {self.pair} {signal['direction']} | "
                f"Bar {idx} | Entry: {entry_price:.5f} | "
                f"SL: {trade.sl:.5f} | TP1: {trade.tp1:.5f} | TP2: {trade.tp2:.5f} | "
                f"Lot: {lot} | Score: {signal['score']}"
            )

        # Force-close any remaining open trades at last bar close
        last_bar = m1_data.iloc[-1]
        for trade in self._open_trades:
            trade.status = "CLOSED"
            trade.result = "EXPIRED"
            trade.exit_reason = "SESSION_END"
            trade.close_bar = total_bars - 1
            trade.close_time = last_bar["time"]
            trade.exit_price = last_bar["close"]
            pips = self._calc_pips(trade.entry, last_bar["close"], trade.direction)
            trade.profit_usd = round(pips * self.pip_value * trade.remaining_lot, 2)
            self._closed_trades.append(trade)
            logger.info(f"[BT] Trade EXPIRED at session end: {trade.trade_id}")

        logger.info(
            f"[BT] Replay complete | Signals: {self._signals_fired} | "
            f"Trades: {len(self._closed_trades)}"
        )
        return [t.to_dict() for t in self._closed_trades]

    # ------------------------------------------------------------------
    # EXIT MANAGEMENT
    # ------------------------------------------------------------------

    def _check_exits(
        self,
        trade: SimulatedTrade,
        bar: pd.Series,
        bar_idx: int,
        bar_time: datetime,
    ) -> bool:
        """
        Check TP1/TP2/SL exits for one trade against current bar OHLC.

        Returns True if the trade was closed, False if still open.
        Conservative assumption: if SL and TP hit same bar, SL wins.
        """
        direction = trade.direction
        bar_high = bar["high"]
        bar_low = bar["low"]

        if direction == "BUY":
            sl_hit = bar_low <= trade.current_sl
            tp1_hit = bar_high >= trade.tp1
            tp2_hit = bar_high >= trade.tp2
        else:  # SELL
            sl_hit = bar_high >= trade.current_sl
            tp1_hit = bar_low <= trade.tp1
            tp2_hit = bar_low <= trade.tp2

        # SL hit — always takes priority
        if sl_hit:
            pips = self._calc_pips(trade.entry, trade.current_sl, direction)
            loss = round(pips * self.pip_value * trade.remaining_lot, 2)
            total_profit = trade.partial_profit + loss
            trade.status = "CLOSED"
            trade.result = "BREAKEVEN" if trade.be_moved and total_profit >= -0.5 else "LOSS"
            trade.exit_reason = "SL_HIT"
            trade.exit_price = trade.current_sl
            trade.profit_usd = round(total_profit, 2)
            trade.close_bar = bar_idx
            trade.close_time = bar_time
            self._closed_trades.append(trade)
            logger.info(
                f"[BT] ❌ SL Hit | {trade.pair} | "
                f"P&L: {trade.profit_usd:+.2f} | Result: {trade.result}"
            )
            return True

        # TP2 hit (after TP1 already hit)
        if tp2_hit and trade.tp1_hit:
            pips = self._calc_pips(trade.entry, trade.tp2, direction)
            tp2_profit = round(pips * self.pip_value * trade.remaining_lot, 2)
            total_profit = trade.partial_profit + tp2_profit
            trade.status = "CLOSED"
            trade.result = "WIN"
            trade.exit_reason = "TP2_HIT"
            trade.exit_price = trade.tp2
            trade.profit_usd = round(total_profit, 2)
            trade.close_bar = bar_idx
            trade.close_time = bar_time
            self._closed_trades.append(trade)
            logger.info(
                f"[BT] ✅ TP2 Hit | {trade.pair} | P&L: {trade.profit_usd:+.2f}"
            )
            return True

        # TP1 hit (first time)
        if tp1_hit and not trade.tp1_hit:
            half_lot = max(0.01, round(trade.lot / 2, 2))
            pips = self._calc_pips(trade.entry, trade.tp1, direction)
            partial = round(pips * self.pip_value * half_lot, 2)
            trade.partial_profit = partial
            trade.remaining_lot = max(0.01, round(trade.lot - half_lot, 2))
            trade.tp1_hit = True
            trade.be_moved = True
            trade.current_sl = trade.entry   # SL moved to breakeven
            logger.info(
                f"[BT] 📊 TP1 Hit | {trade.pair} | "
                f"Partial locked: {partial:+.2f} | SL → BE"
            )
            # Not closed yet — continue monitoring for TP2

        return False

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _calc_pips(self, entry: float, exit_price: float, direction: str) -> float:
        """Calculate pips P&L. Positive = profit, negative = loss."""
        if direction == "BUY":
            return (exit_price - entry) / self.pip_size
        else:
            return (entry - exit_price) / self.pip_size
