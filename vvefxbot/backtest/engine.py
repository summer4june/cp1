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
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import pytz
from core.logger import get_logger
from core.configengine import Config
from core.stateengine import StateEngine
from modules.riskengine import RiskEngine
from modules.correlationfilter import CorrelationFilter
from modules.sessionengine import SessionEngine
from backtest.connector import BacktestConnector

logger = get_logger("BacktestEngine")

class HistoricalSessionEngine(SessionEngine):
    """Overrides SessionEngine during backtesting to use the historical bar timestamp."""

    def __init__(self, config: Config, connector: BacktestConnector):
        super().__init__(config)
        self.connector = connector

    def get_current_ist_time(self) -> datetime:
        utc_time = self.connector.current_time()
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=pytz.utc)
        return utc_time.astimezone(self.tz_ist)


# Minimum number of M1 bars needed before scanning starts (warm-up period)
_WARMUP_BARS = 120


class SimulatedTrade:
    """Tracks the lifecycle of one simulated trade during backtesting."""

    def __init__(
        self, trade_id: str, signal_id: str, pair: str, direction: str,
        entry: float, sl: float, tp1: float, tp2: float,
        lot: float, bar_index: int, bar_time: datetime,
        pip_size: float = 0.0001,
        use_partial_tp: bool = False,
        partial_tp_fraction: float = 0.5,
        be_buffer_pips: float = 30.0,
        session: str = "",
        entry_leg: str = "",
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
        self.pip_size = pip_size
        self.open_bar = bar_index
        self.open_time = bar_time
        self.close_bar: Optional[int] = None
        self.close_time: Optional[datetime] = None
        self.session = session
        self.entry_leg = entry_leg

        # use_partial_tp=True  (ZGMT): close partial_tp_fraction at TP1,
        #                              move SL to BE+buffer, close rest at TP2.
        # use_partial_tp=False (MMXM legacy): same split logic, SL moves to exact BE.
        self.use_partial_tp = use_partial_tp
        self.partial_tp_fraction = partial_tp_fraction
        self.be_buffer_pips = be_buffer_pips

        # State
        self.tp1_hit = False
        self.be_moved = False
        self.current_sl = sl
        self.remaining_lot = lot
        self.status = "OPEN"    # OPEN | CLOSED
        self.result = None      # WIN | LOSS | BREAKEVEN
        self.profit_usd = 0.0
        self.partial_profit = 0.0   # P&L locked at TP1 partial close
        self.exit_price = 0.0
        self.exit_reason = ""   # TP1, TP2, SL, BE_SL

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "signal_id": self.signal_id,
            "pair": self.pair,
            "direction": self.direction,
            "year": self.open_time.year,
            "session": self.session,
            "entry_leg": self.entry_leg,
            "entry_price": round(self.entry, 5),
            "sl_price": round(self.sl, 5),
            "tp1_price": round(self.tp1, 5),
            "tp2_price": round(self.tp2, 5),
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
    Runs any strategy scanner on historical M1/M15/H1 data.

    Args:
        config (Config): Bot configuration.
        connector (BacktestConnector): Historical data connector.
        pair (str): Trading symbol to backtest.
        scanner: An instantiated scanner object with a .scan(pair, session, killzone) method.
                 Defaults to ScannerMMXM if not provided (backward-compatible).
        pip_value (float): USD value per pip per standard lot.
    """

    def __init__(
        self,
        config: Config,
        connector: BacktestConnector,
        pair: str,
        scanner=None,
        pip_value: float = 10.0,
    ):
        self.config = config
        self.connector = connector
        self.pair = pair

        # ── Pip value: USD profit per pip per standard lot (100k units) ──────
        # XAUUSD: 1 pip = 0.01 USD/oz, lot = 100 oz → $1.00/pip
        # JPY pairs: 1 pip = 0.01 JPY, pip value = 1000/price USD/pip (approx)
        # USD-quoted pairs: 1 pip = 0.0001, lot = 100k → $10.00/pip
        pair_upper = pair.upper()
        if "XAU" in pair_upper:
            self.pip_value = 1.0
        elif "JPY" in pair_upper:
            # Dynamic: fetch current price for accurate per-pip USD value
            try:
                candles = connector.get_candles(pair, "M1", count=1)
                if candles is not None and not candles.empty:
                    price = float(candles.iloc[-1]["close"])
                    self.pip_value = round(1000.0 / price, 4) if price else 9.2
                else:
                    self.pip_value = 9.2  # ~1000/109 fallback (USDJPY ~155 → ~6.45)
            except Exception:
                self.pip_value = 9.2
        else:
            self.pip_value = pip_value  # $10 default for USD-quoted pairs

        self.pip_size = 0.01 if ("JPY" in pair_upper or "XAU" in pair_upper) else 0.0001

        # Warn if breakeven buffer > TP1 distance (structural runner issue)
        tm_cfg_init = getattr(config, "trade_management", {})
        be_buf = float(tm_cfg_init.get("breakeven_buffer_pips", 30))
        sl_fx  = float(getattr(config, "zgmt_scanner", {}).get("sl_pips_fx", 25))
        if be_buf > sl_fx and "XAU" not in pair_upper:
            logger.warning(
                f"[BT] ⚠️  breakeven_buffer_pips ({be_buf:.0f}) > sl_pips_fx ({sl_fx:.0f}) for {pair}. "
                f"Runner SL will be placed {be_buf - sl_fx:.0f} pips ABOVE TP1, "
                f"causing almost all runners to stop immediately. "
                f"Consider setting breakeven_buffer_pips <= {sl_fx:.0f} in config.json."
            )

        # Create a minimal in-memory StateEngine for the scanner cooldown tracking
        self.state = StateEngine(":memory:")

        # Use injected scanner, or fall back to MMXM (backward-compatible)
        if scanner is not None:
            self.scanner = scanner
        else:
            from modules.scannermmxm import ScannerMMXM
            self.scanner = ScannerMMXM(config, connector, self.state)

        self.risk_engine = RiskEngine(config, connector)
        self.corr_filter = CorrelationFilter(config)
        self.session_engine = HistoricalSessionEngine(config, connector)

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

        # ZGMT optimization setup: parse window bounds once outside the loop
        is_zgmt = (self.scanner.__class__.__name__ == "ScannerZGMT")
        w_start = None
        w_end = None

        # Read trade-management config once for ZGMT partial TP simulation
        tm_cfg = getattr(self.config, "trade_management", {})
        bt_partial_tp_fraction = float(tm_cfg.get("partial_tp_fraction", 0.5))
        bt_be_buffer_pips = float(tm_cfg.get("breakeven_buffer_pips", 30))

        if is_zgmt:
            zgmt_cfg = getattr(self.config, "zgmt_scanner", {})
            window_start_str = zgmt_cfg.get("zgmt_window_start_ist", "05:30")
            window_end_str = zgmt_cfg.get("zgmt_window_end_ist", "08:00")

            def _parse_time(s: str):
                parts = s.split(":")
                from datetime import time as dt_time
                return dt_time(int(parts[0]), int(parts[1]))

            try:
                w_start = _parse_time(window_start_str)
                w_end = _parse_time(window_end_str)
            except Exception:
                from datetime import time as dt_time
                w_start = dt_time(5, 30)
                w_end = dt_time(8, 0)

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

            # ── ZGMT specific optimization: only scan within the ZGMT signal window ──
            if is_zgmt:
                # Convert current_time (UTC) to IST (UTC + 5:30)
                if current_time.tzinfo is None:
                    current_time_utc = current_time.replace(tzinfo=timezone.utc)
                else:
                    current_time_utc = current_time.astimezone(timezone.utc)
                
                # ZGMT works in IST
                current_time_ist = current_time_utc + timedelta(hours=5, minutes=30)
                current_ist_time = current_time_ist.time()
                
                if not (w_start <= current_ist_time <= w_end):
                    continue

            # ── Skip scanning if outside session / killzone ────────────
            session = self.session_engine.get_active_session()
            killzone = self.session_engine.get_active_killzone()
            if not is_zgmt and not killzone:
                continue

            # ── Skip if max open trades reached ───────────────────────
            if len(self._open_trades) >= self.config.max_open_trades:
                continue

            # ── Signal cooldown: skip scanning for 15 bars after entry ─
            if bars_since_signal < 15:
                continue

            # ── Run scanner ────────────────────────────────────────────
            signals_raw = self.scanner.scan(self.pair, session or "London", killzone)
            if not signals_raw:
                continue

            signals_list = signals_raw if isinstance(signals_raw, list) else [signals_raw]

            for signal in signals_list:
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

                # ── Lot size ────────────────────────────────────────────────
                lot = risk_result["lot_size"]
                # Respect strategy-level fixed lot override if set in signal
                fixed_lot = signal.get("fixed_lot_size", 0.0)
                if fixed_lot and fixed_lot > 0.0:
                    lot = round(fixed_lot, 2)

                # ── Determine fill price ────────────────────────────────────
                # For ZGMT: the strategy places a limit order AT the 0 GMT open price.
                # The M1 bar at 00:00 UTC always includes the 0 GMT price in its range
                # (it opens at that price), so the fill is guaranteed at 00:00.
                # The scanner fires at 00:15 to confirm the setup. We retroactively fill
                # at the 00:00 bar and replay exit checks for the intervening bars.
                # For other strategies: market-order fill at bar close (existing behaviour).
                signal_entry = signal.get("entry_price", current_bar["close"])
                is_zgmt_signal = (signal.get("strategy") == "ZGMT")

                if is_zgmt_signal:
                    entry_price = signal_entry  # Fill at the exact 0 GMT price

                    # Compute SL/TP from the fill price using pip distances
                    sl_pips = signal["sl_pips"]
                    tp_pips = signal["tp_pips"]
                    pip_size = self.pip_size
                    sl_diff = sl_pips * pip_size
                    tp_diff = tp_pips * pip_size
                    if signal["direction"] == "BUY":
                        sl_price  = round(entry_price - sl_diff, 5)
                        tp1_price = round(entry_price + sl_diff, 5)   # TP1 = 1R
                        tp2_price = round(entry_price + tp_diff, 5)   # TP2 = 2R
                    else:
                        sl_price  = round(entry_price + sl_diff, 5)
                        tp1_price = round(entry_price - sl_diff, 5)
                        tp2_price = round(entry_price - tp_diff, 5)

                    # Find the 00:00 UTC bar for today (scan back ≤ 25 bars)
                    if current_time.tzinfo is None:
                        current_time_utc = current_time.replace(tzinfo=timezone.utc)
                    else:
                        current_time_utc = current_time.astimezone(timezone.utc)

                    target_0gmt = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                    if target_0gmt.tzinfo is not None:
                        target_0gmt = target_0gmt.replace(tzinfo=None)  # m1_data is naive UTC

                    day_start_idx = idx
                    fill_time = current_bar["time"]

                    for lookback_i in range(idx, max(-1, idx - 25), -1):
                        bar_t = m1_data.iloc[lookback_i]["time"]
                        if bar_t == target_0gmt:
                            day_start_idx = lookback_i
                            fill_time = bar_t
                            break

                    # Fetch live BE buffer and partial TP ratio from global config
                    bt_be_buffer_pips = self.config.be_buffer_pips
                    bt_partial_tp_fraction = self.config.partial_tp_fraction

                    trade = SimulatedTrade(
                        trade_id=str(uuid.uuid4()),
                        signal_id=signal_id,
                        pair=self.pair,
                        direction=signal["direction"],
                        entry=entry_price,
                        sl=sl_price,
                        tp1=tp1_price,
                        tp2=tp2_price,
                        lot=lot,
                        bar_index=day_start_idx,
                        bar_time=fill_time,
                        pip_size=self.pip_size,
                        use_partial_tp=True,
                        partial_tp_fraction=bt_partial_tp_fraction,
                        be_buffer_pips=bt_be_buffer_pips,
                        session=signal.get("session", ""),
                        entry_leg=signal.get("entry_leg", ""),
                    )

                    logger.info(
                        f"[BT] Trade OPENED | {self.pair} {signal['direction']} | "
                        f"FillBar {day_start_idx} (0GMT) | Entry: {entry_price:.5f} | "
                        f"SL: {trade.sl:.5f} | TP: {trade.tp2:.5f} (2R) | "
                        f"Lot: {lot} | Score: {signal['score']}"
                    )

                    # Retroactively replay exit checks for bars between 00:00 and now (inclusive)
                    trade_closed_early = False
                    for replay_i in range(day_start_idx, idx + 1):
                        replay_bar = m1_data.iloc[replay_i]
                        replay_t = replay_bar["time"]
                        if self._check_exits(trade, replay_bar, replay_i, replay_t):
                            trade_closed_early = True
                            break

                    if not trade_closed_early:
                        self._open_trades.append(trade)

                    bars_since_signal = 0
                    continue  # Skip the generic trade creation below

                else:
                    entry_price = current_bar["close"]  # Market order at bar close
                    sl_price  = signal["sl_price"]
                    tp1_price = signal["tp1_price"]
                    tp2_price = signal["tp2_price"]

                trade = SimulatedTrade(
                    trade_id=str(uuid.uuid4()),
                    signal_id=signal_id,
                    pair=self.pair,
                    direction=signal["direction"],
                    entry=entry_price,
                    sl=sl_price,
                    tp1=tp1_price,
                    tp2=tp2_price,
                    lot=lot,
                    bar_index=idx,
                    bar_time=current_time,
                    session=signal.get("session", ""),
                    entry_leg=signal.get("entry_leg", ""),
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
        trade: "SimulatedTrade",
        bar: pd.Series,
        bar_idx: int,
        bar_time: datetime,
    ) -> bool:
        """
        Check TP/SL exits for one trade against current bar OHLC.

        Exit priority (matching MT5 limit-order fill semantics):
          INITIAL phase (tp1_hit=False): SL beats TP1 on same bar (conservative).
          RUNNER phase  (tp1_hit=True):  TP2 beats SL on same bar (optimistic).
          SAME-BAR TP1+TP2: both processed immediately as WIN.
        """
        direction = trade.direction
        bar_high  = bar["high"]
        bar_low   = bar["low"]

        if direction == "BUY":
            sl_hit  = bar_low  <= trade.current_sl
            tp2_hit = bar_high >= trade.tp2
            tp1_hit = bar_high >= trade.tp1
        else:  # SELL
            sl_hit  = bar_high >= trade.current_sl
            tp2_hit = bar_low  <= trade.tp2
            tp1_hit = bar_low  <= trade.tp1

        # ══════════════════════════════════════════════════════════════════
        # ZGMT partial-TP mode
        # ══════════════════════════════════════════════════════════════════
        if trade.use_partial_tp:

            # ── RUNNER PHASE: TP1 already hit on a previous bar ──────────
            # TP2 is checked FIRST so a same-bar TP2+SL event credits the TP2.
            if trade.tp1_hit:

                if tp2_hit:
                    # ✅ TP2 WIN — TP2 limit order filled within bar range
                    pips      = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl   = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + tp2_pnl
                    trade.status      = "CLOSED"
                    trade.result      = "WIN"
                    trade.exit_reason = "TP2_HIT"
                    trade.exit_price  = trade.tp2
                    trade.profit_usd  = round(total_pnl, 2)
                    trade.close_bar   = bar_idx
                    trade.close_time  = bar_time
                    self._closed_trades.append(trade)
                    logger.info(
                        f"[BT] ✅ TP2 Hit | {trade.pair} | "
                        f"TP1 partial: {trade.partial_profit:+.2f} | "
                        f"TP2 runner: {tp2_pnl:+.2f} | "
                        f"Total P&L: {total_pnl:+.2f}"
                    )
                    return True

                if sl_hit:
                    # 🔶 Runner stopped — SL moved to BE+buffer, locked partial profit
                    pips      = self._calc_pips(trade.entry, trade.current_sl, direction)
                    sl_pnl    = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + sl_pnl
                    trade.status      = "CLOSED"
                    trade.result      = "BREAKEVEN"
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price  = trade.current_sl
                    trade.profit_usd  = round(total_pnl, 2)
                    trade.close_bar   = bar_idx
                    trade.close_time  = bar_time
                    self._closed_trades.append(trade)
                    logger.info(
                        f"[BT] 🔶 Runner SL | {trade.pair} | "
                        f"TP1 locked: {trade.partial_profit:+.2f} | "
                        f"Runner SL: {sl_pnl:+.2f} @ {trade.current_sl:.5f} | "
                        f"Total P&L: {total_pnl:+.2f}"
                    )
                    return True

                return False   # Runner still alive — neither TP2 nor SL reached

            # ── INITIAL PHASE: TP1 not yet hit ───────────────────────────
            # Conservative: SL takes priority over TP1 on same bar.
            if sl_hit:
                pips      = self._calc_pips(trade.entry, trade.current_sl, direction)
                loss      = round(pips * self.pip_value * trade.remaining_lot, 2)
                trade.status      = "CLOSED"
                trade.result      = "LOSS"
                trade.exit_reason = "SL_HIT"
                trade.exit_price  = trade.current_sl
                trade.profit_usd  = round(loss, 2)
                trade.close_bar   = bar_idx
                trade.close_time  = bar_time
                self._closed_trades.append(trade)
                logger.info(
                    f"[BT] ❌ SL Hit (pre-TP1) | {trade.pair} | "
                    f"P&L: {loss:+.2f}"
                )
                return True

            if tp1_hit:
                # Process TP1 partial close
                partial_lot   = round(trade.lot * trade.partial_tp_fraction, 8)
                remainder_lot = round(trade.lot - partial_lot, 8)
                pips_tp1      = self._calc_pips(trade.entry, trade.tp1, direction)
                partial_pnl   = round(pips_tp1 * self.pip_value * partial_lot, 2)

                trade.partial_profit = partial_pnl
                trade.remaining_lot  = max(round(remainder_lot, 8), 0.0)
                trade.tp1_hit  = True
                trade.be_moved = True

                # Move SL to entry + buffer pips
                buffer = trade.be_buffer_pips * trade.pip_size
                if direction == "BUY":
                    trade.current_sl = round(trade.entry + buffer, 5)
                else:
                    trade.current_sl = round(trade.entry - buffer, 5)

                logger.info(
                    f"[BT] 📊 TP1 Hit | {trade.pair} | "
                    f"Closed {partial_lot:.3f} lot @ {trade.tp1:.5f} | "
                    f"P&L locked: {partial_pnl:+.2f} | "
                    f"SL → BE+{trade.be_buffer_pips:.0f}pips @ {trade.current_sl:.5f} | "
                    f"Remaining: {trade.remaining_lot:.3f} lot"
                )

                # Same-bar TP2 hit: close remainder immediately
                if tp2_hit:
                    pips_tp2  = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl   = round(pips_tp2 * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + tp2_pnl
                    trade.status      = "CLOSED"
                    trade.result      = "WIN"
                    trade.exit_reason = "TP2_HIT"
                    trade.exit_price  = trade.tp2
                    trade.profit_usd  = round(total_pnl, 2)
                    trade.close_bar   = bar_idx
                    trade.close_time  = bar_time
                    self._closed_trades.append(trade)
                    logger.info(
                        f"[BT] ✅ TP1+TP2 same bar | {trade.pair} | "
                        f"TP1: {partial_pnl:+.2f} | TP2: {tp2_pnl:+.2f} | "
                        f"Total P&L: {total_pnl:+.2f}"
                    )
                    return True

            return False   # Still open

        # ══════════════════════════════════════════════════════════════════
        # Legacy MMXM split mode: SL always takes priority (conservative)
        # ══════════════════════════════════════════════════════════════════
        if sl_hit:
            pips      = self._calc_pips(trade.entry, trade.current_sl, direction)
            loss      = round(pips * self.pip_value * trade.remaining_lot, 2)
            total_pnl = trade.partial_profit + loss
            trade.status      = "CLOSED"
            trade.result      = "BREAKEVEN" if trade.be_moved else "LOSS"
            trade.exit_reason = "SL_HIT"
            trade.exit_price  = trade.current_sl
            trade.profit_usd  = round(total_pnl, 2)
            trade.close_bar   = bar_idx
            trade.close_time  = bar_time
            self._closed_trades.append(trade)
            logger.info(
                f"[BT] {'🔶' if trade.be_moved else '❌'} SL Hit | {trade.pair} | "
                f"P&L: {trade.profit_usd:+.2f} | Result: {trade.result}"
            )
            return True

        # MMXM: TP2 hit (after TP1 already hit)
        if tp2_hit and trade.tp1_hit:
            pips      = self._calc_pips(trade.entry, trade.tp2, direction)
            tp2_pnl   = round(pips * self.pip_value * trade.remaining_lot, 2)
            total_pnl = trade.partial_profit + tp2_pnl
            trade.status      = "CLOSED"
            trade.result      = "WIN"
            trade.exit_reason = "TP2_HIT"
            trade.exit_price  = trade.tp2
            trade.profit_usd  = round(total_pnl, 2)
            trade.close_bar   = bar_idx
            trade.close_time  = bar_time
            self._closed_trades.append(trade)
            logger.info(
                f"[BT] ✅ TP2 Hit | {trade.pair} | P&L: {trade.profit_usd:+.2f}"
            )
            return True

        # MMXM: TP1 hit (first time)
        if tp1_hit and not trade.tp1_hit:
            half_lot = max(0.01, round(trade.lot / 2, 2))
            pips     = self._calc_pips(trade.entry, trade.tp1, direction)
            partial  = round(pips * self.pip_value * half_lot, 2)
            trade.partial_profit = partial
            trade.remaining_lot  = max(0.01, round(trade.lot - half_lot, 2))
            trade.tp1_hit  = True
            trade.be_moved = True
            trade.current_sl = trade.entry   # MMXM: SL to exact BE
            logger.info(
                f"[BT] 📊 TP1 Hit | {trade.pair} | "
                f"Partial locked: {partial:+.2f} | SL → BE"
            )

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
