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
        entry: float, sl: float, tp1: float, tp2: float, tp3: float,
        lot: float, bar_index: int, bar_time: datetime,
        pip_size: float = 0.0001,
        use_partial_tp: bool = False,
        partial_tp_fraction: float = 0.5,
        be_buffer_pips: float = 2.0,
        session: str = "",
        entry_leg: str = "",
        is_limit: bool = False,
        rr_format: str = "1:2",
        score: float = 0.0,
    ):
        self.trade_id = trade_id
        self.signal_id = signal_id
        self.pair = pair
        self.direction = direction
        self.entry = entry
        self.sl = sl
        self.tp1 = tp1
        self.tp2 = tp2
        self.tp3 = tp3
        self.lot = lot
        self.pip_size = pip_size
        self.open_bar = bar_index
        self.open_time = bar_time
        self.close_bar: Optional[int] = None
        self.close_time: Optional[datetime] = None
        self.session = session
        self.entry_leg = entry_leg
        self.score = score

        # use_partial_tp=True  (ZGMT): close partial_tp_fraction at TP1,
        #                              move SL to BE+buffer, close rest at TP2.
        # use_partial_tp=False (MMXM legacy): same split logic, SL moves to exact BE.
        self.use_partial_tp = use_partial_tp
        self.partial_tp_fraction = partial_tp_fraction
        self.be_buffer_pips = be_buffer_pips

        self.rr_format = rr_format

        # State
        self.tp1_hit = False
        self.tp2_hit = False
        self.tp3_hit = False
        self.be_moved = False
        self.current_sl = sl
        self.remaining_lot = lot
        self.is_limit = is_limit
        self.status = "PENDING" if is_limit else "OPEN"    # PENDING | OPEN | CLOSED
        self.result = None      # WIN | LOSS | BREAKEVEN | CANCELLED
        self.profit_usd = 0.0
        self.partial_profit = 0.0   # P&L locked at TP1 partial close
        self.exit_price = 0.0
        self.exit_reason = ""   # TP1, TP2, SL, BE_SL, EXPIRED

    def to_dict(self):
        pip_size = getattr(self, 'pip_size', 0.0001)
        pair_upper = self.pair.upper()
        if "XAU" in pair_upper or "GOLD" in pair_upper:
            contract_size = 100
            margin_used = ((self.lot * contract_size) * self.entry) / 200
            pip_value = 1.0  # $1/pip/lot
        elif "XAG" in pair_upper or "SILVER" in pair_upper:
            contract_size = 5000
            margin_used = ((self.lot * contract_size) * self.entry) / 200
            pip_value = 50.0  # $50/pip/lot
        else:
            contract_size = 100000
            margin_used = (self.lot * contract_size) / 200
            if "JPY" in pair_upper:
                pip_value = round(1000.0 / self.entry, 6) if self.entry > 0 else 9.2
            else:
                pip_value = 10.0  # $10/pip/lot

        entry_amount = self.lot * contract_size

        sl_pips = round(abs(self.entry - self.sl) / pip_size, 1)
        tp1_pips = round(abs(self.entry - self.tp1) / pip_size, 1) if self.tp1 else 0.0
        tp2_pips = round(abs(self.entry - self.tp2) / pip_size, 1) if self.tp2 else 0.0
        tp3_pips = round(abs(self.entry - self.tp3) / pip_size, 1) if self.tp3 else 0.0

        sl_usd = round(sl_pips * pip_value * self.lot * -1, 2)
        tp1_usd = round(tp1_pips * pip_value * self.lot, 2)
        tp2_usd = round(tp2_pips * pip_value * self.lot, 2)
        tp3_usd = round(tp3_pips * pip_value * self.lot, 2)

        return {
            "trade_id": self.trade_id,
            "pair": self.pair,
            "direction": self.direction,
            "year": self.open_time.year,
            "session": self.session,
            "entry_leg": self.entry_leg,
            "entry_price": round(self.entry, 5),
            "sl_price": round(self.sl, 5),
            "tp1_price": round(self.tp1, 5),
            "tp2_price": round(self.tp2, 5),
            "tp3_price": round(self.tp3, 5),
            "entry": round(self.entry, 5),
            "sl_usd": sl_usd,
            "tp1_usd": tp1_usd,
            "tp2_usd": tp2_usd,
            "tp3_usd": tp3_usd,
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
            "sl_pips": sl_pips,
            "tp1_pips": tp1_pips,
            "tp2_pips": tp2_pips,
            "month": self.open_time.month,
            "week_no": self.open_time.isocalendar()[1],
            # Included for backwards compatibility and safety
            "signal_id": self.signal_id,
            "margin_used": round(margin_used, 2),
            "entry_amount": round(entry_amount, 2),
            "rr_ratio": self.rr_format,
            "score": self.score,
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
        self.pair_upper = pair_upper
        self.pip_size = 0.01 if ("JPY" in pair_upper or "XAU" in pair_upper or "XAG" in pair_upper) else 0.0001

        # pip_value is now computed PER-TRADE dynamically via _get_pip_value(entry_price).
        # Storing a fixed one for the JPY snapshot fallback only (used in the BE-buffer warning).
        if "XAU" in pair_upper:
            # XAU: 1 std lot = 100 oz. 1 pip = $0.01/oz. pip_value = 100 × 0.01 = $1/pip/lot
            self._base_pip_value = 1.0
        elif "XAG" in pair_upper:
            # XAG: 1 std lot = 5000 oz. 1 pip = $0.01/oz. pip_value = 5000 × 0.01 = $50/pip/lot
            self._base_pip_value = 50.0
        elif "JPY" in pair_upper:
            # Dynamic — computed per trade. Store a rough fallback only.
            self._base_pip_value = None   # signals "compute dynamically"
        else:
            # USD-quoted FX (EURUSD, GBPUSD, USDCAD, etc.)
            # 1 std lot = 100,000 units. 1 pip = 0.0001. pip_value = 100k × 0.0001 = $10/pip/lot
            self._base_pip_value = 10.0

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

    def _get_session_for_time(self, current_time: datetime) -> Optional[str]:
        if current_time.tzinfo is None:
            _bar_utc = current_time.replace(tzinfo=timezone.utc)
        else:
            _bar_utc = current_time.astimezone(timezone.utc)
        _bar_ist = (_bar_utc + timedelta(hours=5, minutes=30)).time()

        def _in_range(t, start_str, end_str):
            from datetime import time as dt_time
            s = dt_time(*map(int, start_str.split(":")))
            e = dt_time(*map(int, end_str.split(":")))
            return (t >= s and t < e) if s <= e else (t >= s or t < e)

        return next(
            (name for name, timings in self.config.session_timings.items()
             if _in_range(_bar_ist, timings["start"], timings["end"])),
            None
        )

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
        bt_rr_format = tm_cfg.get("rr_format", "1:2")
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

            # ── Daily expiry: cancel PENDING orders that never triggered ───
            # At 22:00 UTC (NY close) expire all unfilled limit orders (Leg B).
            # This prevents pending orders from spanning multiple days.
            bar_hour = current_time.hour if current_time.tzinfo is None else \
                current_time.astimezone(timezone.utc).hour
            if bar_hour == 22:
                still_open_after_expiry = []
                for trade in self._open_trades:
                    if trade.status == "PENDING" and current_time.date() > trade.open_time.date():
                        trade.status = "CLOSED"
                        trade.result = "CANCELLED"
                        trade.exit_reason = "EXPIRED_2_DAYS"
                        trade.close_bar = idx
                        trade.close_time = current_time
                        trade.exit_price = trade.entry
                        trade.profit_usd = 0.0
                        self._closed_trades.append(trade)
                        logger.debug(
                            f"[BT] Pending EXPIRED (2-day wait) | "
                            f"{trade.pair} {trade.direction} | Entry: {trade.entry:.5f}"
                        )
                    else:
                        still_open_after_expiry.append(trade)
                self._open_trades = still_open_after_expiry

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

            # ── Derive session & killzone from replay BAR time (not live clock) ──
            # session_engine.get_active_session() uses datetime.now() which is always
            # the real-world clock time — wrong for backtesting. Compute from bar.
            session = self._get_session_for_time(current_time)

            if current_time.tzinfo is None:
                _bar_utc = current_time.replace(tzinfo=timezone.utc)
            else:
                _bar_utc = current_time.astimezone(timezone.utc)
            _bar_ist_dt = _bar_utc + timedelta(hours=5, minutes=30)
            _bar_ist = _bar_ist_dt.time()

            def _in_range(t, start_str, end_str):
                from datetime import time as dt_time
                s = dt_time(*map(int, start_str.split(":")))
                e = dt_time(*map(int, end_str.split(":")))
                return (t >= s and t < e) if s <= e else (t >= s or t < e)

            # Summer/Winter rule: March 9 to October 31 is Summer.
            m = _bar_ist_dt.month
            d = _bar_ist_dt.day
            is_summer = (3 < m < 11) or (m == 3 and d >= 9)
            
            active_kz = self.config.killzone_timings_summer if is_summer else self.config.killzone_timings_winter

            killzone = next(
                (name for name, timings in active_kz.items()
                 if _in_range(_bar_ist, timings["start"], timings["end"])),
                None
            )
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
                    {
                        "pair": t.pair,
                        "direction": t.direction,
                        "lot_total": t.lot,
                        "executed_price": t.entry,
                        "sl": t.sl
                    }
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
                is_zgmt_signal = signal.get("strategy", "").startswith("ZGMT")

                if is_zgmt_signal:
                    entry_price = signal_entry  # Fill at the exact 0 GMT price

                    # Compute SL/TP from the fill price using pip distances
                    sl_pips = signal["sl_pips"]
                    tp_pips = signal["tp_pips"]
                    tp3_pips = signal.get("tp3_pips", sl_pips * 3)
                    pip_size = self.pip_size
                    sl_diff = sl_pips * pip_size
                    tp_diff = tp_pips * pip_size
                    tp3_diff = tp3_pips * pip_size
                    if signal["direction"] == "BUY":
                        sl_price  = round(entry_price - sl_diff, 5)
                        tp1_price = round(entry_price + sl_diff, 5)   # TP1 = 1R
                        tp2_price = round(entry_price + tp_diff, 5)   # TP2 = 2R
                        tp3_price = round(entry_price + tp3_diff, 5)  # TP3 = 3R
                    else:
                        sl_price  = round(entry_price + sl_diff, 5)
                        tp1_price = round(entry_price - sl_diff, 5)
                        tp2_price = round(entry_price - tp_diff, 5)
                        tp3_price = round(entry_price - tp3_diff, 5)

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
                    bt_be_buffer_pips = self.config.trade_management.get("breakeven_buffer_pips", 5)
                    bt_partial_tp_fraction = self.config.trade_management.get("partial_tp_fraction", 0.5)

                    # Leg A (DIRECT/ZGMT) = market fill at 0GMT open (not a limit).
                    # Leg B (FILTER/manipulation) = pending limit order above/below 0GMT.
                    entry_mode = signal.get("entry_mode", "DIRECT").upper()
                    is_limit_order = (entry_mode == "FILTER")

                    trade = SimulatedTrade(
                        trade_id=str(uuid.uuid4()),
                        signal_id=signal_id,
                        pair=self.pair,
                        direction=signal["direction"],
                        entry=entry_price,
                        sl=sl_price,
                        tp1=tp1_price if is_zgmt_signal else signal.get("tp1_price", 0.0),
                        tp2=tp2_price if is_zgmt_signal else signal.get("tp2_price", 0.0),
                        tp3=tp3_price if is_zgmt_signal else signal.get("tp3_price", 0.0),
                        lot=lot,
                        bar_index=day_start_idx,
                        bar_time=fill_time,
                        pip_size=self.pip_size,
                        use_partial_tp=True,
                        partial_tp_fraction=bt_partial_tp_fraction,
                        be_buffer_pips=bt_be_buffer_pips,
                        session=signal.get("session", ""),
                        entry_leg=signal.get("entry_leg", ""),
                        is_limit=is_limit_order,
                        rr_format=bt_rr_format,
                        score=signal.get("score", 0.0),
                    )

                    order_type = "Pending Limit" if is_limit_order else "Market Fill (0GMT)"
                    logger.info(
                        f"[BT] Trade PLACED ({order_type}) | {self.pair} {signal['direction']} | "
                        f"FillBar {day_start_idx} (0GMT) | Entry: {entry_price:.5f} | "
                        f"SL: {trade.sl:.5f} | TP: {trade.tp2:.5f} (2R) | "
                        f"Lot: {lot} | Score: {signal['score']} | Leg={entry_mode}"
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
                    tp3_price = signal.get("tp3_price", 0.0)

                trade = SimulatedTrade(
                    trade_id=str(uuid.uuid4()),
                    signal_id=signal_id,
                    pair=self.pair,
                    direction=signal["direction"],
                    entry=entry_price,
                    sl=sl_price,
                    tp1=tp1_price,
                    tp2=tp2_price,
                    tp3=tp3_price,
                    lot=lot,
                    bar_index=idx,
                    bar_time=current_time,
                    session=signal.get("session", ""),
                    entry_leg=signal.get("entry_leg", ""),
                    rr_format=bt_rr_format,
                )
                self._open_trades.append(trade)
                bars_since_signal = 0

                logger.info(
                    f"[BT] Trade OPENED | {self.pair} {signal['direction']} | "
                    f"Bar {idx} | Entry: {entry_price:.5f} | "
                    f"SL: {trade.sl:.5f} | TP1: {trade.tp1:.5f} | TP2: {trade.tp2:.5f} | "
                    f"Lot: {lot} | Score: {signal['score']}"
                )

        # Force-close any remaining open or pending trades at last bar close
        last_bar = m1_data.iloc[-1]
        for trade in self._open_trades:
            if trade.status == "PENDING":
                trade.status = "CLOSED"
                trade.result = "CANCELLED"
                trade.exit_reason = "EXPIRED"
                trade.close_bar = total_bars - 1
                trade.close_time = last_bar["time"]
                trade.exit_price = trade.entry
                self._closed_trades.append(trade)
                logger.info(f"[BT] Pending Trade CANCELLED at session end: {trade.trade_id}")
            else:
                trade.status = "CLOSED"
                trade.result = "EXPIRED"
                trade.exit_reason = "SESSION_END"
                trade.close_bar = total_bars - 1
                trade.close_time = last_bar["time"]
                trade.exit_price = last_bar["close"]
                pips = self._calc_pips(trade.entry, last_bar["close"], trade.direction)
                trade.profit_usd = round(pips * self._get_pip_value(trade.entry) * trade.remaining_lot, 2)
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
        direction = trade.direction
        bar_high  = bar["high"]
        bar_low   = bar["low"]

        if trade.status == "PENDING":
            triggered = False
            if direction == "BUY" and bar_low <= trade.entry:
                triggered = True
            elif direction == "SELL" and bar_high >= trade.entry:
                triggered = True
                
            if triggered:
                trade.status = "OPEN"
                trade.open_time = bar_time
                trade.open_bar = bar_idx
                
                triggered_session = self._get_session_for_time(bar_time)
                if triggered_session:
                    trade.session = triggered_session
                    
                logger.info(
                    f"[BT] Limit Trade TRIGGERED | {trade.pair} {direction} | "
                    f"Bar {bar_idx} | Entry: {trade.entry:.5f} | "
                    f"SL: {trade.sl:.5f} | TP2: {trade.tp2:.5f} | Lot: {trade.lot}"
                )
            else:
                return False

        if direction == "BUY":
            sl_hit  = bar_low  <= trade.current_sl
            tp3_hit = bar_high >= trade.tp3 if trade.rr_format == "1:3" else False
            tp2_hit = bar_high >= trade.tp2
            tp1_hit = bar_high >= trade.tp1
        else:  # SELL
            sl_hit  = bar_high >= trade.current_sl
            tp3_hit = bar_low  <= trade.tp3 if trade.rr_format == "1:3" else False
            tp2_hit = bar_low  <= trade.tp2
            tp1_hit = bar_low  <= trade.tp1

        if trade.use_partial_tp:
            
            # ── 1:3 FORMAT ──
            if trade.rr_format == "1:3":
                # Stage 3: TP2 already hit, waiting for TP3 or SL(at TP1)
                if trade.tp2_hit:
                    if tp3_hit:
                        tp3_pnl = self._calc_pnl(trade.entry, trade.tp3, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + tp3_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN"
                        trade.exit_reason = "TP3_HIT"
                        trade.exit_price = trade.tp3
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] ✅ TP3 Hit | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    if sl_hit:
                        sl_pnl = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + sl_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN" # Technically still a win since SL is at TP1
                        trade.exit_reason = "SL_HIT"
                        trade.exit_price = trade.current_sl
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] 🔶 Stopped at TP1 (Stage 3 SL) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    return False

                # Stage 2: TP1 already hit, waiting for TP2 or SL(at BE+buffer)
                if trade.tp1_hit:
                    if tp2_hit:
                        partial_lot = round(trade.remaining_lot * 0.5, 8)
                        remainder_lot = round(trade.remaining_lot - partial_lot, 8)
                        tp2_pnl = self._calc_pnl(trade.entry, trade.tp2, direction, partial_lot)
                        trade.partial_profit += tp2_pnl
                        trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                        trade.tp2_hit = True
                        trade.current_sl = trade.tp1 # Move SL to TP1 level
                        
                        logger.info(f"[BT] ✅ TP2 Hit (1:3 mode) | {trade.pair} | SL → TP1")
                        
                        if tp3_hit: # Same bar
                            tp3_pnl = self._calc_pnl(trade.entry, trade.tp3, direction, trade.remaining_lot)
                            total_pnl = trade.partial_profit + tp3_pnl
                            trade.status = "CLOSED"
                            trade.result = "WIN"
                            trade.exit_reason = "TP3_HIT"
                            trade.exit_price = trade.tp3
                            trade.profit_usd = round(total_pnl, 2)
                            trade.close_bar = bar_idx
                            trade.close_time = bar_time
                            self._closed_trades.append(trade)
                            logger.info(f"[BT] ✅ TP3 (Same bar) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                            return True
                        return False
                    if sl_hit:
                        sl_pnl = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + sl_pnl
                        trade.status = "CLOSED"
                        trade.result = "BREAKEVEN"
                        trade.exit_reason = "SL_HIT"
                        trade.exit_price = trade.current_sl
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] 🔶 Runner SL (BE) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    return False

                # Stage 1: Pre-TP1
                if sl_hit:
                    loss = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
                    trade.status = "CLOSED"
                    trade.result = "LOSS"
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price = trade.current_sl
                    trade.profit_usd = round(loss, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] ❌ SL Hit (pre-TP1) | {trade.pair} | P&L: {loss:+.2f}")
                    return True
                    
                if tp1_hit:
                    partial_lot = round(trade.lot * trade.partial_tp_fraction, 8)
                    remainder_lot = round(trade.lot - partial_lot, 8)
                    partial_pnl = self._calc_pnl(trade.entry, trade.tp1, direction, partial_lot)
                    trade.partial_profit = partial_pnl
                    trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                    trade.tp1_hit = True
                    trade.be_moved = True
                    
                    buffer = trade.be_buffer_pips * trade.pip_size
                    if direction == "BUY":
                        trade.current_sl = round(trade.entry + buffer, 5)
                    else:
                        trade.current_sl = round(trade.entry - buffer, 5)
                        
                    logger.info(f"[BT] 📊 TP1 Hit (1:3 mode) | {trade.pair} | SL → BE+{trade.be_buffer_pips:.0f}pips")

                    if tp2_hit:
                        partial_lot2 = round(trade.remaining_lot * 0.5, 8)
                        remainder_lot2 = round(trade.remaining_lot - partial_lot2, 8)
                        tp2_pnl = self._calc_pnl(trade.entry, trade.tp2, direction, partial_lot2)
                        trade.partial_profit += tp2_pnl
                        trade.remaining_lot = max(round(remainder_lot2, 8), 0.0)
                        trade.tp2_hit = True
                        trade.current_sl = trade.tp1
                        logger.info(f"[BT] ✅ TP2 (Same bar) | {trade.pair} | SL → TP1")

                        if tp3_hit:
                            tp3_pnl = self._calc_pnl(trade.entry, trade.tp3, direction, trade.remaining_lot)
                            total_pnl = trade.partial_profit + tp3_pnl
                            trade.status = "CLOSED"
                            trade.result = "WIN"
                            trade.exit_reason = "TP3_HIT"
                            trade.exit_price = trade.tp3
                            trade.profit_usd = round(total_pnl, 2)
                            trade.close_bar = bar_idx
                            trade.close_time = bar_time
                            self._closed_trades.append(trade)
                            logger.info(f"[BT] ✅ TP3 (Same bar) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                            return True
                        return False
                    return False
                return False


            # ── 1:2 FORMAT (Legacy ZGMT) ──
            else:
                if trade.tp1_hit:
                    if tp2_hit:
                        tp2_pnl = self._calc_pnl(trade.entry, trade.tp2, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + tp2_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN"
                        trade.exit_reason = "TP2_HIT"
                        trade.exit_price = trade.tp2
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] ✅ TP2 Hit | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    if sl_hit:
                        sl_pnl = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + sl_pnl
                        trade.status = "CLOSED"
                        trade.result = "BREAKEVEN"
                        trade.exit_reason = "SL_HIT"
                        trade.exit_price = trade.current_sl
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] 🔶 Runner SL | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    return False

                if sl_hit:
                    loss = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
                    trade.status = "CLOSED"
                    trade.result = "LOSS"
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price = trade.current_sl
                    trade.profit_usd = round(loss, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] ❌ SL Hit (pre-TP1) | {trade.pair} | P&L: {loss:+.2f}")
                    return True

                if tp1_hit:
                    partial_lot = round(trade.lot * trade.partial_tp_fraction, 8)
                    remainder_lot = round(trade.lot - partial_lot, 8)
                    partial_pnl = self._calc_pnl(trade.entry, trade.tp1, direction, partial_lot)
                    trade.partial_profit = partial_pnl
                    trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                    trade.tp1_hit = True
                    trade.be_moved = True
                    
                    buffer = trade.be_buffer_pips * trade.pip_size
                    if direction == "BUY":
                        trade.current_sl = round(trade.entry + buffer, 5)
                    else:
                        trade.current_sl = round(trade.entry - buffer, 5)

                    logger.info(f"[BT] 📊 TP1 Hit | {trade.pair} | P&L locked: {partial_pnl:+.2f} | SL → BE+{trade.be_buffer_pips:.0f}pips")

                    if tp2_hit:
                        tp2_pnl = self._calc_pnl(trade.entry, trade.tp2, direction, trade.remaining_lot)
                        total_pnl = trade.partial_profit + tp2_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN"
                        trade.exit_reason = "TP2_HIT"
                        trade.exit_price = trade.tp2
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] ✅ TP1+TP2 same bar | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                return False

        # ══════════════════════════════════════════════════════════════════
        # Legacy MMXM split mode: SL always takes priority (conservative)
        # ══════════════════════════════════════════════════════════════════
        if sl_hit:
            loss      = self._calc_pnl(trade.entry, trade.current_sl, direction, trade.remaining_lot)
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
            tp2_pnl   = self._calc_pnl(trade.entry, trade.tp2, direction, trade.remaining_lot)
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
            partial  = self._calc_pnl(trade.entry, trade.tp1, direction, half_lot)
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

    def _get_pip_value(self, entry_price: float) -> float:
        """
        Return USD value per pip per STANDARD LOT for this pair.
        For JPY crosses the value depends on the entry price, so it is
        computed dynamically rather than snapshotted once at startup.

        Formulae:
          XAU  : 100 oz/lot × $0.01/pip  = $1.00 / pip / lot
          XAG  : 5000 oz/lot × $0.01/pip = $50.00 / pip / lot
          JPY  : 100,000 units, 1 pip = 0.01 JPY
                 pip_value = (100,000 × 0.01) / price_USD = 1,000 / price
          FX   : 100,000 units × 0.0001 = $10.00 / pip / lot
        """
        if self._base_pip_value is not None:
            return self._base_pip_value
        # JPY crosses: dynamic
        if entry_price and entry_price > 0:
            return round(1000.0 / entry_price, 6)
        return 9.2  # safe fallback

    def _calc_pips(self, entry: float, exit_price: float, direction: str) -> float:
        """Calculate raw pip distance. Positive = profit direction, negative = loss."""
        if direction == "BUY":
            return (exit_price - entry) / self.pip_size
        else:
            return (entry - exit_price) / self.pip_size

    def _calc_pnl(self, entry: float, exit_price: float, direction: str, lot: float) -> float:
        """
        Calculate P&L in USD for a given price move.
        Uses the entry price to derive the correct pip_value (critical for JPY pairs).
        """
        pips = self._calc_pips(entry, exit_price, direction)
        pip_value = self._get_pip_value(entry)
        return round(pips * pip_value * lot, 2)
