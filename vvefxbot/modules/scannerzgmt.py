"""
scannerzgmt.py — ICT 0-GMT Open Strategy Scanner (VvE FxBOT)

Strategy Reference: ICT 0-GMT Open Master Strategy
- 0 GMT = 5:30 AM IST — Daily reference open price (IPDA True Day Open)
- Midnight NY reference = 9:30 AM IST
- Instruments: FX majors + XAUUSD, XAGUSD (both treated as metals)
- Daily bias via PD Array Matrix (premium/discount zone)
- Step 2B: 0 GMT level may only be respected ONCE (if already tested → skip)
- Entry modes: DIRECT | FILTER (FX ±25 pips, Metals ±95 pips) | SPLIT (filter-only, see below)
- SL: ADR(5-day) ÷ 2 for ALL instruments (dynamic, day-by-day)
  Fallback if ADR unavailable: XAUUSD/XAGUSD = 95 pips | FX = 25 pips
- TP: 2 × SL distance → fixed 1:2 RR
- SPLIT mode: per spec, half at 0 GMT direct + half at manipulation zone.
  Current implementation emits the FILTER leg only (manipulation zone entry),
  because the backtest engine expects a single signal dict per scanner call.
  The direct leg is conceptually accounted for via position_fraction=0.5 in signal.
  To enable true two-leg execution, refactor the engine's signal dispatch first.
"""

import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta, time as dt_time
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine

logger = get_logger("ScannerZGMT")


class ScannerZGMT:
    """
    Scanner for the ICT 0-GMT Open strategy.

    Checks daily bias via PD Array Matrix, identifies the 0 GMT open price
    (5:30 AM IST), validates Step 2B untested condition, and generates a
    BUY or SELL signal with instrument-specific fixed SL/TP and 1:2 RR.
    """

    # IST offset = UTC + 5:30
    _IST_OFFSET = timedelta(hours=5, minutes=30)

    def __init__(self, config: Config, mt5: MT5Connector, state: StateEngine):
        self.config = config
        self.mt5 = mt5
        self.state = state
        # Per-pair dedup: tracks last signal bar time for cooldown
        self._last_signal_time: dict = {}
        # Per-pair daily signal count: { "YYYY-MM-DD:PAIR": count }
        self._daily_counts: dict = {}
        # Tracks daily status per pair: YYYY-MM-DD:PAIR -> True if finalized/invalidated
        self._daily_finalized: dict = {}
        # Cached config section — also used by HTF OB exception methods
        self.zgmt_cfg: dict = getattr(config, "zgmt_scanner", {})

    # ──────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────

    def _utc_now(self) -> datetime:
        """Get the current UTC time, supporting backtest connector's historical clock if present."""
        if hasattr(self.mt5, "current_time"):
            t = self.mt5.current_time()
            if t.tzinfo is None:
                return t.replace(tzinfo=timezone.utc)
            return t.astimezone(timezone.utc)
        return datetime.now(timezone.utc)

    @staticmethod
    def _to_ist(utc_dt: datetime) -> datetime:
        """Convert a UTC-aware datetime to IST (UTC+5:30)."""
        return utc_dt + ScannerZGMT._IST_OFFSET

    @staticmethod
    def _parse_hhmm(s: str) -> dt_time:
        """Parse 'HH:MM' config string to datetime.time."""
        parts = s.split(":")
        return dt_time(int(parts[0]), int(parts[1]))

    def _pip_size(self, pair: str) -> float:
        """Returns pip size per instrument convention.
        - JPY pairs: 0.01 (1 pip = 0.01 JPY)
        - XAUUSD / XAGUSD (metals): 0.01 (1 pip = $0.01/oz or per unit)
        - All other FX pairs: 0.0001
        """
        p = pair.upper()
        if "JPY" in p:
            return 0.01
        if "XAU" in p or "XAG" in p:  # Gold AND Silver use 0.01
            return 0.01
        return 0.0001

    def _pips_to_price(self, pair: str, pips: float) -> float:
        """Convert pips to a price difference using live symbol point."""
        pip_size = self._pip_size(pair)
        return pip_size * pips

    def _price_to_pips(self, pair: str, price_diff: float) -> float:
        """Convert a price difference to pips."""
        pip_size = self._pip_size(pair)
        if pip_size == 0:
            return 0.0
        return abs(price_diff) / pip_size

    def _is_metal(self, pair: str) -> bool:
        """Returns True for XAUUSD and XAGUSD (both treated as metals per strategy spec)."""
        p = pair.upper()
        return "XAU" in p or "XAG" in p

    def _today_ist_str(self) -> str:
        return self._to_ist(self._utc_now()).strftime("%Y-%m-%d")

    def _daily_count_key(self, pair: str) -> str:
        return f"{self._today_ist_str()}:{pair}"

    def _increment_daily_count(self, pair: str):
        key = self._daily_count_key(pair)
        self._daily_counts[key] = self._daily_counts.get(key, 0) + 1

    def _get_daily_count(self, pair: str) -> int:
        # Purge stale keys from previous days
        today = self._today_ist_str()
        stale = [k for k in self._daily_counts if not k.startswith(today)]
        for k in stale:
            del self._daily_counts[k]
        return self._daily_counts.get(self._daily_count_key(pair), 0)

    def _is_daily_finalized(self, pair: str) -> bool:
        """Check if today's ZGMT setup has been completed or invalidated for this pair."""
        today = self._today_ist_str()
        key = f"{today}:{pair}"
        # Purge stale keys from previous days
        stale = [k for k in self._daily_finalized if not k.startswith(today)]
        for k in stale:
            del self._daily_finalized[k]
        return self._daily_finalized.get(key, False)

    def _mark_daily_finalized(self, pair: str):
        """Mark today's ZGMT setup as finalized/invalidated for this pair."""
        key = f"{self._today_ist_str()}:{pair}"
        self._daily_finalized[key] = True

    # ──────────────────────────────────────────────────────────────────
    # Step 1 — Daily bias via PD Array Matrix
    # ──────────────────────────────────────────────────────────────────

    def _get_daily_bias(self, pair: str, zgmt_cfg: dict, zgmt_price: float) -> tuple[str | None, bool, float, float]:
        """
        Determine bullish or bearish bias for Leg A and Leg C.
        Uses T-1 (Yesterday) 50% midpoint.
        If 0 GMT > Midpoint -> SELL (Bearish).
        If 0 GMT < Midpoint -> BUY (Bullish).
        Returns Tuple[bias_str_or_none, is_structural_absence, range_high, range_low].
        """
        candles = self.mt5.get_candles(pair, "D1", count=3)
        if candles is None or len(candles) < 2:
            logger.debug(f"[{pair}] ZGMT: Insufficient D1 candles for PD bias.")
            return None, True, 0.0, 0.0

        # -1 is current day. -2 is exactly T-1 (the completely finished prior day)
        yesterday = candles.iloc[-2]

        range_high = float(yesterday["high"])
        range_low = float(yesterday["low"])

        if range_high <= range_low:
            logger.debug(f"[{pair}] ZGMT: Invalid D1 range high={range_high} low={range_low}.")
            return None, True, 0.0, 0.0

        midpoint = (range_high + range_low) / 2.0

        if zgmt_price > midpoint:
            bias = "BEARISH"  # 0 GMT opened above yesterday's 50% (sell side)
        elif zgmt_price < midpoint:
            bias = "BULLISH"  # 0 GMT opened below yesterday's 50% (buy side)
        else:
            bias = None  # Exactly at equilibrium

        logger.debug(
            f"[{pair}] ZGMT: T-1 Range High={range_high:.5f} Low={range_low:.5f} "
            f"Mid={midpoint:.5f} 0GMT={zgmt_price:.5f} → Bias={bias}"
        )
        return bias, False, range_high, range_low

    def _is_pd_array_swept_before_zgmt(self, pair: str, range_high: float, range_low: float) -> bool:
        """
        Check if the T-1 High or Low was swept between the broker's daily open
        (00:00 broker time) and 0 GMT today.
        """
        now_utc = self._utc_now()
        target_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        offset_hours = self._get_broker_utc_offset_hours(pair)
        
        # If broker offset <= 0, the day starts at or after 0 GMT, so there's no "before 0 GMT" on the current broker day.
        if offset_hours <= 0:
            return False
            
        target_broker_datetime = target_utc + timedelta(hours=offset_hours)
            
        candles = self.mt5.get_candles(pair, "M15", count=96)
        if candles is None or candles.empty:
            return False
            
        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
                
            # Check if this candle is on the SAME broker day as the 0 GMT target
            if candle_time.date() == target_broker_datetime.date():
                # Check if this candle occurred BEFORE the 0 GMT time
                if candle_time < target_broker_datetime:
                    # If high touched previous high, or low touched previous low
                    if float(row["high"]) >= range_high or float(row["low"]) <= range_low:
                        return True
        return False

    # ──────────────────────────────────────────────────────────────────
    # Step 2 — Identify 0 GMT open price from today's H1 candles
    # ──────────────────────────────────────────────────────────────────

    def _get_broker_utc_offset_hours(self, pair: str) -> int:
        """Calculate the broker's offset from UTC in hours."""
        if hasattr(self.mt5, "offset_hours"):
            return int(self.mt5.offset_hours)
            
        tick = self.mt5.get_tick(pair)
        if not tick or "time" not in tick:
            return 0
        real_utc_ts = self._utc_now().timestamp()
        broker_ts = tick["time"]
        offset_seconds = broker_ts - real_utc_ts
        return round(offset_seconds / 3600.0)

    def _get_zgmt_price(self, pair: str) -> tuple[float | None, bool]:
        """
        Fetch the H1 candle open price that corresponds to today's 0 GMT
        (which is 00:00 UTC = 5:30 AM IST).
        """
        now_utc = self._utc_now()
        target_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        
        offset_hours = self._get_broker_utc_offset_hours(pair)
        target_broker_datetime = target_utc + timedelta(hours=offset_hours)

        # Give MT5 a 3-minute grace period after 0GMT to ensure the open price is available
        if now_utc < target_utc + timedelta(minutes=3):
            logger.debug(f"[{pair}] ZGMT: Waiting 3 minutes past 0 GMT for MT5 data to stabilize.")
            return None, False

        candles = self.mt5.get_candles(pair, "H1", count=30)
        if candles is None or candles.empty:
            logger.debug(f"[{pair}] ZGMT: No H1 candles returned.")
            return None, False

        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            if (candle_time.year == target_broker_datetime.year and
                    candle_time.month == target_broker_datetime.month and
                    candle_time.day == target_broker_datetime.day and
                    candle_time.hour == target_broker_datetime.hour and
                    candle_time.minute == target_broker_datetime.minute):
                zgmt_price = float(row["open"])
                logger.debug(f"[{pair}] ZGMT: Found 0 GMT open price = {zgmt_price:.5f} at {candle_time} (offset {offset_hours}h)")
                return zgmt_price, False

        is_structural = now_utc.hour >= 2
        logger.debug(f"[{pair}] ZGMT: 0 GMT H1 candle not found in fetched data. is_structural={is_structural}")
        return None, is_structural

    # ──────────────────────────────────────────────────────────────────
    # Step 2B — Check if 0 GMT level has already been tested today
    # ──────────────────────────────────────────────────────────────────

    def _is_zgmt_level_tested(self, pair: str, zgmt_price: float, zgmt_cfg: dict) -> bool:
        """
        Check M1 candles from today's 0 GMT onwards until now to see if price came within
        zgmt_test_threshold_pips of the 0 GMT open price.

        Returns True if tested (→ skip), False if untested (→ trade).
        """
        threshold_pips = zgmt_cfg.get("zgmt_test_threshold_pips", 5)
        threshold_price = self._pips_to_price(pair, threshold_pips)

        now_utc = self._utc_now()
        today_zgmt_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        exclude_mins = zgmt_cfg.get("zgmt_test_exclude_first_mins", 15)
        test_start_time_utc = today_zgmt_utc + timedelta(minutes=exclude_mins)

        # Guard: if we haven't even passed the exclusion window yet, don't signal.
        if now_utc < test_start_time_utc:
            logger.debug(
                f"[{pair}] ZGMT Step 2B: Still inside exclusion window "
                f"(now={now_utc.strftime('%H:%M')} UTC, window_ends={test_start_time_utc.strftime('%H:%M')} UTC). "
                f"Deferring check."
            )
            return True  # Treat as "not ready" → block signal until window elapses

        # Calculate minutes elapsed since 0 GMT today
        elapsed_minutes = int((now_utc - today_zgmt_utc).total_seconds() / 60)
        # Fetch enough M1 candles to cover the entire day from 0 GMT to now
        fetch_count = max(60, elapsed_minutes + 15)

        candles = self.mt5.get_candles(pair, "M1", count=fetch_count)
        if candles is None or candles.empty:
            logger.debug(f"[{pair}] ZGMT: No M1 candles for Step 2B test. Assuming untested.")
            return False

        # MT5 M1 candles are returned in broker time (but parsed as UTC)
        # So we must shift our test_start_time to broker time
        offset_hours = self._get_broker_utc_offset_hours(pair)
        test_start_broker_time = test_start_time_utc + timedelta(hours=offset_hours)

        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Only evaluate candles that opened after the exclusion window
            if candle_time < test_start_broker_time:
                continue

            candle_low = float(row["low"])
            candle_high = float(row["high"])

            # "Tested" = price actually touched or crossed the 0GMT level.
            # We do NOT use a proximity threshold because that wrongly invalidates setups
            # where price passed nearby but never actually reached the level.
            # A candle "tests" the level only if the 0GMT price sits within [low, high].
            if candle_low <= zgmt_price <= candle_high:
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: Level ALREADY TESTED (price touched). "
                    f"Candle H={candle_high:.5f} L={candle_low:.5f} vs ZGMT={zgmt_price:.5f}"
                )
                return True

        logger.debug(f"[{pair}] ZGMT Step 2B: Level NOT YET TESTED. Proceeding.")
        return False

    # ──────────────────────────────────────────────────────────────────
    # Step 3 — Power of Three confirmation (optional)
    # ──────────────────────────────────────────────────────────────────

    def _check_power_of_three(self, pair: str, bias: str) -> bool:
        """
        Loose Power of Three check on the daily chart.
        Bullish: current day open near low (not expanded much yet).
        Bearish: current day open near high.
        """
        candles = self.mt5.get_candles(pair, "D1", count=2)
        if candles is None or candles.empty or len(candles) < 1:
            logger.debug(f"[{pair}] ZGMT PoT: No D1 candle for PoT check — skipping.")
            return True  # Fail-open: don't block if data unavailable

        today = candles.iloc[-1]
        open_price = float(today["open"])
        current_high = float(today["high"])
        current_low = float(today["low"])
        candle_range = current_high - current_low

        if candle_range <= 0:
            return True  # Degenerate candle

        open_position = (open_price - current_low) / candle_range  # 0=near low, 1=near high

        if bias == "BULLISH":
            # Open should be in the lower 50% of today's range (not expanded up much)
            ok = open_position <= 0.5
        else:  # BEARISH
            # Open should be in the upper 50% of today's range
            ok = open_position >= 0.5

        logger.debug(
            f"[{pair}] ZGMT PoT: Bias={bias} OpenPos={open_position:.2f} → {'PASS' if ok else 'FAIL'}"
        )
        return ok

    # ──────────────────────────────────────────────────────────────────
    # Entry price calculation (Steps 4, 5, 6)
    # ──────────────────────────────────────────────────────────────────

    def _compute_entry_sl_tp(
        self, pair: str, bias: str, zgmt_price: float, tick: dict, zgmt_cfg: dict, override_entry_mode: str = None
    ) -> dict | None:
        """
        Calculates the entry, SL, and TP prices for ZGMT using dynamic ADR(5).

        Filter pips (manipulation range, Step 4):
          Metals (XAUUSD/XAGUSD): zgmt_filter_pips_metal (default 95 — midpoint of 90–100 pips).
          FX pairs:               zgmt_filter_pips_fx     (default 25 — midpoint of 20–30 pips).

        Entry modes:
          DIRECT: enter at 0 GMT price.
          FILTER: enter at 0 GMT ± filter_pips (Judas Swing manipulation zone).
          SPLIT:  half at 0 GMT direct + half at manipulation zone.

        Returns a dict with entry_price, sl_price, tp1_price, tp2_price,
        sl_pips, tp_pips, filter_pips, entry_mode.
        """
        entry_mode = (override_entry_mode or zgmt_cfg.get("zgmt_entry_mode", "DIRECT")).upper()

        # ── Step 4: Manipulation zone filter pips (metal vs FX) ─────
        if self._is_metal(pair):
            filter_pips = zgmt_cfg.get("zgmt_filter_pips_metal", 95)
        else:
            filter_pips = zgmt_cfg.get("zgmt_filter_pips_fx", 25)
        filter_diff = self._pips_to_price(pair, filter_pips)

        # ── Steps 5/6: Dynamic ADR-based SL/TP (STRICT — no fallback) ──
        # SL = 5-day ADR ÷ 2. If ADR data is unavailable, the signal is cancelled.
        # We do NOT fall back to fixed pips — a signal without a real ADR-based SL is not valid.
        adr_sl_dist = self._calculate_adr_sl(pair)  # returns price-unit distance or None
        if not adr_sl_dist or adr_sl_dist <= 0:
            logger.warning(
                f"[{pair}] ZGMT: ADR(5) unavailable — signal cancelled. "
                f"Cannot calculate SL without real ADR data."
            )
            return None

        sl_dist_price = adr_sl_dist
        tp_dist_price = adr_sl_dist * 2
        sl_pips = sl_dist_price / self._pip_size(pair)
        tp_pips = sl_pips * 2

        # ── Cap SL at configurable maximum ──────────────────────────
        # Prevents runaway ADR-based SL (e.g. 500-1200 pips on JPY cross days)
        if self._is_metal(pair):
            max_sl_pips = float(zgmt_cfg.get("max_sl_pips_metal", 1500))  # 1500 pips = $15 on Gold
        else:
            max_sl_pips = float(zgmt_cfg.get("max_sl_pips_fx", 50))
        if sl_pips > max_sl_pips:
            logger.debug(
                f"[{pair}] ZGMT: ADR SL ({sl_pips:.1f} pips) exceeds cap ({max_sl_pips} pips). Capping."
            )
            sl_pips       = max_sl_pips
            tp_pips       = sl_pips * 2
            tp3_pips      = sl_pips * 3
            sl_dist_price = self._pips_to_price(pair, sl_pips)
            tp_dist_price = self._pips_to_price(pair, tp_pips)
            tp3_dist_price= self._pips_to_price(pair, tp3_pips)
        else:
            tp3_pips      = sl_pips * 3
            tp3_dist_price= self._pips_to_price(pair, tp3_pips)

        # ── Step 4: Entry price per mode ────────────────────────────

        if bias == "BULLISH":
            if entry_mode == "DIRECT":
                # Option A: Buy exactly at the 0 GMT open price (IPDA True Day Open)
                entry_price = zgmt_price
            elif entry_mode == "FILTER":
                # Option B: Buy limit zgmt_filter_pips_fx/metal BELOW 0 GMT open (Judas Swing)
                entry_price = zgmt_price - filter_diff
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = zgmt_price
                entry_mode = "DIRECT"

            sl_price  = entry_price - sl_dist_price
            tp1_price = entry_price + sl_dist_price   # TP1 = 1R
            tp2_price = entry_price + tp_dist_price   # TP2 = 2R
            tp3_price = entry_price + tp3_dist_price  # TP3 = 3R

        else:  # BEARISH
            if entry_mode == "DIRECT":
                # Option A: Sell directly at 0 GMT open price
                entry_price = zgmt_price
            elif entry_mode == "FILTER":
                # Option B: Sell limit zgmt_filter_pips_fx/metal ABOVE 0 GMT open
                entry_price = zgmt_price + filter_diff
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = zgmt_price
                entry_mode = "DIRECT"

            sl_price  = entry_price + sl_dist_price
            tp1_price = entry_price - sl_dist_price   # TP1 = 1R
            tp2_price = entry_price - tp_dist_price   # TP2 = 2R
            tp3_price = entry_price - tp3_dist_price  # TP3 = 3R

        return {
            "entry_price":      round(entry_price, 5),
            "sl_price":         round(sl_price, 5),
            "tp1_price":        round(tp1_price, 5),
            "tp2_price":        round(tp2_price, 5),
            "tp3_price":        round(tp3_price, 5),
            "sl_pips":          round(sl_pips, 2),
            "tp_pips":          round(tp_pips, 2),
            "tp3_pips":         round(tp3_pips, 2),
            "entry_mode":       entry_mode,
            "filter_pips":      filter_pips,
        }

    # ──────────────────────────────────────────────────────────────────
    # Main scan method
    # ──────────────────────────────────────────────────────────────────

    def scan(self, pair: str, session: str, killzone: str) -> dict | list | None:
        """
        Execute the ZGMT scan for a single pair.
        Returns a signal dict (or list of dicts for SPLIT) on success, None otherwise.
        """
        # ── Check if already finalized/invalidated today ─────────────
        if self._is_daily_finalized(pair):
            return None

        logger.debug(f"[{pair}] ZGMT: Scan started | Session={session} | KZ={killzone}")

        # ── 0. Config gate ───────────────────────────────────────────
        zgmt_cfg = self.config.zgmt_scanner
        allow_buy = zgmt_cfg.get("allow_buy", True)
        allow_sell = zgmt_cfg.get("allow_sell", True)
        if not allow_buy and not allow_sell:
            return None

        # ── 0d. Scanner-level cooldown (minutes since last signal) ───
        cooldown_minutes = zgmt_cfg.get("cooldown_minutes", 60)
        last_sig = self._last_signal_time.get(pair)
        if last_sig is not None:
            elapsed = (self._utc_now() - last_sig).total_seconds() / 60.0
            if elapsed < cooldown_minutes:
                logger.debug(
                    f"[{pair}] ZGMT: Scanner cooldown active ({elapsed:.1f}/{cooldown_minutes} min)."
                )
                return None

        # ── Step 2B timing window check ──────────────────────────────
        now_ist = self._to_ist(self._utc_now())
        window_start = self._parse_hhmm(zgmt_cfg.get("zgmt_window_start_ist", "05:30"))
        window_end = self._parse_hhmm(zgmt_cfg.get("zgmt_window_end_ist", "08:00"))
        current_ist_time = now_ist.time()

        is_in_zgmt_window = window_start <= current_ist_time <= window_end
        if not is_in_zgmt_window:
            logger.debug(
                f"[{pair}] ZGMT: Outside Strategy A window "
                f"({window_start}–{window_end} IST). Strategies B/C may still run."
            )

        # ── Step 1: 0 GMT open price ──────────────────────────────────
        zgmt_price, is_structural = self._get_zgmt_price(pair)
        if zgmt_price is None:
            logger.info(f"[{pair}] ZGMT: 0 GMT open price not available — skipping.")
            if is_structural or hasattr(self.mt5, "current_time"):
                self._mark_daily_finalized(pair)
            return None

        # ── Step 2: Daily bias (Leg A & C) ────────────────────────────
        require_pd = zgmt_cfg.get("require_pd_array_check", True)
        pd_swept_before_zgmt = False
        
        if require_pd:
            bias, is_structural, range_high, range_low = self._get_daily_bias(pair, zgmt_cfg, zgmt_price)
            if bias is None:
                logger.info(f"[{pair}] ZGMT: Could not determine D1 PD bias — skipping.")
                if is_structural or hasattr(self.mt5, "current_time"):
                    self._mark_daily_finalized(pair)
                return None
                
            if self._is_pd_array_swept_before_zgmt(pair, range_high, range_low):
                pd_swept_before_zgmt = True
                logger.info(f"[{pair}] ZGMT: PD Array swept before 0 GMT. Leg A and C invalidated.")
        else:
            # No bias check: infer from current price vs yesterday's close
            logger.debug(f"[{pair}] ZGMT: PD array check disabled — using tick direction.")
            tick = self.mt5.get_tick(pair)
            bias = "BULLISH" if (tick and tick["bid"] > zgmt_price) else "BEARISH"
            if bias is None:
                return None

        # ── Allow direction gate after bias ──────────────────────────
        if bias == "BULLISH" and not allow_buy:
            logger.debug(f"[{pair}] ZGMT: Bullish bias but allow_buy=False.")
            return None
        if bias == "BEARISH" and not allow_sell:
            logger.debug(f"[{pair}] ZGMT: Bearish bias but allow_sell=False.")
            return None

        # ── Step 2B: Untested condition ───────────────────────────────
        is_tested = self._is_zgmt_level_tested(pair, zgmt_price, zgmt_cfg)
        strategy_a_valid = not is_tested

        if is_tested:
            now_utc = self._utc_now()
            today_zgmt_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            exclude_mins = zgmt_cfg.get("zgmt_test_exclude_first_mins", 15)
            test_start_time = today_zgmt_utc + timedelta(minutes=exclude_mins)

            if now_utc < test_start_time:
                # Still inside exclusion window — price hasn't displaced yet.
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: Inside exclusion window — deferring entirely."
                )
                return None
            else:
                # Level has genuinely been touched after the exclusion window → invalid for Strategy A.
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: 0 GMT level already tested "
                    f"({zgmt_price:.5f}) — Strategy A invalid for today."
                )
                
                # Optimization: if B and C are disabled, finalize the day early
                if not zgmt_cfg.get("strategy_b_enabled", False) and not zgmt_cfg.get("strategy_c_enabled", False):
                    logger.info(f"[{pair}] ZGMT Step 2B: 0 GMT level tested and B/C disabled. Finalizing for today.")
                    self._mark_daily_finalized(pair)
                    return None

        # ── Step 3: Power of Three (optional) ────────────────────────
        if zgmt_cfg.get("require_power_of_three", False):
            if not self._check_power_of_three(pair, bias):
                logger.debug(f"[{pair}] ZGMT: Power of Three check failed — skipping.")
                return None

        # ── Live tick ─────────────────────────────────────────────────
        tick = self.mt5.get_tick(pair)
        if not tick:
            logger.debug(f"[{pair}] ZGMT: No live tick data — skipping.")
            return None

        # ── Signal Construction ───────────────────────────────────────
        # Strategy B: HTF OB Exception
        ob_signal = None
        if zgmt_cfg.get("strategy_b_enabled", False):
            ob_signal = self._check_htf_ob_exception(pair, bias, session, killzone)
            if ob_signal is not None:
                ob_signal["setup_type"] = "ZGMT-B"
                ob_signal["strategy"] = "ZGMT-B"
                ob_signal["bias_summary"] = ob_signal["bias_summary"].replace("ZGMT-EXCEPTION", "ZGMT-B")
                logger.info(f"[{pair}] ZGMT-B: HTF OB condition met.")

        signals_to_emit = []

        def build_signal_dict(levs: dict, strat_id: str) -> dict:
            spr = self.mt5.get_current_spread(pair)
            den = levs["sl_pips"] + spr
            eff_rr = (levs["tp_pips"] - spr) / den if den > 0 else 0.0
            pd_zone = "DISCOUNT" if bias == "BULLISH" else "PREMIUM"
            filt_note = f" ±{levs['filter_pips']}pips" if strat_id == "ZGMT-C" else ""
            summary = f"{strat_id} | PD: {pd_zone} | {filt_note} | 0GMT={zgmt_price:.5f}"
            
            return {
                "signal_id": str(uuid.uuid4()),
                "pair": pair,
                "session": session,
                "killzone": killzone,
                "entry_leg": {"ZGMT-A": "A", "ZGMT-B": "B", "ZGMT-C": "C"}.get(strat_id, "A"),
                "entry_mode": "DIRECT" if strat_id == "ZGMT-A" else "FILTER",
                "timeframe_bias": zgmt_cfg.get("timeframe_bias", "D1"),
                "timeframe_entry": zgmt_cfg.get("timeframe_entry", "H1"),
                "direction": "BUY" if bias == "BULLISH" else "SELL",
                "bias_summary": summary,
                "entry_price": levs["entry_price"],
                "sl_price": levs["sl_price"],
                "tp1_price": levs["tp1_price"],
                "tp2_price": levs["tp2_price"],
                "tp3_price": levs.get("tp3_price", 0.0),
                "sl_pips": levs["sl_pips"],
                "tp_pips": levs["tp_pips"],
                "tp3_pips": levs.get("tp3_pips", 0.0),
                "spread_pips": spr,
                "effective_rr": round(eff_rr, 3),
                "score": round({"ZGMT-A": 80.0, "ZGMT-B": 95.0, "ZGMT-C": 75.0}.get(strat_id, 70.0) + min(eff_rr * 2.0, 10.0), 1),
                "detected_time": self._utc_now().isoformat(),
                "strategy": strat_id,
                "setup_type": strat_id,
                "fixed_lot_size": float(zgmt_cfg.get("fixed_lot_size", 0.0)),
                "skip_rr_check": bool(zgmt_cfg.get("skip_rr_check", True)),
                "position_fraction": 1.0,
            }

        # Strategy A (0 GMT Liquidity)
        if strategy_a_valid and not pd_swept_before_zgmt and is_in_zgmt_window and zgmt_cfg.get("strategy_a_enabled", False):
            # Only take Strategy A in the Asian Killzone
            if killzone.lower() == "asia":
                levs_direct = self._compute_entry_sl_tp(pair, bias, zgmt_price, tick, zgmt_cfg, override_entry_mode="DIRECT")
                if levs_direct:
                    signals_to_emit.append(build_signal_dict(levs_direct, "ZGMT-A"))
            else:
                logger.debug(f"[{pair}] ZGMT: Skipping Strategy A because killzone '{killzone}' is not Asia.")

        # Strategy B (0 GMT + OB Model)
        if ob_signal is not None:
            signals_to_emit.append(ob_signal)

        # Strategy C (Manipulation / Judas Swing)
        if strategy_a_valid and not pd_swept_before_zgmt and zgmt_cfg.get("strategy_c_enabled", False):
            levs_filter = self._compute_entry_sl_tp(pair, bias, zgmt_price, tick, zgmt_cfg, override_entry_mode="FILTER")
            if levs_filter:
                signals_to_emit.append(build_signal_dict(levs_filter, "ZGMT-C"))

        if not signals_to_emit:
            return None

        # ── Register signals ───────────────────────────────────────────
        self._last_signal_time[pair] = self._utc_now()
        self._increment_daily_count(pair)

        for s in signals_to_emit:
            logger.info(
                f"[{pair}] ZGMT Signal ✅ {s['direction']} | "
                f"Entry={s['entry_price']:.5f} SL={s['sl_price']:.5f} "
                f"TP2={s['tp2_price']:.5f} | 0GMT={zgmt_price:.5f} | "
                f"Mode={s.get('setup_type', 'ZGMT')} | Score={s['score']}"
            )

        self._mark_daily_finalized(pair)

        return signals_to_emit if len(signals_to_emit) > 1 else signals_to_emit[0]


    # ══════════════════════════════════════════════════════════════════
    # HTF Order Block Exception — private methods
    # ══════════════════════════════════════════════════════════════════

    def _check_htf_ob_exception(self, pair: str, bias: str, session: str, killzone: str) -> dict | None:
        """
        Checks if a valid unmitigated HTF Order Block on H4 or H1 overrides the 0-GMT setup.
        Returns signal dict if valid OB found inside correct Fibonacci zone, else None.
        """
        zgmt_cfg = self.zgmt_cfg

        # For 20 trading days of history: 20 days * 6 H4 candles = 120, 20 days * 24 H1 candles = 480
        h4_candles = self.mt5.get_candles(pair, "H4", count=zgmt_cfg.get("zgmt_ob_candles_4h", 120))
        h1_candles = self.mt5.get_candles(pair, "H1", count=zgmt_cfg.get("zgmt_ob_candles_1h", 480))

        h4_empty = h4_candles is None or (hasattr(h4_candles, 'empty') and h4_candles.empty)
        h1_empty = h1_candles is None or (hasattr(h1_candles, 'empty') and h1_candles.empty)
        if h4_empty and h1_empty:
            return None

        current_price = self.mt5.get_current_bid(pair)
        if not current_price:
            return None

        symbol_point = self.mt5.get_symbol_point(pair)  # used only for OB body comparisons
        pip_size = self._pip_size(pair)                  # used for pip-based calculations
        tap_threshold = zgmt_cfg.get("zgmt_ob_tap_threshold_pips", 5) * pip_size

        all_obs = []
        for df, tf in [(h4_candles, "H4"), (h1_candles, "H1")]:
            if df is None or (hasattr(df, 'empty') and df.empty):
                continue
            df = df.reset_index(drop=True)
            all_obs += self._detect_normal_ob(df, tf)
            all_obs += self._detect_mitigation_block(df, tf)
            all_obs += self._detect_breaker_block(df, tf)

        if not all_obs:
            return None
            
        logger.debug(f"[{pair}] _check_htf_ob_exception: Found {len(all_obs)} total OBs across H4/H1")

        # Filter 1: unmitigated only
        valid_obs = [ob for ob in all_obs if not ob["is_mitigated"]]
        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(valid_obs)} OBs survived unmitigated filter")
        if not valid_obs:
            return None

        # Filter 2: price must currently be tapping into the OB zone
        tapping_obs = [
            ob for ob in valid_obs
            if (ob["body_low"] - tap_threshold) <= current_price <= (ob["body_high"] + tap_threshold)
        ]
        if len(valid_obs) > 0 and len(tapping_obs) == 0:
            # Just log periodically so it doesn't spam every minute
            pass
        if not tapping_obs:
            return None
        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(tapping_obs)} OBs tapping current price {current_price}")

        # Filter 3: OB must sit inside the correct Fibonacci premium / discount zone
        fib_min = zgmt_cfg.get("zgmt_ob_fib_min", 0.50)
        fib_max = zgmt_cfg.get("zgmt_ob_fib_max", 0.618)
        swing_lookback = zgmt_cfg.get("zgmt_swing_lookback", 3)

        fib_valid_obs = []
        for ob in tapping_obs:
            df = h4_candles if ob["timeframe"] == "H4" else h1_candles
            if df is None or (hasattr(df, 'empty') and df.empty):
                continue
            df = df.reset_index(drop=True)
            if self._is_ob_in_fib_zone(df, ob, swing_lookback, fib_min, fib_max):
                fib_valid_obs.append(ob)

        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(fib_valid_obs)} OBs survived Fib Filter")
        if not fib_valid_obs:
            return None

        # Select best OB: prefer H4 over H1, then most recent (highest candle_index)
        fib_valid_obs.sort(key=lambda x: (0 if x["timeframe"] == "H4" else 1, -x["candle_index"]))
        best_ob = fib_valid_obs[0]

        direction = best_ob["direction"]
        
        # Enforce that Leg B trades in the SAME direction as the Daily Bias
        expected_direction = "BUY" if bias == "BULLISH" else "SELL"
        if direction != expected_direction:
            logger.debug(f"[{pair}] ZGMT-B: OB direction ({direction}) does not match Daily Bias ({expected_direction}). Skipping.")
            return None

        if direction == "BUY" and not zgmt_cfg.get("allow_buy", True):
            return None
        if direction == "SELL" and not zgmt_cfg.get("allow_sell", True):
            return None

        entry_price = best_ob["body_mid"]

        # OB-based dynamic SL (distance from entry to OB extreme)
        if direction == "BUY":
            sl_distance = entry_price - best_ob["body_low"]
        else:
            sl_distance = best_ob["body_high"] - entry_price

        if sl_distance <= 0:
            return None

        sl_pips = sl_distance / pip_size   # use pip_size not symbol_point — point ≠ pip on 5-decimal brokers
        tp_pips = sl_pips * 2

        # Cap SL at configurable maximum (same cap as standard ZGMT entries)
        if self._is_metal(pair):
            max_sl_pips = float(zgmt_cfg.get("max_sl_pips_metal", 1500))  # 1500 pips = $15 on Gold
        else:
            max_sl_pips = float(zgmt_cfg.get("max_sl_pips_fx", 50))
        if sl_pips > max_sl_pips:
            logger.debug(
                f"[{pair}] ZGMT-EXCEPTION: ADR SL ({sl_pips:.1f} pips) exceeds cap "
                f"({max_sl_pips} pips). Capping."
            )
            sl_pips   = max_sl_pips
            tp_pips   = sl_pips * 2
            tp3_pips  = sl_pips * 3
            sl_distance = self._pips_to_price(pair, sl_pips)
        else:
            tp3_pips  = sl_pips * 3

        if direction == "BUY":
            sl_price  = entry_price - sl_distance
            tp1_price = entry_price + sl_distance          # TP1 = 1R
            tp_price  = entry_price + (sl_distance * 2)    # TP2 = 2R
            tp3_price = entry_price + (sl_distance * 3)    # TP3 = 3R
        else:
            sl_price  = entry_price + sl_distance
            tp1_price = entry_price - sl_distance          # TP1 = 1R
            tp_price  = entry_price - (sl_distance * 2)    # TP2 = 2R
            tp3_price = entry_price - (sl_distance * 3)    # TP3 = 3R

        spread_pips = self.mt5.get_current_spread(pair)
        denom = sl_pips + spread_pips
        effective_rr = (tp_pips - spread_pips) / denom if denom > 0 else 0.0

        zone_label = "PREMIUM" if direction == "SELL" else "DISCOUNT"

        return {
            "signal_id": str(uuid.uuid4()),
            "pair": pair,
            "session": session,
            "killzone": killzone,
            "entry_leg": "B",
            "entry_mode": "DIRECT",
            "timeframe_bias": best_ob["timeframe"],
            "timeframe_entry": best_ob["timeframe"],
            "direction": direction,
            "bias_summary": (
                f"ZGMT-EXCEPTION | {best_ob['ob_type']} OB | "
                f"{best_ob['timeframe']} | Zone: {zone_label} | "
                f"OB H/L: {round(best_ob['body_high'], 5)} / {round(best_ob['body_low'], 5)}"
            ),
            "ob_high": round(best_ob["body_high"], 5),
            "ob_low": round(best_ob["body_low"], 5),
            "entry_price": round(entry_price, 5),
            "sl_price":    round(sl_price,    5),
            "tp1_price":   round(tp1_price,   5),   # TP1 = 1R (partial close here)
            "tp2_price":   round(tp_price,    5),   # TP2 = 2R (runner target)
            "tp3_price":   round(tp3_price,   5),   # TP3 = 3R
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "tp3_pips": round(tp3_pips, 2),
            "spread_pips": spread_pips,
            "effective_rr": round(effective_rr, 3),
            "score": round(95.0 + min(effective_rr * 2.0, 5.0), 1),
            "detected_time": datetime.now(timezone.utc).isoformat(),
            "strategy": "ZGMT-EXCEPTION",
            "setup_type": "ZGMT-EXCEPTION",
            "fixed_lot_size": float(zgmt_cfg.get("fixed_lot_size", 0.0)),
            "skip_rr_check": bool(zgmt_cfg.get("skip_rr_check", True)),
        }

    # ──────────────────────────────────────────────────────────────────

    def _detect_normal_ob(self, df: pd.DataFrame, tf: str) -> list:
        """
        Detects Normal Order Blocks.
        Bullish: last bearish candle before a strong bullish displacement (close > prior high).
        Bearish: last bullish candle before a strong bearish displacement (close < prior low).
        """
        obs = []
        for i in range(len(df) - 4):
            candle = df.iloc[i]
            body_high = float(candle['high'])
            body_low  = float(candle['low'])
            body_mid  = (body_high + body_low) / 2

            # Bullish Normal OB: bearish candle followed by strong upward displacement
            if candle['close'] < candle['open']:
                for j in range(i + 1, min(i + 6, len(df))):
                    if df.iloc[j]['close'] > candle['high']:
                        is_mitigated = self._check_mitigated(df, i, "BUY", body_low)
                        obs.append({
                            "ob_type": "NORMAL",
                            "direction": "BUY",
                            "body_high": body_high,
                            "body_low": body_low,
                            "body_mid": body_mid,
                            "candle_index": i,
                            "timeframe": tf,
                            "is_mitigated": is_mitigated,
                        })
                        break

            # Bearish Normal OB: bullish candle followed by strong downward displacement
            elif candle['close'] > candle['open']:
                for j in range(i + 1, min(i + 6, len(df))):
                    if df.iloc[j]['close'] < candle['low']:
                        is_mitigated = self._check_mitigated(df, i, "SELL", body_high)
                        obs.append({
                            "ob_type": "NORMAL",
                            "direction": "SELL",
                            "body_high": body_high,
                            "body_low": body_low,
                            "body_mid": body_mid,
                            "candle_index": i,
                            "timeframe": tf,
                            "is_mitigated": is_mitigated,
                        })
                        break
        return obs

    # ──────────────────────────────────────────────────────────────────

    def _detect_mitigation_block(self, df: pd.DataFrame, tf: str) -> list:
        """
        Detects Mitigation Blocks: institutions revisit a prior imbalance zone
        (deep 50%+ retracement) before continuing in the original direction.
        Bullish: bullish move → retrace below midpoint → bullish continuation.
        Bearish: bearish move → retrace above midpoint → bearish continuation.
        """
        obs = []
        for i in range(5, len(df) - 5):
            move_start = df.iloc[i - 5]
            move_end   = df.iloc[i]
            move_range = float(move_end['close']) - float(move_start['close'])

            if abs(move_range) < float(df.iloc[i]['close']) * 0.001:
                continue  # Negligibly small move

            midpoint = float(move_start['close']) + move_range * 0.50

            for j in range(i + 1, min(i + 6, len(df))):
                retrace = df.iloc[j]

                # Bullish mitigation: bullish move, retraced below midpoint
                if move_range > 0 and float(retrace['low']) < midpoint:
                    body_high = float(move_start['high'])
                    body_low  = float(move_start['low'])
                    body_mid  = (body_high + body_low) / 2
                    obs.append({
                        "ob_type": "MITIGATION",
                        "direction": "BUY",
                        "body_high": body_high,
                        "body_low": body_low,
                        "body_mid": body_mid,
                        "candle_index": i - 5,
                        "timeframe": tf,
                        "is_mitigated": self._check_mitigated(df, i - 5, "BUY", body_low),
                    })
                    break

                # Bearish mitigation: bearish move, retraced above midpoint
                elif move_range < 0 and float(retrace['high']) > midpoint:
                    body_high = float(move_start['high'])
                    body_low  = float(move_start['low'])
                    body_mid  = (body_high + body_low) / 2
                    obs.append({
                        "ob_type": "MITIGATION",
                        "direction": "SELL",
                        "body_high": body_high,
                        "body_low": body_low,
                        "body_mid": body_mid,
                        "candle_index": i - 5,
                        "timeframe": tf,
                        "is_mitigated": self._check_mitigated(df, i - 5, "SELL", body_high),
                    })
                    break
        return obs

    # ──────────────────────────────────────────────────────────────────

    def _detect_breaker_block(self, df: pd.DataFrame, tf: str) -> list:
        """
        Detects Breaker Blocks — failed OBs that flipped after a structure break.
        Bullish Breaker: bearish OB that price later broke above → now acts as support.
        Bearish Breaker: bullish OB that price later broke below → now acts as resistance.
        """
        normal_obs = self._detect_normal_ob(df, tf)
        breakers = []

        for ob in normal_obs:
            idx = ob["candle_index"]
            subsequent = df.iloc[idx + 1:]

            if ob["direction"] == "SELL":
                # Bullish Breaker: bearish OB that was broken to the upside
                if len(subsequent) > 0 and any(subsequent['close'] > ob["body_high"]):
                    breakers.append({
                        "ob_type": "BREAKER",
                        "direction": "BUY",
                        "body_high": ob["body_high"],
                        "body_low": ob["body_low"],
                        "body_mid": ob["body_mid"],
                        "candle_index": ob["candle_index"],
                        "timeframe": tf,
                        "is_mitigated": self._check_mitigated(df, idx, "BUY", ob["body_low"]),
                    })

            elif ob["direction"] == "BUY":
                # Bearish Breaker: bullish OB that was broken to the downside
                if len(subsequent) > 0 and any(subsequent['close'] < ob["body_low"]):
                    breakers.append({
                        "ob_type": "BREAKER",
                        "direction": "SELL",
                        "body_high": ob["body_high"],
                        "body_low": ob["body_low"],
                        "body_mid": ob["body_mid"],
                        "candle_index": ob["candle_index"],
                        "timeframe": tf,
                        "is_mitigated": self._check_mitigated(df, idx, "SELL", ob["body_high"]),
                    })

        return breakers

    # ──────────────────────────────────────────────────────────────────

    def _check_mitigated(self, df: pd.DataFrame, ob_index: int, direction: str, level: float) -> bool:
        """
        Returns True if the OB has been mitigated (retested) by a subsequent candle.
        BUY OB:  mitigated if any subsequent candle's low  <= body_low.
        SELL OB: mitigated if any subsequent candle's high >= body_high.
        """
        subsequent = df.iloc[ob_index + 1:]
        if len(subsequent) == 0:
            return False
        if direction == "BUY":
            return bool(any(subsequent['low'] <= level))
        else:
            return bool(any(subsequent['high'] >= level))

    # ──────────────────────────────────────────────────────────────────

    def _is_ob_in_fib_zone(
        self,
        df: pd.DataFrame,
        ob: dict,
        swing_lookback: int,
        fib_min: float,
        fib_max: float,
    ) -> bool:
        """
        Validates that the OB body_mid sits inside the correct Fibonacci
        premium or discount zone (wick-to-wick swing high/low).

        Bearish OB: must be in premium zone (above 50% Fib from high→low).
        Bullish OB: must be in discount zone (below 50% Fib from low→high).
        """
        highs = df['high'].values
        lows  = df['low'].values
        n = len(df)

        swing_high = None
        swing_low  = None

        for i in range(swing_lookback, n - swing_lookback):
            # Swing high: wick higher than swing_lookback candles on each side
            if (all(highs[i] > highs[i - k] for k in range(1, swing_lookback + 1)) and
                    all(highs[i] > highs[i + k] for k in range(1, swing_lookback + 1))):
                swing_high = float(highs[i])

            # Swing low: wick lower than swing_lookback candles on each side
            if (all(lows[i] < lows[i - k] for k in range(1, swing_lookback + 1)) and
                    all(lows[i] < lows[i + k] for k in range(1, swing_lookback + 1))):
                swing_low = float(lows[i])

        if swing_high is None or swing_low is None or swing_high <= swing_low:
            return False

        fib_range = swing_high - swing_low

        if ob["direction"] == "SELL":
            # Premium zone: fib drawn from high → low.
            # 50% from top = swing_high - fib_range * 0.50
            # 61.8% from top = swing_high - fib_range * 0.618
            fib_zone_top    = swing_high - fib_range * fib_min   # closer to high (50%)
            fib_zone_bottom = swing_high - fib_range * fib_max   # deeper (61.8%)
            return fib_zone_bottom <= ob["body_mid"] <= fib_zone_top

        else:  # BUY
            # Discount zone: fib drawn from low → high.
            # 50% from bottom = swing_low + fib_range * 0.50
            # 61.8% from bottom = swing_low + fib_range * 0.618
            fib_zone_bottom = swing_low + fib_range * fib_min    # 50%
            fib_zone_top    = swing_low + fib_range * fib_max    # 61.8%
            return fib_zone_bottom <= ob["body_mid"] <= fib_zone_top

    # ──────────────────────────────────────────────────────────────────

    def _calculate_adr_sl(self, pair: str) -> float | None:
        """
        Dynamic SL using the previous N-day ADR (wick-to-wick).
        ADR = average of (daily high − daily low) over the last N completed days.
        SL distance = ADR ÷ 2.
        Returns price-unit distance, or None if data is insufficient.
        """
        adr_days = self.zgmt_cfg.get("zgmt_adr_days", 5)
        # Fetch extra days to account for weekends/Sundays
        d1 = self.mt5.get_candles(pair, "D1", count=adr_days + 5)

        if d1 is None or len(d1) < 2:
            return None

        import pandas as pd
        # Ensure datetime is timezone-aware and parse day of week
        if d1['time'].dt.tz is None:
            d1['time'] = d1['time'].dt.tz_localize('UTC')
            
        # Filter out Sunday candles (weekday == 6) because they represent a tiny 2-hour window
        # which heavily skews the True Average and High-Low range calculations.
        d1_filtered = d1[d1['time'].dt.weekday != 6]
        
        if len(d1_filtered) < adr_days + 1:
            logger.warning(f"[{pair}] ZGMT-EXCEPTION: Insufficient valid D1 trading days for ADR.")
            return None

        # The last row is the current forming candle. We want `adr_days` COMPLETED candles before it.
        completed = d1_filtered.iloc[-adr_days - 1 : -1]
        
        adr_mode = self.zgmt_cfg.get("adr_mode", "HIGH_LOW_RANGE")
        
        if adr_mode == "HIGH_LOW_RANGE":
            highest_high = float(completed['high'].max())
            lowest_low = float(completed['low'].min())
            adr = (highest_high - lowest_low) / adr_days
        else:  # TRUE_AVERAGE
            adr = float((completed['high'] - completed['low']).mean())
            
        return adr / 2  # SL = ADR ÷ 2
