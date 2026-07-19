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
from zoneinfo import ZoneInfo
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
        # Cache for HTF OBs to speed up backtests
        self._htf_ob_cache: dict = {}

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
        - Indices (US30, etc): 1.0
        - All other FX pairs: 0.0001
        """
        p = pair.upper()
        if "JPY" in p:
            return 0.01
        if "XAU" in p or "XAG" in p:  # Gold AND Silver use 0.01
            return 0.01
        elif any(idx in p for idx in ["US500", "SPX", "USTEC", "US100", "NAS100"]):
            return 0.1
        elif any(idx in p for idx in ["US30", "GER40", "UK100", "WS30"]):
            return 1.0
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

    def _get_asia_killzone_end_broker_ts(self, now_utc: datetime, pair: str) -> int:
        now_ist = self._to_ist(now_utc)
        m = now_ist.month
        d = now_ist.day
        is_summer = False
        if 3 < m < 11:
            is_summer = True
        elif m == 3 and d >= 9:
            is_summer = True
            
        timings = self.config.killzone_timings_summer if is_summer else self.config.killzone_timings_winter
        asia_end_str = timings.get("Asia", {}).get("end", "07:30")
        end_t = self._parse_hhmm(asia_end_str)
        
        end_ist = now_ist.replace(hour=end_t.hour, minute=end_t.minute, second=0, microsecond=0)
        
        # If the generated end time is in the past (or exactly now), 
        # push it slightly into the future so MT5 doesn't reject it immediately.
        if end_ist <= now_ist:
            end_ist = now_ist + timedelta(minutes=1)
            
        # end_ist currently holds the IST digits but its tzinfo is STILL timezone.utc
        # because _to_ist only adds a timedelta. We must shift it back to true UTC 
        # digits before computing the absolute POSIX timestamp.
        true_utc_end = end_ist - self._IST_OFFSET
        
        # MT5 requires the expiration timestamp to be expressed in local broker time.
        broker_offset_hours = self._get_broker_utc_offset_hours(pair)
        broker_ts = int(true_utc_end.timestamp()) + int(broker_offset_hours * 3600)
        return broker_ts

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

    def _get_t_minus_1_window(self) -> tuple[datetime, datetime]:
        """
        Calculates the exact 24-hour window for the previous trading day (T-1) 
        based strictly on the Forex market close (5:00 PM New York time).
        This automatically handles Daylight Saving Time (DST) shifts between 
        21:00 UTC (Summer/EDT) and 22:00 UTC (Winter/EST).
        Returns (start_utc, end_utc) as UTC-aware datetimes.
        """
        current_utc = self._utc_now()
        ny_tz = ZoneInfo("America/New_York")
        now_ny = current_utc.astimezone(ny_tz)
        
        # 1. Find the most recent 17:00 (5 PM) NY time
        recent_close = now_ny.replace(hour=17, minute=0, second=0, microsecond=0)
        
        # If it is currently before 17:00 in NY, the most recent close was yesterday
        if now_ny < recent_close:
            recent_close -= timedelta(days=1)
            
        # The end of T-1 is the recent_close
        t_start = recent_close
        
        # 2. Skip the weekend gap
        # Forex closes Friday 17:00 NY and opens Sunday 17:00 NY.
        if t_start.weekday() == 6: # Sunday 17:00
            t_start -= timedelta(days=2) # Shift back to Friday 17:00
        elif t_start.weekday() == 5: # Saturday 17:00 (market closed, but just in case)
            t_start -= timedelta(days=1) # Shift back to Friday 17:00
            
        t_minus_1_start = t_start - timedelta(days=1)
        
        return t_minus_1_start.astimezone(timezone.utc), t_start.astimezone(timezone.utc)

    def _get_daily_bias(self, pair: str, zgmt_cfg: dict, zgmt_price: float) -> tuple[str | None, bool, float, float]:
        """
        Determine bullish or bearish bias for Leg A and Leg C.
        Uses T-1 (Yesterday) 50% midpoint, defined precisely as the 24-hour window
        from 2:30 AM IST (T-1) to 2:30 AM IST (T), to align with Forex daily close.
        If 0 GMT > Midpoint -> SELL (Bearish).
        If 0 GMT < Midpoint -> BUY (Bullish).
        Returns Tuple[bias_str_or_none, is_structural_absence, range_high, range_low].
        """
        start_utc, end_utc = self._get_t_minus_1_window()
        
        # Fetch enough M15 candles to cover the lookback (~4 days max = 384 candles)
        candles = self.mt5.get_candles(pair, "M15", count=400)
        if candles is None or candles.empty:
            logger.debug(f"[{pair}] ZGMT: Insufficient M15 candles for PD bias.")
            return None, True, 0.0, 0.0
            
        # Filter candles exactly within the [start_utc, end_utc) window
        window_highs = []
        window_lows = []
        
        for _, row in candles.iterrows():
            cand_time = row["time"]
            if cand_time.tzinfo is None:
                cand_time = cand_time.replace(tzinfo=timezone.utc)
                
            if start_utc <= cand_time < end_utc:
                window_highs.append(float(row["high"]))
                window_lows.append(float(row["low"]))
                
        if not window_highs or not window_lows:
            logger.debug(f"[{pair}] ZGMT: No M15 candles found in window {start_utc} to {end_utc} for T-1 PD bias.")
            return None, True, 0.0, 0.0
            
        range_high = max(window_highs)
        range_low = min(window_lows)

        if range_high <= range_low:
            logger.debug(f"[{pair}] ZGMT: Invalid true range high={range_high} low={range_low}.")
            return None, True, 0.0, 0.0

        midpoint = (range_high + range_low) / 2.0

        if zgmt_price > midpoint:
            bias = "BEARISH"  # 0 GMT opened above yesterday's 50% (sell side)
        elif zgmt_price < midpoint:
            bias = "BULLISH"  # 0 GMT opened below yesterday's 50% (buy side)
        else:
            bias = None  # Exactly at equilibrium

        start_ist_str = (start_utc + self._IST_OFFSET).strftime('%Y-%m-%d %A %H:%M')
        end_ist_str   = (end_utc   + self._IST_OFFSET).strftime('%H:%M')
        logger.info(
            f"[{pair}] ZGMT: Using precise 24h T-1 window ({start_ist_str} IST → {end_ist_str} IST) for PD Array | "
            f"High={range_high:.5f} Low={range_low:.5f} Mid={midpoint:.5f} | "
            f"0GMT={zgmt_price:.5f} → Bias={bias}"
        )
        return bias, False, range_high, range_low

    def _is_pd_array_swept_yet(self, pair: str, range_high: float, range_low: float) -> bool:
        """
        Check if the T-1 High or Low was swept between the New York Close (start of Asian session, exactly 2:30 AM IST in summer)
        and the CURRENT exact time. This ensures that if liquidity is grabbed before a signal fires, the setup is invalidated.
        """
        now_utc = self._utc_now()
        _, ny_close_utc = self._get_t_minus_1_window()
        
        window_start = ny_close_utc
        window_end = now_utc
            
        candles = self.mt5.get_candles(pair, "M15", count=96)
        if candles is None or candles.empty:
            return False
            
        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
                
            if window_start <= candle_time < window_end:
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

        # We no longer hard-block for 3 minutes; if the candle is available, we use it immediately.

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
                    candle_time.hour == target_broker_datetime.hour):
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
        self, pair: str, bias: str, zgmt_price: float, tick: dict, zgmt_cfg: dict, 
        override_entry_mode: str = None, range_high: float = None, range_low: float = None
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
        # Note: Leg C (FILTER) now dynamically calculates filter_pips. 
        # We initialize it here as 0 to be overwritten dynamically.
        filter_pips = 0.0

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
                # ZGMT-C: 50% of (Previous Day Low - 0GMT Price)
                if range_low is None:
                    logger.debug(f"[{pair}] ZGMT-C INVALID: range_low is None.")
                    return None
                
                pip_size = self._pip_size(pair)
                gap_pips = abs(zgmt_price - range_low) / pip_size
                if gap_pips < 5:
                    logger.debug(f"[{pair}] ZGMT-C INVALID: Gap (0GMT to PrevLow) is {gap_pips:.1f} pips (< 5).")
                    return None
                
                entry_price = (range_low + zgmt_price) / 2.0
                filter_pips = abs(zgmt_price - entry_price) / pip_size
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
                # ZGMT-C: 50% of (Previous Day High - 0GMT Price)
                if range_high is None:
                    logger.debug(f"[{pair}] ZGMT-C INVALID: range_high is None.")
                    return None
                
                pip_size = self._pip_size(pair)
                gap_pips = abs(range_high - zgmt_price) / pip_size
                if gap_pips < 5:
                    logger.debug(f"[{pair}] ZGMT-C INVALID: Gap (0GMT to PrevHigh) is {gap_pips:.1f} pips (< 5).")
                    return None
                
                entry_price = (range_high + zgmt_price) / 2.0
                filter_pips = abs(entry_price - zgmt_price) / pip_size
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
        # If a specific list of pairs is defined for ZGMT, filter by it.
        zgmt_cfg = self.config.zgmt_scanner
        allowed_pairs = zgmt_cfg.get("pairs", [])
        if allowed_pairs and pair not in allowed_pairs:
            return None

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
            if is_structural:
                self._mark_daily_finalized(pair)
            return None

        # ── Step 2: Daily bias (Leg A & C) ────────────────────────────
        require_pd = zgmt_cfg.get("require_pd_array_check", True)
        pd_swept_yet = False
        range_high = None
        range_low = None
        
        if require_pd:
            bias, is_structural, range_high, range_low = self._get_daily_bias(pair, zgmt_cfg, zgmt_price)
            if bias is None:
                logger.info(f"[{pair}] ZGMT: Could not determine D1 PD bias — skipping.")
                if is_structural:
                    self._mark_daily_finalized(pair)
                return None
                
            if self._is_pd_array_swept_yet(pair, range_high, range_low):
                pd_swept_yet = True
                logger.info(f"[{pair}] ZGMT: PD Array swept before 0 GMT. Leg A and C invalidated.")
        else:
            # No bias check: infer from current price vs yesterday's close
            logger.debug(f"[{pair}] ZGMT: PD array check disabled — using tick direction.")
            tick = self.mt5.get_tick(pair)
            bias = "BULLISH" if (tick and tick["bid"] > zgmt_price) else "BEARISH"
            range_high, range_low = None, None
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
        strategy_c_valid = not is_tested

        if is_tested:
            now_utc = self._utc_now()
            today_zgmt_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            exclude_mins = zgmt_cfg.get("zgmt_test_exclude_first_mins", 15)
            test_start_time = today_zgmt_utc + timedelta(minutes=exclude_mins)

            if now_utc < test_start_time:
                # Inside exclusion window. Price hasn't displaced yet.
                # Leg A: We MUST allow it so it can place the Limit order 3 mins into the Killzone.
                strategy_a_valid = True
                # Leg C: Not allowed yet (needs displacement first)
                strategy_c_valid = False
                logger.debug(f"[{pair}] ZGMT Step 2B: Inside exclusion window. Leg A allowed, Leg C deferred.")
            else:
                # Level has genuinely been touched after the exclusion window → invalid for A & C
                strategy_a_valid = False
                strategy_c_valid = False
                logger.debug(f"[{pair}] ZGMT Step 2B: 0 GMT level already tested ({zgmt_price:.5f}) — A & C invalid.")
                
                # Optimization: if B is disabled, finalize the day early
                if not zgmt_cfg.get("strategy_b_enabled", False):
                    logger.info(f"[{pair}] ZGMT Step 2B: 0 GMT level tested and B disabled. Finalizing for today.")
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
                logger.info(f"[{pair}] ZGMT-B: HTF OB condition met ({ob_signal['timeframe_entry']} timeframe).")

        signals_to_emit = []

        def build_signal_dict(levs: dict, strat_id: str, expiration_time: int = 0) -> dict:
            spr = self.mt5.get_current_spread(pair)
            spread_val = spr * self._pip_size(pair) if spr > 0 else 0.0
            
            entry = levs["entry_price"]
            
            if bias == "BULLISH":
                # For BUY: SL is below entry. Subtract spread to move it further down.
                sl_adjusted = levs["sl_price"] - spread_val
                new_risk = entry - sl_adjusted
                
                tp1_adjusted = entry + (new_risk * 1)
                tp2_adjusted = entry + (new_risk * 2)
                tp3_adjusted = entry + (new_risk * 3) if levs.get("tp3_price") else 0.0
            else:
                # For SELL: SL is above entry. Add spread to move it further up.
                sl_adjusted = levs["sl_price"] + spread_val
                new_risk = sl_adjusted - entry
                
                tp1_adjusted = entry - (new_risk * 1)
                tp2_adjusted = entry - (new_risk * 2)
                tp3_adjusted = entry - (new_risk * 3) if levs.get("tp3_price") else 0.0
                
            new_sl_pips = new_risk / self._pip_size(pair)
            new_tp_pips = new_sl_pips * 2
            new_tp3_pips = new_sl_pips * 3
            
            eff_rr = 2.0  # By definition, TP2 is exactly 2R now
            
            pd_zone = "DISCOUNT" if bias == "BULLISH" else "PREMIUM"
            filt_note = f" ±{levs['filter_pips']}pips" if strat_id == "ZGMT-C" else ""
            summary = f"{strat_id} | PD: {pd_zone} | {filt_note} | 0GMT={zgmt_price:.5f}"
            
            signal_dict = {
                "signal_id": str(uuid.uuid4()),
                "pair": pair,
                "session": session,
                "killzone": killzone,
                "entry_leg": {"ZGMT-A": "A", "ZGMT-B": "B", "ZGMT-C": "C"}.get(strat_id, "A"),
                "entry_mode": "FILTER",  # ZGMT-A and ZGMT-C both use Limit (Pending) Orders
                "timeframe_bias": zgmt_cfg.get("timeframe_bias", "D1"),
                "timeframe_entry": zgmt_cfg.get("timeframe_entry", "H1"),
                "direction": "BUY" if bias == "BULLISH" else "SELL",
                "bias_summary": summary,
                "entry_price": entry,
                "sl_price": round(sl_adjusted, 5),
                "tp1_price": round(tp1_adjusted, 5),
                "tp2_price": round(tp2_adjusted, 5),
                "tp3_price": round(tp3_adjusted, 5),
                "sl_pips": round(new_sl_pips, 2),
                "tp_pips": round(new_tp_pips, 2),
                "tp3_pips": round(new_tp3_pips, 2),
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
            if expiration_time > 0:
                signal_dict["expiration_time"] = expiration_time
            return signal_dict

        # Strategy A (0 GMT Liquidity)
        if strategy_a_valid and not pd_swept_yet and is_in_zgmt_window and zgmt_cfg.get("strategy_a_enabled", False):
            # Only take Strategy A in the Asian Killzone
            if killzone and killzone.lower() == "asia":
                # We use DIRECT calculation to ensure entry_price == 0GMT price (no offset)
                levs_direct = self._compute_entry_sl_tp(
                    pair, bias, zgmt_price, tick, zgmt_cfg, 
                    override_entry_mode="DIRECT", 
                    range_high=range_high, range_low=range_low
                )
                if levs_direct:
                    asia_end_ts = self._get_asia_killzone_end_broker_ts(self._utc_now(), pair)
                    signals_to_emit.append(build_signal_dict(levs_direct, "ZGMT-A", expiration_time=asia_end_ts))
                    logger.info(f"[{pair}] ZGMT-A VALID: Limit order at 0 GMT price scheduled.")
            else:
                logger.debug(f"[{pair}] ZGMT: Skipping Strategy A because killzone '{killzone}' is not Asia.")

        # Strategy B (0 GMT + OB Model)
        if ob_signal is not None:
            signals_to_emit.append(ob_signal)

        # Strategy C (Manipulation / Judas Swing)
        if zgmt_cfg.get("strategy_c_enabled", False):
            if not strategy_c_valid:
                logger.debug(f"[{pair}] ZGMT-C INVALID: Strategy C is invalid (0 GMT level already tested or in exclusion window).")
            elif pd_swept_yet:
                logger.debug(f"[{pair}] ZGMT-C INVALID: PD array already swept before 0 GMT.")
            elif not killzone or killzone.lower() != "asia":
                logger.debug(f"[{pair}] ZGMT-C INVALID: Skipping Strategy C because killzone '{killzone}' is not Asia.")
            else:
                levs_filter = self._compute_entry_sl_tp(
                    pair, bias, zgmt_price, tick, zgmt_cfg, 
                    override_entry_mode="FILTER", 
                    range_high=range_high, range_low=range_low
                )
                if levs_filter:
                    asia_end_ts = self._get_asia_killzone_end_broker_ts(self._utc_now(), pair)
                    signals_to_emit.append(build_signal_dict(levs_filter, "ZGMT-C", expiration_time=asia_end_ts))
                    logger.info(f"[{pair}] ZGMT-C VALID: Judas swing limit order added.")
                else:
                    logger.debug(f"[{pair}] ZGMT-C INVALID: Failed to compute entry/sl/tp levels.")

        if not signals_to_emit:
            return None

        for s in signals_to_emit:
            logger.info(
                f"[{pair}] ZGMT Signal ✅ {s['direction']} | "
                f"Entry={s['entry_price']:.5f} SL={s['sl_price']:.5f} "
                f"TP2={s['tp2_price']:.5f} | 0GMT={zgmt_price:.5f} | "
                f"Mode={s.get('setup_type', 'ZGMT')} | Score={s['score']}"
            )

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

        if not killzone or killzone.lower() == "none":
            # Leg B must tap within SOME killzone (Asia, London, or NY)
            return None

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
            
            last_candle_time = df.iloc[-1]['time']
            cache_key = f"{pair}_{tf}_{last_candle_time}"
            
            if cache_key in self._htf_ob_cache:
                tf_obs = self._htf_ob_cache[cache_key]
            else:
                tf_obs = []
                tf_obs += self._detect_normal_ob(df, tf, pair)
                tf_obs += self._detect_mitigation_block(df, tf, pair)
                tf_obs += self._detect_breaker_block(df, tf, pair)
                
                # Clean up old cache keys for this pair and timeframe
                keys_to_delete = [k for k in self._htf_ob_cache if k.startswith(f"{pair}_{tf}_")]
                for k in keys_to_delete:
                    del self._htf_ob_cache[k]
                    
                self._htf_ob_cache[cache_key] = tf_obs
                
            all_obs += tf_obs

        if not all_obs:
            return None
            
        # logger.debug(f"[{pair}] _check_htf_ob_exception: Found {len(all_obs)} total OBs across H4/H1")

        # Filter 1: unmitigated only
        valid_obs = [ob for ob in all_obs if not ob["is_mitigated"]]
        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(valid_obs)} OBs survived unmitigated filter")
        if not valid_obs:
            logger.debug(f"[{pair}] ZGMT-B INVALID: No unmitigated OBs found.")
            return None

        # Filter 1b: direction must match daily bias
        # BULLISH bias → look for BUY OBs (in Discount)
        # BEARISH bias → look for SELL OBs (in Premium)
        expected_ob_dir = "BUY" if bias == "BULLISH" else "SELL"
        valid_obs = [ob for ob in valid_obs if ob["direction"] == expected_ob_dir]
        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(valid_obs)} {expected_ob_dir} OBs after direction filter (bias={bias})")
        if not valid_obs:
            logger.debug(f"[{pair}] ZGMT-B INVALID: No {expected_ob_dir} OBs matching daily bias={bias}.")
            return None

        # Filter 2: price must currently be tapping into the OB zone
        tapping_obs = [
            ob for ob in valid_obs
            if (ob["body_low"] - tap_threshold) <= current_price <= (ob["body_high"] + tap_threshold)
        ]
        if not tapping_obs:
            logger.debug(f"[{pair}] ZGMT-B INVALID: Current price {current_price} is not tapping any valid {expected_ob_dir} OBs.")
            return None
        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(tapping_obs)} OBs tapping current price {current_price}")

        # Filter 3: OB must sit inside the correct Fibonacci premium / discount zone
        # Pre-compute dealing ranges per timeframe using proper swing detection
        h4_swing_l = zgmt_cfg.get("zgmt_swing_l_h4", 2)
        h4_swing_r = zgmt_cfg.get("zgmt_swing_r_h4", 2)
        h1_swing_l = zgmt_cfg.get("zgmt_swing_l_h1", 2)
        h1_swing_r = zgmt_cfg.get("zgmt_swing_r_h1", 2)

        dealing_ranges = {}
        for df_tf, tf_name, L, R in [
            (h4_candles, "H4", h4_swing_l, h4_swing_r),
            (h1_candles, "H1", h1_swing_l, h1_swing_r),
        ]:
            if df_tf is None or (hasattr(df_tf, 'empty') and df_tf.empty):
                dealing_ranges[tf_name] = None
                continue
            df_tf = df_tf.reset_index(drop=True)
            dealing_ranges[tf_name] = self._get_dealing_range(df_tf, bias, L, R, pair)
            if dealing_ranges[tf_name]:
                sl, sh = dealing_ranges[tf_name]
                logger.debug(
                    f"[{pair}] ZGMT-B {tf_name} Dealing Range: SwingLow={sl:.5f} SwingHigh={sh:.5f} | Bias={bias}"
                )
            else:
                logger.debug(f"[{pair}] ZGMT-B {tf_name}: No valid dealing range found for bias={bias}.")

        fib_valid_obs = []
        for ob in tapping_obs:
            dr = dealing_ranges.get(ob["timeframe"])
            if dr is None:
                logger.debug(f"[{pair}] ZGMT-B: Skipping {ob['timeframe']} OB — no dealing range available.")
                continue
            if self._is_ob_in_fib_zone(ob, dr, bias, pair):
                fib_valid_obs.append(ob)

        logger.debug(f"[{pair}] _check_htf_ob_exception: {len(fib_valid_obs)} OBs survived Fib Filter")
        if not fib_valid_obs:
            logger.debug(f"[{pair}] ZGMT-B INVALID: No valid OBs found in Premium/Discount zones.")
            return None

        # Select best OB based on Priority Scoring (Highest Score Wins)
        # 1. Distance Score: Closer to current price is better
        # 2. Zone Score: fib_score_bonus (+10 for key sub-zone, +5 for general zone)
        # 3. Type Score: Normal (+5), Breaker (+3), Mitigation (+1)
        # 4. Recency Score: Higher for more recent (larger candle_index)
        for ob in fib_valid_obs:
            dist = abs(ob["body_mid"] - current_price) / pip_size
            dist_score = (1.0 / (dist + 1.0)) * 10.0
            
            zone_score = ob.get("fib_score_bonus", 5)
            
            ob_type = ob.get("type", "NORMAL")
            type_score = 5 if ob_type == "NORMAL" else (3 if ob_type == "BREAKER" else 1)
            
            # Recency: normalise index relative to total candles (approximation: higher index = more recent)
            recency_score = ob["candle_index"] * 0.01 
            
            ob["priority_score"] = dist_score + zone_score + type_score + recency_score
            
        fib_valid_obs.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
        best_ob = fib_valid_obs[0]

        direction = best_ob["direction"]

        if direction == "BUY" and not zgmt_cfg.get("allow_buy", True):
            logger.debug(f"[{pair}] ZGMT-B: direction BUY but allow_buy=False")
            return None
        if direction == "SELL" and not zgmt_cfg.get("allow_sell", True):
            logger.debug(f"[{pair}] ZGMT-B: direction SELL but allow_sell=False")
            return None

        entry_price = best_ob["body_mid"]

        # ── SL via ADR5/2 (strategy spec: SL = 5-day ADR ÷ 2, wick-to-wick) ────
        # This is the same formula used by ZGMT-A and ZGMT-C.
        adr_sl_dist = self._calculate_adr_sl(pair)
        if not adr_sl_dist or adr_sl_dist <= 0:
            logger.warning(
                f"[{pair}] ZGMT-B: ADR(5) unavailable — signal cancelled. "
                f"Cannot calculate SL without real ADR data."
            )
            return None

        if direction == "BUY":
            ob_sl_dist = entry_price - float(best_ob.get("candle_low", entry_price))
        else:
            ob_sl_dist = float(best_ob.get("candle_high", entry_price)) - entry_price
            
        ob_sl_dist += (2 * pip_size) # 2 pip buffer
        
        sl_distance = max(adr_sl_dist, ob_sl_dist)   # price-unit distance
        sl_pips = sl_distance / pip_size
        tp_pips = sl_pips * 2

        logger.debug(
            f"[{pair}] ZGMT-B SL: ADR5/2 = {adr_sl_dist/pip_size:.1f} pips | "
            f"OB_extreme = {ob_sl_dist/pip_size:.1f} pips | "
            f"Final SL = {sl_pips:.1f} pips | entry={entry_price:.5f}"
        )

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
            "entry_mode": "FILTER",
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

    def _calc_atr14(self, df: pd.DataFrame, up_to_index: int) -> float:
        """
        Compute ATR(14) using the 14 candles immediately before `up_to_index`.
        Returns 0.0 if insufficient data.
        """
        start = max(1, up_to_index - 14)
        highs  = df['high'].values.astype(float)
        lows   = df['low'].values.astype(float)
        closes = df['close'].values.astype(float)
        trs = []
        for k in range(start, up_to_index):
            tr = max(
                highs[k] - lows[k],
                abs(highs[k] - closes[k - 1]),
                abs(lows[k]  - closes[k - 1]),
            )
            trs.append(tr)
        return sum(trs) / len(trs) if trs else 0.0

    def _detect_normal_ob(self, df: pd.DataFrame, tf: str, pair: str) -> list:
        """
        Detects Normal Order Blocks per spec (Section 4.3).

        Bullish Normal OB (direction=BUY):
          1. OB candle[i] is BEARISH (close < open).
          2. Within D=5 candles, a displacement candle closes ABOVE OB body_high.
          3. Total bullish move (max high of displacement window - OB low) >= 2 × ATR(14).
          4. Displacement candle close breaks the most recent prior swing high.

        Bearish Normal OB (direction=SELL):
          1. OB candle[i] is BULLISH (close > open).
          2. Within D=5 candles, a displacement candle closes BELOW OB body_low.
          3. Total bearish move (OB high - min low of displacement window) >= 2 × ATR(14).
          4. Displacement candle close breaks the most recent prior swing low.

        OB zone uses BODY only (no wicks):
            body_high = max(open, close)
            body_low  = min(open, close)
        """
        obs = []
        highs  = df['high'].values.astype(float)
        lows   = df['low'].values.astype(float)
        closes = df['close'].values.astype(float)
        opens  = df['open'].values.astype(float)
        n = len(df)
        zgmt_cfg = self.zgmt_cfg
        D = zgmt_cfg.get("zgmt_ob_displacement_candles", 5)
        atr_mult = zgmt_cfg.get("zgmt_ob_atr_multiplier", 2.0)

        for i in range(n - 2):  # need at least 1 displacement candle; j is bounded by min(i+D+1, n)
            body_high = float(max(opens[i], closes[i]))
            body_low  = float(min(opens[i], closes[i]))
            body_mid  = (body_high + body_low) / 2.0

            # ATR(14) at the OB candle
            atr14 = self._calc_atr14(df, i)

            # Collect most recent prior swing high and swing low before index i
            lookback = min(i, zgmt_cfg.get("zgmt_swing_lookback", 3))
            prior_highs = [float(highs[k]) for k in range(i - lookback, i)] if lookback > 0 else []
            prior_lows  = [float(lows[k])  for k in range(i - lookback, i)] if lookback > 0 else []
            prev_swing_high = max(prior_highs) if prior_highs else None
            prev_swing_low  = min(prior_lows)  if prior_lows else None

            # ── Bullish Normal OB: bearish candle ────────────────────────
            if closes[i] < opens[i]:
                for j in range(i + 1, min(i + D + 1, n)):
                    disp_close = float(closes[j])
                    if disp_close > body_high:
                        # Check 1: total bullish move >= 2×ATR(14)
                        window_high = float(max(highs[i + 1 : j + 1]))
                        total_move  = window_high - float(lows[i])
                        if atr14 > 0 and total_move < atr_mult * atr14:
                            logger.debug(
                                f"[{pair}] Normal OB(BUY) at {i}: displacement {total_move:.5f} "
                                f"< {atr_mult}×ATR14={atr_mult*atr14:.5f} — skipped."
                            )
                            break  # No point checking further candles in this window

                        # Check 2: displacement close must break a prior swing high
                        if prev_swing_high is not None and disp_close <= prev_swing_high:
                            logger.debug(
                                f"[{pair}] Normal OB(BUY) at {i}: displacement close {disp_close:.5f} "
                                f"did not break prior swing high {prev_swing_high:.5f} — skipped."
                            )
                            break

                        is_mitigated = self._check_mitigated_after(df, j, "BUY", body_low, pair)
                        obs.append({
                            "ob_type": "NORMAL",
                            "direction": "BUY",
                            "body_high": body_high,
                            "body_low":  body_low,
                            "body_mid":  body_mid,
                            "candle_high": float(highs[i]),
                            "candle_low": float(lows[i]),
                            "candle_index": i,
                            "displacement_index": j,
                            "timeframe": tf,
                            "is_mitigated": is_mitigated,
                        })
                        break

            # ── Bearish Normal OB: bullish candle ────────────────────────
            elif closes[i] > opens[i]:
                for j in range(i + 1, min(i + D + 1, n)):
                    disp_close = float(closes[j])
                    if disp_close < body_low:
                        # Check 1: total bearish move >= 2×ATR(14)
                        window_low  = float(min(lows[i + 1 : j + 1]))
                        total_move  = float(highs[i]) - window_low
                        if atr14 > 0 and total_move < atr_mult * atr14:
                            logger.debug(
                                f"[{pair}] Normal OB(SELL) at {i}: displacement {total_move:.5f} "
                                f"< {atr_mult}×ATR14={atr_mult*atr14:.5f} — skipped."
                            )
                            break

                        # Check 2: displacement close must break a prior swing low
                        if prev_swing_low is not None and disp_close >= prev_swing_low:
                            logger.debug(
                                f"[{pair}] Normal OB(SELL) at {i}: displacement close {disp_close:.5f} "
                                f"did not break prior swing low {prev_swing_low:.5f} — skipped."
                            )
                            break

                        is_mitigated = self._check_mitigated_after(df, j, "SELL", body_high, pair)
                        obs.append({
                            "ob_type": "NORMAL",
                            "direction": "SELL",
                            "body_high": body_high,
                            "body_low":  body_low,
                            "body_mid":  body_mid,
                            "candle_high": float(highs[i]),
                            "candle_low": float(lows[i]),
                            "candle_index": i,
                            "displacement_index": j,
                            "timeframe": tf,
                            "is_mitigated": is_mitigated,
                        })
                        break
        return obs

    # ──────────────────────────────────────────────────────────────────

    def _detect_mitigation_block(self, df: pd.DataFrame, tf: str, pair: str) -> list:
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
                        "is_mitigated": self._check_mitigated_after(df, i - 5, "BUY", body_high, pair),
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
                        "is_mitigated": self._check_mitigated_after(df, i - 5, "SELL", body_low, pair),
                    })
                    break
        return obs

    # ──────────────────────────────────────────────────────────────────

    def _detect_breaker_block(self, df: pd.DataFrame, tf: str, pair: str) -> list:
        """
        Detects Breaker Blocks — failed OBs that flipped role after a body close
        through the OB extreme, followed by a market structure shift.

        Bullish Breaker:
          1. Start with a BEARISH normal OB (body_high = max(open,close) of that candle).
          2. A later candle's BODY closes ABOVE the OB body_high  ← body close, not wick.
          3. After that break, price takes a prior swing HIGH     ← structure shift up.
          4. On retrace the failed bearish OB body now acts as support.

        Bearish Breaker:
          1. Start with a BULLISH normal OB.
          2. A later candle's BODY closes BELOW the OB body_low.
          3. After that break, price takes a prior swing LOW      ← structure shift down.
          4. On retrace the failed bullish OB body acts as resistance.
        """
        normal_obs = self._detect_normal_ob(df, tf, pair)
        breakers = []
        highs  = df['high'].values.astype(float)
        lows   = df['low'].values.astype(float)
        opens  = df['open'].values.astype(float)
        closes = df['close'].values.astype(float)
        n = len(df)

        for ob in normal_obs:
            idx = ob["candle_index"]
            disp_idx = ob.get("displacement_index", idx + 1)
            # Look only after the displacement candle
            search_start = disp_idx + 1
            if search_start >= n:
                continue

            if ob["direction"] == "SELL":
                # ── Bullish Breaker ──────────────────────────────────────────
                # Step 1: find a candle whose BODY closes above the OB body_high
                break_idx = None
                for k in range(search_start, n):
                    body_close_k = float(closes[k])
                    if body_close_k > ob["body_high"]:
                        break_idx = k
                        break
                if break_idx is None:
                    continue  # Never broke — not a breaker

                # Step 2: after the break candle, look for a structure shift UP
                # (price takes a swing high that existed BEFORE the break candle)
                # Use a simple proxy: any high after break_idx exceeds a prior swing high
                pre_break_highs = highs[:break_idx]
                if len(pre_break_highs) == 0:
                    continue
                ref_swing_high = float(pre_break_highs.max())
                structure_shift = any(
                    float(highs[k]) > ref_swing_high for k in range(break_idx + 1, n)
                )
                if not structure_shift:
                    logger.debug(
                        f"[{tf}] Breaker(BUY) at idx={idx}: break at {break_idx} "
                        f"but no structure shift above {ref_swing_high:.5f} — skipped."
                    )
                    continue

                breakers.append({
                    "ob_type": "BREAKER",
                    "direction": "BUY",
                    "body_high": ob["body_high"],
                    "body_low":  ob["body_low"],
                    "body_mid":  ob["body_mid"],
                    "candle_index": break_idx,
                    "displacement_index": break_idx,
                    "timeframe": tf,
                    # Mitigated only if a LATER candle wick enters the body zone
                    # (check from break_idx+1 so the break candle itself doesn't count)
                    "is_mitigated": self._check_mitigated_after(df, break_idx, "BUY", ob["body_low"], pair),
                })

            elif ob["direction"] == "BUY":
                # ── Bearish Breaker ──────────────────────────────────────────
                # Step 1: find a candle whose BODY closes below the OB body_low
                break_idx = None
                for k in range(search_start, n):
                    body_close_k = float(closes[k])
                    if body_close_k < ob["body_low"]:
                        break_idx = k
                        break
                if break_idx is None:
                    continue

                # Step 2: after the break, look for a structure shift DOWN
                pre_break_lows = lows[:break_idx]
                if len(pre_break_lows) == 0:
                    continue
                ref_swing_low = float(pre_break_lows.min())
                structure_shift = any(
                    float(lows[k]) < ref_swing_low for k in range(break_idx + 1, n)
                )
                if not structure_shift:
                    logger.debug(
                        f"[{tf}] Breaker(SELL) at idx={idx}: break at {break_idx} "
                        f"but no structure shift below {ref_swing_low:.5f} — skipped."
                    )
                    continue

                breakers.append({
                    "ob_type": "BREAKER",
                    "direction": "SELL",
                    "body_high": ob["body_high"],
                    "body_low":  ob["body_low"],
                    "body_mid":  ob["body_mid"],
                    "candle_index": break_idx,
                    "displacement_index": break_idx,
                    "timeframe": tf,
                    "is_mitigated": self._check_mitigated_after(df, break_idx, "SELL", ob["body_high"], pair),
                })

        return breakers

    # ──────────────────────────────────────────────────────────────────



    def _check_mitigated_after(self, df: pd.DataFrame, start_after_index: int, direction: str, level: float, pair: str) -> bool:
        """
        Returns True if the OB has been mitigated (retested) by any candle
        AFTER start_after_index (i.e., start_after_index+1 onwards).

        Uses WICK touch per strategy spec:
          BUY  OB mitigated if any later wick low  reaches OB body_low  (level).
          SELL OB mitigated if any later wick high reaches OB body_high (level).
          
        Uses a tolerance of 1 pip (0.0001) for mitigation.
        """
        subsequent = df.iloc[start_after_index + 1:]
        if len(subsequent) == 0:
            return False
            
        mitigation_tolerance = 1.0 * self._pip_size(pair)
            
        if direction == "BUY":
            # Mitigated if any wick low drops into or below the OB body_low + tolerance
            threshold = level + mitigation_tolerance
            return bool(any(subsequent['low'] <= threshold))
        else:
            # Mitigated if any wick high rises into or above the OB body_high - tolerance
            threshold = level - mitigation_tolerance
            return bool(any(subsequent['high'] >= threshold))

    # ──────────────────────────────────────────────────────────────────
    # Proper Swing Detection (per strategy spec)
    # ──────────────────────────────────────────────────────────────────

    def _detect_swings(
        self,
        df: pd.DataFrame,
        L: int = 2,
        R: int = 2,
        atr_min_separation: float = 0.5,  # kept for API compat but no longer used
    ) -> tuple[list, list]:
        """
        Detect swing highs and swing lows using wick highs/lows per spec (Section 1).

        Rules (per spec):
          - Swing High at i: high[i] STRICTLY > high[i-j] AND high[i] STRICTLY > high[i+j]
            for ALL j in 1..L. Equal values INVALIDATE the swing.
          - Swing Low at i: low[i] STRICTLY < low[i-j] AND low[i] STRICTLY < low[i+j]
            for ALL j in 1..L.
          - Min spacing: index-based. Two swings of the same type closer than
            MIN_SWING_DISTANCE = 2*L+1 candles are resolved by keeping the more extreme.
          - Same-candle SH+SL: if candle i qualifies as both, DISCARD both (anomaly).

        Returns:
            swing_highs: list of dicts {index, price} sorted ascending by index
            swing_lows:  list of dicts {index, price} sorted ascending by index
        """
        highs = df['high'].values.astype(float)
        lows  = df['low'].values.astype(float)
        n = len(df)

        # Spec Section 1.10: minimum index distance between two swings of the same type
        MIN_SWING_DISTANCE = 2 * L + 1

        raw_highs = []
        raw_lows  = []
        # Track candles that are BOTH SH and SL so we can discard them (spec Section 1.16)
        anomaly_indices = set()

        for i in range(L, n - L):   # need L candles on BOTH sides for confirmation
            # ── Swing High: strict > on ALL L left AND L right neighbours ──
            is_sh = all(highs[i] > highs[i - k] for k in range(1, L + 1)) and \
                    all(highs[i] > highs[i + k] for k in range(1, L + 1))

            # ── Swing Low: strict < on ALL L left AND L right neighbours ───
            is_sl = all(lows[i] < lows[i - k] for k in range(1, L + 1)) and \
                    all(lows[i] < lows[i + k] for k in range(1, L + 1))

            # Spec 1.16: same-candle SH+SL is anomalous — discard both
            if is_sh and is_sl:
                anomaly_indices.add(i)
                continue

            if is_sh:
                raw_highs.append({"index": i, "price": highs[i]})
            if is_sl:
                raw_lows.append({"index": i, "price": lows[i]})

        # ── Index-based minimum spacing filter (spec Section 1.10) ─────────
        def _filter_by_index_spacing(swings: list, keep_highest: bool) -> list:
            """Remove swings closer than MIN_SWING_DISTANCE candles; keep more extreme."""
            if len(swings) <= 1:
                return swings
            filtered = [swings[0]]
            for sw in swings[1:]:
                prev = filtered[-1]
                if abs(sw["index"] - prev["index"]) < MIN_SWING_DISTANCE:
                    # Too close — keep the more extreme one
                    if keep_highest:
                        if sw["price"] > prev["price"]:
                            filtered[-1] = sw
                    else:
                        if sw["price"] < prev["price"]:
                            filtered[-1] = sw
                else:
                    filtered.append(sw)
            return filtered

        swing_highs = _filter_by_index_spacing(raw_highs, keep_highest=True)
        swing_lows  = _filter_by_index_spacing(raw_lows,  keep_highest=False)

        return swing_highs, swing_lows

    def _validate_swing_pair(self, df: pd.DataFrame, sh_index: int, sl_index: int, L: int, pair: str) -> bool:
        """
        Validates a swing pair by checking if the 50% equilibrium has been touched.
        If price touches the equilibrium AFTER confirmation, the pair is invalid.
        Gap over equilibrium also counts as a touch.
        """
        sh_price = float(df.iloc[sh_index]['high'])
        sl_price = float(df.iloc[sl_index]['low'])
        equilibrium = (sh_price + sl_price) / 2.0
        
        # Confirmation index is L candles after the latter of the two swings
        confirmation_index = max(sh_index, sl_index) + L
        n = len(df)
        
        # Configurable touch tolerance (e.g. 2 pips)
        touch_tolerance = 2.0 * self._pip_size(pair)
        
        # Check all candles AFTER confirmation
        for k in range(confirmation_index + 1, n):
            candle_low = float(df.iloc[k]['low'])
            candle_high = float(df.iloc[k]['high'])
            
            # 1. Standard touch check (wick touches equilibrium)
            if (candle_low - touch_tolerance) <= equilibrium <= (candle_high + touch_tolerance):
                return False
                
            # 2. Gap check
            if k > confirmation_index + 1:
                prev_high = float(df.iloc[k - 1]['high'])
                prev_low = float(df.iloc[k - 1]['low'])
                
                # Bullish gap over equilibrium
                if prev_high < equilibrium and candle_low > equilibrium:
                    return False
                
                # Bearish gap over equilibrium
                if prev_low > equilibrium and candle_high < equilibrium:
                    return False
                    
        return True

    def _get_dealing_range(
        self,
        df: pd.DataFrame,
        bias: str,
        L: int,
        R: int,
        pair: str,
    ) -> tuple[float, float] | None:
        """
        Build the current dealing range from the latest confirmed OPPOSITE swing pair.

        Bullish bias  → dealing range = most recent swing_low → swing_high
                        (i.e., swing_low must come BEFORE swing_high in time)
        Bearish bias  → dealing range = most recent swing_high → swing_low
                        (i.e., swing_high must come BEFORE swing_low in time)

        Returns (swing_low_price, swing_high_price) or None if no valid pair found.
        """
        swing_highs, swing_lows = self._detect_swings(df, L=L, R=R)

        if not swing_highs or not swing_lows:
            logger.debug(f"[{pair}] _get_dealing_range: Not enough swings detected (H={len(swing_highs)} L={len(swing_lows)}).")
            return None

        if bias == "BULLISH":
            # Need most recent swing_high that has a swing_low BEFORE it
            # Iterate swing highs from most recent backwards
            for sh in reversed(swing_highs):
                # Find the most recent swing low that occurs BEFORE this swing high
                prior_lows = [sl for sl in swing_lows if sl["index"] < sh["index"]]
                if not prior_lows:
                    continue
                sl = prior_lows[-1]  # most recent prior swing low
                # Sanity: swing low must be lower than swing high
                if sl["price"] >= sh["price"]:
                    continue
                
                # 50% Rule Validation
                if not self._validate_swing_pair(df, sh["index"], sl["index"], L, pair):
                    continue
                    
                return (sl["price"], sh["price"])  # (range_low, range_high)
        else:  # BEARISH
            # Need most recent swing_low that has a swing_high BEFORE it
            for sl in reversed(swing_lows):
                prior_highs = [sh for sh in swing_highs if sh["index"] < sl["index"]]
                if not prior_highs:
                    continue
                sh = prior_highs[-1]  # most recent prior swing high
                if sh["price"] <= sl["price"]:
                    continue
                
                # 50% Rule Validation
                if not self._validate_swing_pair(df, sh["index"], sl["index"], L, pair):
                    continue
                    
                return (sl["price"], sh["price"])  # (range_low, range_high)

        logger.debug(f"[{pair}] _get_dealing_range: No valid opposite-swing pair found for bias={bias}.")
        return None

    def _is_ob_in_fib_zone(
        self,
        ob: dict,
        dealing_range: tuple[float, float],
        bias: str,
        pair: str,
    ) -> bool:
        """
        Validates that the OB body_mid sits inside the correct Fibonacci
        premium or discount zone of the current dealing range (wick-to-wick).

        Dealing range = (swing_low_price, swing_high_price).
        Range size D = swing_high - swing_low.

        Fibonacci levels (measured from swing_low upward):
            F382  = swing_low + 0.382 * D
            F50   = swing_low + 0.500 * D   ← equilibrium
            F618  = swing_low + 0.618 * D

        Bullish OB → must be in DISCOUNT (below F50):
            OB body_mid <= F50
            Key sub-zone: F382 to F50

        Bearish OB → must be in PREMIUM (above F50):
            OB body_mid >= F50
            Key sub-zone: F50 to F618
        """
        range_low, range_high = dealing_range

        if range_high <= range_low:
            return False

        D = range_high - range_low
        F382 = range_low + 0.382 * D
        F50  = range_low + 0.500 * D
        F618 = range_low + 0.618 * D

        mid = ob["body_mid"]

        if ob["direction"] == "BUY":
            # Bullish OB must sit in DISCOUNT zone — below the 50% equilibrium
            is_valid = mid <= F50
            if is_valid:
                # Key sub-zone: F382 to F50
                is_sweet = F382 <= mid <= F50
                ob["fib_score_bonus"] = 10 if is_sweet else 5
                zone = "Discount (Key Sub-zone 38.2%-50%)" if is_sweet else "Discount"
                logger.debug(
                    f"[{pair}] Fib ✅ BUY OB: RangeLow={range_low:.5f} RangeHigh={range_high:.5f} "
                    f"F50={F50:.5f} | OB_mid={mid:.5f} in {zone}"
                )
            else:
                ob["fib_score_bonus"] = 0
                logger.debug(
                    f"[{pair}] Fib ❌ BUY OB: OB_mid={mid:.5f} is ABOVE F50={F50:.5f} — in Premium, not Discount."
                )
            return is_valid

        else:  # SELL OB
            # Bearish OB must sit in PREMIUM zone — above the 50% equilibrium
            is_valid = mid >= F50
            if is_valid:
                # Key sub-zone: F50 to F618
                is_sweet = F50 <= mid <= F618
                ob["fib_score_bonus"] = 10 if is_sweet else 5
                zone = "Premium (Key Sub-zone 50%-61.8%)" if is_sweet else "Premium"
                logger.debug(
                    f"[{pair}] Fib ✅ SELL OB: RangeLow={range_low:.5f} RangeHigh={range_high:.5f} "
                    f"F50={F50:.5f} | OB_mid={mid:.5f} in {zone}"
                )
            else:
                ob["fib_score_bonus"] = 0
                logger.debug(
                    f"[{pair}] Fib ❌ SELL OB: OB_mid={mid:.5f} is BELOW F50={F50:.5f} — in Discount, not Premium."
                )
            return is_valid

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
            
        # Filter out Saturday (5) and Sunday (6) candles which skew True Average
        d1_filtered = d1[~d1['time'].dt.weekday.isin([5, 6])].copy()
        
        # Holiday Filter: Drop days where the range is abnormally small (e.g. < 25% of median)
        if len(d1_filtered) > 2:
            ranges = d1_filtered['high'] - d1_filtered['low']
            median_range = ranges.median()
            d1_filtered = d1_filtered[ranges > (0.25 * median_range)]
        
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
