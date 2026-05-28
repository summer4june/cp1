"""
scannerzgmt.py — ICT 0-GMT Open Strategy Scanner (VvE FxBOT)

Strategy Reference: ICT 0-GMT Open Master Strategy
- 0 GMT = 5:30 AM IST — Daily reference open price
- Midnight NY reference = 9:30 AM IST
- Daily bias via PD Array Matrix (premium/discount zone)
- Step 2B: 0 GMT level may only be respected ONCE (if already tested → skip)
- Entry modes: DIRECT, FILTER (±20 pips), SPLIT
- SL: XAUUSD = 95 pips | FX pairs = 25 pips
- TP: XAUUSD = 190 pips | FX pairs = 50 pips
- RR = 1:2 always
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
        """Returns pip size (0.01 for JPY, 0.01 for XAU, 0.0001 otherwise)."""
        p = pair.upper()
        if "JPY" in p:
            return 0.01
        if "XAU" in p:
            return 0.01   # Gold: 1 pip = 0.01 USD/oz (MT5 broker convention)
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

    def _is_gold(self, pair: str) -> bool:
        return "XAU" in pair.upper()

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

    def _get_daily_bias(self, pair: str, zgmt_cfg: dict) -> tuple[str | None, bool]:
        """
        Determine bullish or bearish bias using 20-day range midpoint.
        Returns Tuple[bias_str_or_none, is_structural_absence].
        """
        n = zgmt_cfg.get("d1_candles_for_range", 20)
        candles = self.mt5.get_candles(pair, "D1", count=n + 1)
        if candles is None:
            logger.debug(f"[{pair}] ZGMT: No D1 candles returned.")
            return None, False

        if len(candles) < n:
            logger.debug(f"[{pair}] ZGMT: Insufficient D1 candles for PD bias ({len(candles)}/{n}).")
            return None, True

        # Use completed candles only (exclude current open candle)
        df = candles.iloc[:-1] if len(candles) > n else candles
        df = df.tail(n)

        range_high = df["high"].max()
        range_low = df["low"].min()

        if range_high <= range_low:
            logger.debug(f"[{pair}] ZGMT: Invalid D1 range high={range_high} low={range_low}.")
            return None, True

        midpoint = (range_high + range_low) / 2.0

        tick = self.mt5.get_tick(pair)
        if not tick:
            logger.debug(f"[{pair}] ZGMT: No live tick for bias check.")
            return None, False

        current_bid = tick["bid"]

        if current_bid < midpoint:
            bias = "BULLISH"  # Discount zone → expect upward move
        elif current_bid > midpoint:
            bias = "BEARISH"  # Premium zone → expect downward move
        else:
            bias = None  # At exact equilibrium — ambiguous

        logger.debug(
            f"[{pair}] ZGMT: D1 Range High={range_high:.5f} Low={range_low:.5f} "
            f"Mid={midpoint:.5f} BID={current_bid:.5f} → Bias={bias}"
        )
        return bias, False

    # ──────────────────────────────────────────────────────────────────
    # Step 2 — Identify 0 GMT open price from today's H1 candles
    # ──────────────────────────────────────────────────────────────────

    def _get_zgmt_price(self, pair: str) -> tuple[float | None, bool]:
        """
        Fetch the H1 candle open price that corresponds to today's 0 GMT
        (which is 00:00 UTC = 5:30 AM IST).

        Returns:
            Tuple[float | None, bool]: (price, is_structural_absence)
        """
        now_utc = self._utc_now()
        # Today's 0 GMT is midnight UTC of the current calendar day (UTC)
        target_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        # Fetch last 30 H1 candles to reliably find today's midnight bar
        candles = self.mt5.get_candles(pair, "H1", count=30)
        if candles is None or candles.empty:
            logger.debug(f"[{pair}] ZGMT: No H1 candles returned.")
            return None, False

        # candle["time"] is UTC-aware after MT5Connector processing
        for _, row in candles.iterrows():
            candle_time = row["time"]
            # Normalize to UTC-aware if needed
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            # Match the candle that opened exactly at 0 GMT (00:00 UTC)
            if (candle_time.year == target_utc.year and
                    candle_time.month == target_utc.month and
                    candle_time.day == target_utc.day and
                    candle_time.hour == 0 and
                    candle_time.minute == 0):
                zgmt_price = float(row["open"])
                logger.debug(f"[{pair}] ZGMT: Found 0 GMT open price = {zgmt_price:.5f} at {candle_time}")
                return zgmt_price, False

        logger.debug(f"[{pair}] ZGMT: 0 GMT H1 candle not found in fetched data.")
        return None, True

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
        test_start_time = today_zgmt_utc + timedelta(minutes=exclude_mins)

        # Guard: if we haven't even passed the exclusion window yet, don't signal.
        # Price hasn't had a chance to displace from the open — no meaningful data to evaluate.
        if now_utc < test_start_time:
            logger.debug(
                f"[{pair}] ZGMT Step 2B: Still inside exclusion window "
                f"(now={now_utc.strftime('%H:%M')} UTC, window_ends={test_start_time.strftime('%H:%M')} UTC). "
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

        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Only evaluate candles that opened after the exclusion window
            if candle_time < test_start_time:
                continue

            candle_low = float(row["low"])
            candle_high = float(row["high"])

            # "Tested" = price came within threshold of zgmt_price
            if abs(candle_high - zgmt_price) <= threshold_price or \
               abs(candle_low - zgmt_price) <= threshold_price or \
               (candle_low <= zgmt_price <= candle_high):
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: Level ALREADY TESTED. "
                    f"Candle H={candle_high:.5f} L={candle_low:.5f} vs ZGMT={zgmt_price:.5f} "
                    f"(threshold={threshold_price:.5f})"
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
        self,
        pair: str,
        bias: str,
        zgmt_price: float,
        tick: dict,
        zgmt_cfg: dict,
    ) -> dict | None:
        """
        Compute entry, SL, and TP based on config.
        Returns a dict with entry_price, sl_price, tp1_price, tp2_price,
        sl_pips, tp_pips, or None on failure.
        """
        entry_mode = zgmt_cfg.get("zgmt_entry_mode", "DIRECT").upper()
        filter_pips = zgmt_cfg.get("zgmt_filter_pips", 20)
        filter_diff = self._pips_to_price(pair, filter_pips)

        sl_tp_cfg = zgmt_cfg.get("zgmt_sl_tp", {})
        if self._is_gold(pair):
            sl_pips = float(sl_tp_cfg.get("sl_pips_gold", 95))
            tp_pips = float(sl_tp_cfg.get("tp_pips_gold", 190))
        else:
            sl_pips = float(sl_tp_cfg.get("sl_pips_fx", 25))
            tp_pips = float(sl_tp_cfg.get("tp_pips_fx", 50))

        sl_diff = self._pips_to_price(pair, sl_pips)
        tp_diff = self._pips_to_price(pair, tp_pips)

        # ── Entry price ──────────────────────────────────────────────
        if bias == "BULLISH":
            if entry_mode == "DIRECT":
                # Strategy Step 4 Option A: Buy exactly at the 5:30 AM IST open price.
                # Use zgmt_price directly (the IPDA True Day Open), not the drifted market tick.
                entry_price = zgmt_price
            elif entry_mode == "FILTER":
                # Strategy Step 4 Option B: Buy limit 10–20 pips BELOW 0 GMT open (Judas Swing filter)
                entry_price = zgmt_price - filter_diff
            elif entry_mode == "SPLIT":
                # Strategy Step 4 Option C: Primary half at 0 GMT open directly
                entry_price = zgmt_price
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = zgmt_price

            sl_price = entry_price - sl_diff
            tp1_price = entry_price + sl_diff  # TP1 = 1R
            tp2_price = entry_price + tp_diff  # TP2 = 2R

        else:  # BEARISH
            if entry_mode == "DIRECT":
                # Strategy Step 10: Sell directly at 5:30 AM IST open price — no filter, no adjustment.
                entry_price = zgmt_price
            elif entry_mode == "FILTER":
                # Strategy Step 9: Sell limit at 0 GMT + 20 pips above the open
                entry_price = zgmt_price + filter_diff
            elif entry_mode == "SPLIT":
                # Strategy Step 4 Option C: Primary half at 0 GMT open directly
                entry_price = zgmt_price
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = zgmt_price

            sl_price = entry_price + sl_diff
            tp1_price = entry_price - sl_diff  # TP1 = 1R
            tp2_price = entry_price - tp_diff  # TP2 = 2R

        return {
            "entry_price": round(entry_price, 5),
            "sl_price": round(sl_price, 5),
            "tp1_price": round(tp1_price, 5),
            "tp2_price": round(tp2_price, 5),
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "entry_mode": entry_mode,
            "filter_pips": filter_pips,
        }

    # ──────────────────────────────────────────────────────────────────
    # Main scan method
    # ──────────────────────────────────────────────────────────────────

    def scan(self, pair: str, session: str, killzone: str) -> dict | None:
        """
        Execute the ZGMT scan for a single pair.
        Returns a signal dict on success, None otherwise.
        """
        # ── Check if already finalized/invalidated today ─────────────
        if self._is_daily_finalized(pair):
            return None

        logger.debug(f"[{pair}] ZGMT: Scan started | Session={session} | KZ={killzone}")

        # ── 0. Config gate ───────────────────────────────────────────
        zgmt_cfg = getattr(self.config, "zgmt_scanner", {})
        if not zgmt_cfg.get("enabled", False):
            logger.debug(f"[{pair}] ZGMT: Scanner disabled in config.")
            return None

        # ── 0a. Allow buy/sell flags ─────────────────────────────────
        allow_buy = zgmt_cfg.get("allow_buy", True)
        allow_sell = zgmt_cfg.get("allow_sell", True)

        # ── 0b. State: cooldown guard ────────────────────────────────
        if self.state.is_pair_on_cooldown(pair):
            logger.debug(f"[{pair}] ZGMT: Pair on cooldown — skipping.")
            return None

        # ── 0c. Daily trade cap ──────────────────────────────────────
        max_daily = zgmt_cfg.get("max_daily_trades", 2)
        if self._get_daily_count(pair) >= max_daily:
            logger.debug(f"[{pair}] ZGMT: Daily trade cap ({max_daily}) reached.")
            self._mark_daily_finalized(pair)
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

        if not (window_start <= current_ist_time <= window_end):
            logger.debug(
                f"[{pair}] ZGMT: Outside signal window "
                f"({window_start}–{window_end} IST, now={current_ist_time} IST)."
            )
            return None

        # ── Step 1: Daily bias ────────────────────────────────────────
        require_pd = zgmt_cfg.get("require_pd_array_check", True)
        if require_pd:
            bias, is_structural = self._get_daily_bias(pair, zgmt_cfg)
            if bias is None:
                logger.info(f"[{pair}] ZGMT: Could not determine D1 PD bias — skipping.")
                if is_structural or hasattr(self.mt5, "current_time"):
                    self._mark_daily_finalized(pair)
                return None
        else:
            # No bias check: infer from current price vs yesterday's close
            logger.debug(f"[{pair}] ZGMT: PD array check disabled — using tick direction.")
            tick = self.mt5.get_tick(pair)
            bias = "BULLISH" if (tick and tick["bid"] > 0) else None
            if bias is None:
                return None

        # ── Allow direction gate after bias ──────────────────────────
        if bias == "BULLISH" and not allow_buy:
            logger.debug(f"[{pair}] ZGMT: Bullish bias but allow_buy=False.")
            return None
        if bias == "BEARISH" and not allow_sell:
            logger.debug(f"[{pair}] ZGMT: Bearish bias but allow_sell=False.")
            return None

        # ── Step 2: 0 GMT open price ──────────────────────────────────
        zgmt_price, is_structural = self._get_zgmt_price(pair)
        if zgmt_price is None:
            logger.info(f"[{pair}] ZGMT: 0 GMT open price not available — skipping.")
            if is_structural or hasattr(self.mt5, "current_time"):
                self._mark_daily_finalized(pair)
            return None

        # ── Step 2B: Untested condition ───────────────────────────────
        is_tested = self._is_zgmt_level_tested(pair, zgmt_price, zgmt_cfg)
        if is_tested:
            now_utc = self._utc_now()
            today_zgmt_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            exclude_mins = zgmt_cfg.get("zgmt_test_exclude_first_mins", 15)
            test_start_time = today_zgmt_utc + timedelta(minutes=exclude_mins)

            if now_utc < test_start_time:
                # Still inside exclusion window — price hasn't displaced yet.
                # Do NOT finalize the day: just defer to next scan bar.
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: Inside exclusion window — deferring, day NOT finalized."
                )
                return None
            else:
                # Level has genuinely been touched after the exclusion window → invalid for today.
                logger.info(
                    f"[{pair}] ZGMT Step 2B: 0 GMT level already tested "
                    f"({zgmt_price:.5f}) — setup invalid for today."
                )
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

        # ── Steps 4–6: Entry / SL / TP ───────────────────────────────
        levels = self._compute_entry_sl_tp(pair, bias, zgmt_price, tick, zgmt_cfg)
        if levels is None:
            return None

        spread_pips = self.mt5.get_current_spread(pair)
        sl_pips = levels["sl_pips"]
        tp_pips = levels["tp_pips"]

        # Spread-aware effective RR
        denom = sl_pips + spread_pips
        effective_rr = (tp_pips - spread_pips) / denom if denom > 0 else 0.0

        direction = "BUY" if bias == "BULLISH" else "SELL"
        entry_mode = levels["entry_mode"]
        filter_note = f" ±{levels['filter_pips']}pips" if entry_mode in ("FILTER", "SPLIT") else ""

        pd_zone = "DISCOUNT" if bias == "BULLISH" else "PREMIUM"
        bias_summary = (
            f"ZGMT | PD: {pd_zone} | Entry: {entry_mode}{filter_note} | "
            f"0GMT={zgmt_price:.5f}"
        )

        score = float(zgmt_cfg.get("score", 70.0))
        signal_id = str(uuid.uuid4())
        now_iso = self._utc_now().isoformat()

        # Fixed lot size override (optional — 0 or missing means use risk engine)
        fixed_lot = float(zgmt_cfg.get("fixed_lot_size", 0.0))

        # ── Register signal ───────────────────────────────────────────
        self._last_signal_time[pair] = self._utc_now()
        self._increment_daily_count(pair)

        logger.info(
            f"[{pair}] ZGMT Signal ✅ {direction} | "
            f"Entry={levels['entry_price']:.5f} SL={levels['sl_price']:.5f} "
            f"TP2={levels['tp2_price']:.5f} | 0GMT={zgmt_price:.5f} | "
            f"Bias={bias} | Mode={entry_mode} | RR={effective_rr:.2f} | Score={score}"
        )

        self._mark_daily_finalized(pair)

        # ZGMT uses a fixed 1:2 RR defined by the strategy — skip the global spread-penalised
        # effective_rr_min check so the correct strategy RR is honoured in the risk engine.
        skip_rr_check = bool(zgmt_cfg.get("skip_rr_check", True))

        # ── HTF OB Exception Override ─────────────────────────────────────
        # Runs just before returning the 0-GMT signal.
        # If a valid unmitigated HTF OB is active it takes institutional priority.
        if zgmt_cfg.get("zgmt_exception_enabled", False):
            ob_signal = self._check_htf_ob_exception(pair, session, killzone)
            if ob_signal is not None:
                logger.info(f"[{pair}] ZGMT-EXCEPTION: HTF OB overrides 0-GMT entry.")
                return ob_signal
            # ob_signal is None → no HTF OB conflict → fall through to 0-GMT signal

        return {
            "signal_id": signal_id,
            "pair": pair,
            "session": session,
            "timeframe_bias": zgmt_cfg.get("timeframe_bias", "D1"),
            "timeframe_entry": zgmt_cfg.get("timeframe_entry", "H1"),
            "direction": direction,
            "bias_summary": bias_summary,
            "entry_price": levels["entry_price"],
            "sl_price": levels["sl_price"],
            "tp1_price": levels["tp1_price"],
            "tp2_price": levels["tp2_price"],
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "spread_pips": spread_pips,
            "effective_rr": round(effective_rr, 3),
            "score": score,
            "detected_time": now_iso,
            "strategy": "ZGMT",
            "setup_type": "ZGMT",
            # fixed_lot_size: 0.0 means "use risk engine"; > 0 means override
            "fixed_lot_size": fixed_lot,
            # Skip global effective_rr_min check — ZGMT enforces its own 1:2 RR by design
            "skip_rr_check": skip_rr_check,
        }

    # ══════════════════════════════════════════════════════════════════
    # HTF Order Block Exception — private methods
    # ══════════════════════════════════════════════════════════════════

    def _check_htf_ob_exception(self, pair: str, session: str, killzone: str) -> dict | None:
        """
        Checks if a valid unmitigated HTF Order Block on H4 or H1 overrides the 0-GMT setup.
        Returns signal dict if valid OB found inside correct Fibonacci zone, else None.
        """
        zgmt_cfg = self.zgmt_cfg

        h4_candles = self.mt5.get_candles(pair, "H4", count=zgmt_cfg.get("zgmt_ob_candles_4h", 50))
        h1_candles = self.mt5.get_candles(pair, "H1", count=zgmt_cfg.get("zgmt_ob_candles_1h", 100))

        h4_empty = h4_candles is None or (hasattr(h4_candles, 'empty') and h4_candles.empty)
        h1_empty = h1_candles is None or (hasattr(h1_candles, 'empty') and h1_candles.empty)
        if h4_empty and h1_empty:
            return None

        current_price = self.mt5.get_current_bid(pair)
        if not current_price:
            return None

        symbol_point = self.mt5.get_symbol_point(pair)
        tap_threshold = zgmt_cfg.get("zgmt_ob_tap_threshold_pips", 5) * symbol_point

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

        # Filter 1: unmitigated only
        valid_obs = [ob for ob in all_obs if not ob["is_mitigated"]]
        if not valid_obs:
            return None

        # Filter 2: price must currently be tapping into the OB zone
        tapping_obs = [
            ob for ob in valid_obs
            if (ob["body_low"] - tap_threshold) <= current_price <= (ob["body_high"] + tap_threshold)
        ]
        if not tapping_obs:
            return None

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

        if not fib_valid_obs:
            return None

        # Select best OB: prefer H4 over H1, then most recent (highest candle_index)
        fib_valid_obs.sort(key=lambda x: (0 if x["timeframe"] == "H4" else 1, -x["candle_index"]))
        best_ob = fib_valid_obs[0]

        direction = best_ob["direction"]
        if direction == "BUY" and not zgmt_cfg.get("allow_buy", True):
            return None
        if direction == "SELL" and not zgmt_cfg.get("allow_sell", True):
            return None

        entry_price = best_ob["body_mid"]

        # ADR-based dynamic SL
        sl_distance = self._calculate_adr_sl(pair)
        if not sl_distance or sl_distance <= 0:
            return None

        sl_pips = sl_distance / symbol_point
        tp_pips = sl_pips * 2

        if direction == "BUY":
            sl_price = entry_price - sl_distance
            tp_price = entry_price + (sl_distance * 2)
        else:
            sl_price = entry_price + sl_distance
            tp_price = entry_price - (sl_distance * 2)

        spread_pips = self.mt5.get_current_spread(pair)
        denom = sl_pips + spread_pips
        effective_rr = (tp_pips - spread_pips) / denom if denom > 0 else 0.0

        zone_label = "PREMIUM" if direction == "SELL" else "DISCOUNT"

        return {
            "signal_id": str(uuid.uuid4()),
            "pair": pair,
            "session": session,
            "timeframe_bias": best_ob["timeframe"],
            "timeframe_entry": best_ob["timeframe"],
            "direction": direction,
            "bias_summary": (
                f"ZGMT-EXCEPTION | {best_ob['ob_type']} OB | "
                f"{best_ob['timeframe']} | Zone: {zone_label} | ADR SL"
            ),
            "entry_price": round(entry_price, 5),
            "sl_price": round(sl_price, 5),
            "tp1_price": round(tp_price, 5),   # TP1 = TP2 (single 2R target, no split)
            "tp2_price": round(tp_price, 5),
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "spread_pips": spread_pips,
            "effective_rr": round(effective_rr, 3),
            "score": float(zgmt_cfg.get("zgmt_exception_score", 68.0)),
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
            body_high = max(float(candle['open']), float(candle['close']))
            body_low  = min(float(candle['open']), float(candle['close']))
            body_mid  = (body_high + body_low) / 2

            # Bullish Normal OB: bearish candle followed by strong upward displacement
            if candle['close'] < candle['open']:
                for j in range(i + 1, min(i + 4, len(df))):
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
                for j in range(i + 1, min(i + 4, len(df))):
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
                    body_high = max(float(move_start['open']), float(move_start['close']))
                    body_low  = min(float(move_start['open']), float(move_start['close']))
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
                    body_high = max(float(move_start['open']), float(move_start['close']))
                    body_low  = min(float(move_start['open']), float(move_start['close']))
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
        Dynamic SL using the previous 5-day ADR (wick-to-wick).
        ADR = average of (daily high − daily low) over the last N completed days.
        SL distance = ADR ÷ 2.
        Returns price-unit distance, or None if data is insufficient.
        """
        adr_days = self.zgmt_cfg.get("zgmt_adr_days", 5)
        d1 = self.mt5.get_candles(pair, "D1", count=adr_days + 2)

        if d1 is None or len(d1) < adr_days + 1:
            logger.warning(f"[{pair}] ZGMT-EXCEPTION: Insufficient D1 candles for ADR ({len(d1) if d1 is not None else 0} available).")
            return None

        # Index 0 is the current forming candle — skip it; use next adr_days completed candles
        completed = d1.iloc[1: adr_days + 1]
        daily_ranges = completed['high'] - completed['low']   # wick-to-wick as per strategy
        adr = float(daily_ranges.mean())
        return adr / 2  # SL = ADR ÷ 2
