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

        # Calculate minutes elapsed since 0 GMT today
        elapsed_minutes = int((now_utc - today_zgmt_utc).total_seconds() / 60)
        # Fetch enough M1 candles to cover the entire day from 0 GMT to now
        fetch_count = max(60, elapsed_minutes + 15)

        candles = self.mt5.get_candles(pair, "M1", count=fetch_count)
        if candles is None or candles.empty:
            logger.debug(f"[{pair}] ZGMT: No M1 candles for Step 2B test. Assuming untested.")
            return False

        exclude_mins = zgmt_cfg.get("zgmt_test_exclude_first_mins", 15)
        test_start_time = today_zgmt_utc + timedelta(minutes=exclude_mins)

        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Skip candles before the test start time to ignore initial open consolidation
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
                entry_price = tick["ask"]  # Buy at ask
            elif entry_mode == "FILTER":
                # Buy limit 20 pips BELOW 0 GMT open → more favourable fill
                entry_price = zgmt_price - filter_diff
            elif entry_mode == "SPLIT":
                # Primary half at direct ask; secondary noted in bias_summary
                entry_price = tick["ask"]
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = tick["ask"]

            sl_price = entry_price - sl_diff
            tp1_price = entry_price + sl_diff  # TP1 = 1R
            tp2_price = entry_price + tp_diff  # TP2 = 2R

        else:  # BEARISH
            if entry_mode == "DIRECT":
                entry_price = tick["bid"]  # Sell at bid
            elif entry_mode == "FILTER":
                # Sell limit 20 pips ABOVE 0 GMT open
                entry_price = zgmt_price + filter_diff
            elif entry_mode == "SPLIT":
                entry_price = tick["bid"]
            else:
                logger.warning(f"[{pair}] ZGMT: Unknown entry_mode '{entry_mode}'. Defaulting to DIRECT.")
                entry_price = tick["bid"]

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
        if self._is_zgmt_level_tested(pair, zgmt_price, zgmt_cfg):
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
        }
