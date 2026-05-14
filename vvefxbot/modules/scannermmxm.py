import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine

logger = get_logger("ScannerMMXM")


class ScannerMMXM:
    """ICT MMXM signal scanner implementing the 5-step detection sequence."""

    def __init__(self, config: Config, mt5connector: MT5Connector, state_engine: StateEngine):
        """
        Initializes the ScannerMMXM.

        Args:
            config (Config): Validated bot configuration.
            mt5connector (MT5Connector): Live MT5 connection.
            state_engine (StateEngine): Persistence engine for cooldowns and state.
        """
        self.config = config
        self.mt5 = mt5connector
        self.state = state_engine

    # ------------------------------------------------------------------
    # PIP VALUE HELPERS
    # ------------------------------------------------------------------

    def _pip_size(self, pair: str) -> float:
        """
        Returns the pip size (price per pip) for a given pair.

        Args:
            pair (str): Trading symbol.

        Returns:
            float: Pip size (0.01 for JPY pairs, 0.0001 for others).
        """
        return 0.01 if "JPY" in pair.upper() else 0.0001

    def _price_to_pips(self, price_diff: float, pair: str) -> float:
        """
        Converts a price difference to pips.

        Args:
            price_diff (float): Absolute price distance.
            pair (str): Trading symbol.

        Returns:
            float: Distance in pips.
        """
        return abs(price_diff) / self._pip_size(pair)

    def _pips_to_price(self, pips: float, pair: str) -> float:
        """
        Converts pips to a price distance.

        Args:
            pips (float): Number of pips.
            pair (str): Trading symbol.

        Returns:
            float: Price distance.
        """
        return pips * self._pip_size(pair)

    # ------------------------------------------------------------------
    # STEP 1 — BIAS (M15)
    # ------------------------------------------------------------------

    def _get_bias(self, m15_candles: pd.DataFrame) -> Optional[str]:
        """
        Determine directional bias from the last 20 M15 candles using 50% level.

        Args:
            m15_candles (pd.DataFrame): M15 OHLCV data.

        Returns:
            str | None: "BUY", "SELL", or None if undetermined.
        """
        if len(m15_candles) < 20:
            return None

        last_20 = m15_candles.iloc[-20:]
        range_high = last_20["high"].max()
        range_low = last_20["low"].min()
        midpoint = (range_high + range_low) / 2.0
        current_price = m15_candles.iloc[-1]["close"]

        if current_price < midpoint:
            return "BUY"
        elif current_price > midpoint:
            return "SELL"
        return None

    # ------------------------------------------------------------------
    # STEP 2 — LIQUIDITY SWEEP (M15)
    # ------------------------------------------------------------------

    def detect_liquidity_sweep(self, m15_candles: pd.DataFrame, direction: str) -> Dict[str, Any]:
        """
        Detect a liquidity sweep on M15 candles.

        A BUY sweep: one of the last 3 candles wicks below the 10-candle low and
        closes back above it (false break). A SELL sweep: wicks above the 10-candle
        high and closes back below it.

        Args:
            m15_candles (pd.DataFrame): M15 OHLCV data.
            direction (str): "BUY" or "SELL".

        Returns:
            dict: {"detected": bool, "sweep_price": float, "sweep_candle_index": int}
        """
        null_result = {"detected": False, "sweep_price": 0.0, "sweep_candle_index": -1}

        if len(m15_candles) < 13:
            return null_result

        # Reference level from prior 10 candles (exclude last 3)
        reference = m15_candles.iloc[-13:-3]
        last_3 = m15_candles.iloc[-3:]

        if direction == "BUY":
            sweep_level = reference["low"].min()
            for idx, (_, candle) in enumerate(last_3.iterrows()):
                # Wick extends below sweep level but candle closes above it
                if candle["low"] < sweep_level and candle["close"] > sweep_level:
                    return {
                        "detected": True,
                        "sweep_price": sweep_level,
                        "sweep_candle_index": int(len(m15_candles) - 3 + idx)
                    }

        elif direction == "SELL":
            sweep_level = reference["high"].max()
            for idx, (_, candle) in enumerate(last_3.iterrows()):
                # Wick extends above sweep level but candle closes below it
                if candle["high"] > sweep_level and candle["close"] < sweep_level:
                    return {
                        "detected": True,
                        "sweep_price": sweep_level,
                        "sweep_candle_index": int(len(m15_candles) - 3 + idx)
                    }

        return null_result

    # ------------------------------------------------------------------
    # STEP 3 — MSS / DISPLACEMENT (M1)
    # ------------------------------------------------------------------

    def detect_mss_displacement(
        self, m1_candles: pd.DataFrame, direction: str, atr: float
    ) -> Dict[str, Any]:
        """
        Detect a market structure shift / displacement candle on M1.

        Displacement = candle body > 1.5 × ATR(14) breaking the last M15 swing.
        Uses the last 15 M1 candles to find the swing and the displacement candle.

        Args:
            m1_candles (pd.DataFrame): M1 OHLCV data.
            direction (str): "BUY" or "SELL".
            atr (float): ATR(14) of M1 candles.

        Returns:
            dict: {"detected": bool, "displacement_candle_index": int}
        """
        null_result = {"detected": False, "displacement_candle_index": -1}

        if len(m1_candles) < 16 or atr <= 0:
            return null_result

        # Swing reference from prior 15 candles (excluding last candle)
        swing_window = m1_candles.iloc[-16:-1]
        last_candle = m1_candles.iloc[-1]
        body_size = abs(last_candle["close"] - last_candle["open"])

        if body_size < 1.5 * atr:
            return null_result

        if direction == "BUY":
            swing_high = swing_window["high"].max()
            # Strong bullish displacement breaking above swing high
            if last_candle["close"] > swing_high and last_candle["close"] > last_candle["open"]:
                return {"detected": True, "displacement_candle_index": int(len(m1_candles) - 1)}

        elif direction == "SELL":
            swing_low = swing_window["low"].min()
            # Strong bearish displacement breaking below swing low
            if last_candle["close"] < swing_low and last_candle["close"] < last_candle["open"]:
                return {"detected": True, "displacement_candle_index": int(len(m1_candles) - 1)}

        return null_result

    # ------------------------------------------------------------------
    # STEP 4 — FVG DETECTION (M1)
    # ------------------------------------------------------------------

    def detect_fvg(
        self, m1_candles: pd.DataFrame, direction: str, displacement_idx: int
    ) -> Dict[str, Any]:
        """
        Detect an unfilled Fair Value Gap on M1 candles around the displacement candle.

        BUY FVG:  candle[i-2].high < candle[i].low
        SELL FVG: candle[i-2].low  > candle[i].high

        Checks the displacement candle and the candle immediately after it.

        Args:
            m1_candles (pd.DataFrame): M1 OHLCV data.
            direction (str): "BUY" or "SELL".
            displacement_idx (int): Index in m1_candles of the displacement candle.

        Returns:
            dict: {"detected": bool, "fvg_high": float, "fvg_low": float}
        """
        null_result = {"detected": False, "fvg_high": 0.0, "fvg_low": 0.0}

        # We need at least candle[i-2], candle[i-1] (displacement), candle[i]
        # Check at displacement_idx (i-1 in the trio) and the candle after it
        for i in [displacement_idx, displacement_idx + 1]:
            if i < 2 or i >= len(m1_candles):
                continue

            c_prev2 = m1_candles.iloc[i - 2]
            c_curr = m1_candles.iloc[i]

            if direction == "BUY":
                fvg_low = c_prev2["high"]
                fvg_high = c_curr["low"]
                if fvg_high > fvg_low:
                    # Check the gap is not already filled by candle[i-1]
                    c_mid = m1_candles.iloc[i - 1]
                    if c_mid["low"] > fvg_low:  # gap still open
                        return {"detected": True, "fvg_high": fvg_high, "fvg_low": fvg_low}

            elif direction == "SELL":
                fvg_high = c_prev2["low"]
                fvg_low = c_curr["high"]
                if fvg_high > fvg_low:
                    c_mid = m1_candles.iloc[i - 1]
                    if c_mid["high"] < fvg_high:  # gap still open
                        return {"detected": True, "fvg_high": fvg_high, "fvg_low": fvg_low}

        return null_result

    # ------------------------------------------------------------------
    # STEP 5 — CONFLUENCE CHECK (H1)
    # ------------------------------------------------------------------

    def _check_h1_confluence(self, pair: str, direction: str) -> int:
        """
        Count confluence points from H1 structure alignment.

        Checks whether the last H1 swing direction agrees with the bias.
        Returns 0 or 1 (used to scale confluence score).

        Args:
            pair (str): Trading symbol.
            direction (str): "BUY" or "SELL".

        Returns:
            int: Number of confluences (0 or 1).
        """
        h1_candles = self.mt5.get_candles(pair, "H1", count=20)
        if h1_candles.empty or len(h1_candles) < 5:
            return 0

        last_5 = h1_candles.iloc[-5:]
        h1_trend_up = last_5["close"].iloc[-1] > last_5["close"].iloc[0]

        if direction == "BUY" and h1_trend_up:
            return 1
        elif direction == "SELL" and not h1_trend_up:
            return 1
        return 0

    # ------------------------------------------------------------------
    # SCORING
    # ------------------------------------------------------------------

    def calculate_score(
        self,
        liquidity: bool,
        displacement: bool,
        fvg: bool,
        confluence_count: int
    ) -> float:
        """
        Calculate the total signal score from its 4 components.

        Each component is worth 25 points. Confluence score scales linearly
        with confluence_count capped at 1 (25 points).

        Args:
            liquidity (bool): Liquidity sweep detected.
            displacement (bool): MSS/displacement detected.
            fvg (bool): FVG detected.
            confluence_count (int): Number of confluence factors (0 or 1).

        Returns:
            float: Total score in range 0–100.
        """
        score = 0.0
        if liquidity:
            score += 25.0
        if displacement:
            score += 25.0
        if fvg:
            score += 25.0
        # Confluence component: 25 points if at least 1 confluence factor
        score += min(confluence_count, 1) * 25.0
        return score

    # ------------------------------------------------------------------
    # SIGNAL BUILDER
    # ------------------------------------------------------------------

    def build_signal(
        self,
        pair: str,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        score: float,
        spread: float,
        session: str,
        fvg_zone: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Construct the complete signal dictionary matching signals_detected schema.

        Args:
            pair (str): Trading symbol.
            direction (str): "BUY" or "SELL".
            entry (float): Entry price.
            sl (float): Stop loss price.
            tp1 (float): Take profit 1 price.
            tp2 (float): Take profit 2 price.
            score (float): Signal quality score.
            spread (float): Current spread in pips.
            session (str): Active session name.
            fvg_zone (dict): FVG high/low prices.

        Returns:
            dict: Complete signal dictionary.
        """
        sl_pips = self._price_to_pips(abs(entry - sl), pair)
        tp_pips = self._price_to_pips(abs(tp2 - entry), pair)

        effective_rr = 0.0
        denominator = sl_pips + spread
        if denominator > 0:
            effective_rr = (tp_pips - spread) / denominator

        bias_summary = (
            f"{direction} | FVG [{fvg_zone.get('fvg_low', 0):.5f} – "
            f"{fvg_zone.get('fvg_high', 0):.5f}] | Score: {score}"
        )

        return {
            "signal_id": str(uuid.uuid4()),
            "pair": pair,
            "session": session,
            "timeframe_bias": "M15",
            "timeframe_entry": "M1",
            "direction": direction,
            "bias_summary": bias_summary,
            "entry_price": round(entry, 5),
            "sl_price": round(sl, 5),
            "tp1_price": round(tp1, 5),
            "tp2_price": round(tp2, 5),
            "sl_pips": round(sl_pips, 2),
            "tp_pips": round(tp_pips, 2),
            "spread_pips": round(spread, 2),
            "effective_rr": round(effective_rr, 4),
            "score": score,
            "detected_time": datetime.now(timezone.utc).isoformat()
        }

    # ------------------------------------------------------------------
    # MAIN SCAN
    # ------------------------------------------------------------------

    def scan(self, pair: str, session: str, killzone: str) -> Optional[Dict[str, Any]]:
        """
        Run the full 5-step ICT MMXM detection sequence for a pair.

        Returns a signal dict if score >= self.config.aplus_threshold, otherwise None.

        Args:
            pair (str): Trading symbol.
            session (str): Active session name.
            killzone (str): Active killzone name.

        Returns:
            dict | None: Signal dict or None if no valid A+ setup found.
        """
        # Guard: must be in a killzone
        if not killzone:
            return None

        # Guard: check pair cooldown
        if self.state.is_pair_on_cooldown(pair):
            logger.info(f"[{pair}] Skipped — pair is on cooldown.")
            return None

        # Fetch candle data
        m15_candles = self.mt5.get_candles(pair, "M15", count=100)
        m1_candles = self.mt5.get_candles(pair, "M1", count=100)

        if m15_candles.empty or m1_candles.empty:
            logger.warning(f"[{pair}] Candle data unavailable. Skipping scan.")
            return None

        if len(m15_candles) < 20 or len(m1_candles) < 16:
            logger.warning(f"[{pair}] Insufficient candle data. Skipping scan.")
            return None

        # STEP 1 — BIAS
        direction = self._get_bias(m15_candles)
        if direction is None:
            logger.debug(f"[{pair}] No clear bias. Skipping.")
            return None

        # STEP 2 — LIQUIDITY SWEEP (M15)
        sweep_result = self.detect_liquidity_sweep(m15_candles, direction)
        if not sweep_result["detected"]:
            logger.debug(f"[{pair}] No liquidity sweep detected. Skipping.")
            return None

        # STEP 3 — MSS / DISPLACEMENT (M1)
        atr_series = (m1_candles["high"] - m1_candles["low"]).abs().iloc[-14:]
        atr = float(atr_series.mean()) if len(atr_series) == 14 else 0.0

        mss_result = self.detect_mss_displacement(m1_candles, direction, atr)
        if not mss_result["detected"]:
            logger.debug(f"[{pair}] No MSS/displacement detected. Skipping.")
            return None

        # STEP 4 — FVG (M1)
        fvg_result = self.detect_fvg(m1_candles, direction, mss_result["displacement_candle_index"])

        # STEP 5 — CONFLUENCE (H1)
        confluence_count = self._check_h1_confluence(pair, direction)

        # SCORING
        l_score = 25.0 if sweep_result["detected"] else 0.0
        d_score = 25.0 if mss_result["detected"] else 0.0
        f_score = 25.0 if fvg_result["detected"] else 0.0
        c_score = min(confluence_count, 1) * 25.0
        score = l_score + d_score + f_score + c_score

        logger.info(f"[{pair}] Score breakdown — "
                    f"Liquidity: {l_score}/25 | "
                    f"Displacement: {d_score}/25 | "
                    f"FVG: {f_score}/25 | "
                    f"Confluence: {c_score}/25 | "
                    f"TOTAL: {score}/100 | "
                    f"Threshold: {self.config.aplus_threshold}")

        if score >= self.config.aplus_threshold:
            logger.info(f"[{pair}] | ✅ A+ SIGNAL DETECTED | Score: {score}")
        else:
            logger.info(f"[{pair}] | ❌ Score too low | {score} < {self.config.aplus_threshold} | Skipped")
            return None

        # ENTRY: FVG midpoint if FVG detected, else current M1 close
        if fvg_result["detected"]:
            entry = (fvg_result["fvg_high"] + fvg_result["fvg_low"]) / 2.0
        else:
            entry = float(m1_candles.iloc[-1]["close"])

        # SPREAD in pips
        spread_pips = self.mt5.get_current_spread(pair)
        if spread_pips < 0:
            spread_pips = 0.0

        # SL / TP CALCULATION
        spread_price_buffer = self._pips_to_price(2.0 * spread_pips, pair)

        if direction == "BUY":
            sl = sweep_result["sweep_price"] - spread_price_buffer
            risk_price = abs(entry - sl)
            tp1 = entry + risk_price * 1.0
            tp2 = entry + risk_price * 2.0
        else:  # SELL
            sl = sweep_result["sweep_price"] + spread_price_buffer
            risk_price = abs(sl - entry)
            tp1 = entry - risk_price * 1.0
            tp2 = entry - risk_price * 2.0

        # BUILD SIGNAL DICT
        signal = self.build_signal(
            pair=pair,
            direction=direction,
            entry=entry,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            score=score,
            spread=spread_pips,
            session=session,
            fvg_zone=fvg_result
        )

        logger.info(
            f"[{pair}] A+ SIGNAL | {direction} | Score: {score} | "
            f"Entry: {signal['entry_price']} | SL: {signal['sl_price']} | "
            f"TP1: {signal['tp1_price']} | TP2: {signal['tp2_price']} | "
            f"RR: {signal['effective_rr']}"
        )

        return signal
