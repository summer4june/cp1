from enum import Enum
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
import statistics

class State(Enum):
    NO_RANGE = 1
    POSSIBLE_RANGE = 2
    EARLY_ACCUMULATION = 3
    CONFIRMED_ACCUMULATION = 4
    LIQUIDITY_FORMED = 5
    MONITORING_FOR_MANIPULATION = 6
    MANIPULATION_STARTED = 7
    MANIPULATION_CONFIRMED = 8
    MSS = 9
    DISPLACEMENT = 10
    DISTRIBUTION = 11
    INVALID = 12
    RESET = 13

class ManipulationType(Enum):
    UPSIDE_MANIPULATION = 1
    DOWNSIDE_MANIPULATION = 2
    DOUBLE_SWEEP = 3

class MSSType(Enum):
    BULLISH_MSS = 1
    BEARISH_MSS = 2

class TradeDirection(Enum):
    LONG = 1
    SHORT = 2

@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    index: int = 0

@dataclass
class LiquidityCluster:
    level: float
    touches: int
    indices: List[int]
    type: str

@dataclass
class SwingPoint:
    index: int
    level: float
    type: str

@dataclass
class Manipulation:
    type: ManipulationType
    sweep_level: float
    candle_index: int
    direction_after: TradeDirection

@dataclass
class MSS:
    type: MSSType
    level: float
    confirmed: bool

@dataclass
class AccumulationState:
    state: State = State.NO_RANGE
    range_high: float = 0.0
    range_low: float = 0.0
    start_index: int = 0
    candle_count: int = 0
    expansion_count: int = 0
    high_touches: int = 0
    low_touches: int = 0
    locked: bool = False
    confidence: float = 0.0
    candles: List[Candle] = field(default_factory=list)
    equal_highs: List[LiquidityCluster] = field(default_factory=list)
    equal_lows: List[LiquidityCluster] = field(default_factory=list)
    bsl_target: float = 0.0
    ssl_target: float = 0.0
    manipulation: Optional[Manipulation] = None
    pending_manipulation: Optional[dict] = None
    mss: Optional[MSS] = None
    mss_target: float = 0.0
    mss_target_set: bool = False
    awaiting_displacement: bool = False
    displacement: bool = False
    post_manipulation_candles: List[Candle] = field(default_factory=list)
    monitoring_candles: int = 0
    partial_sweeps: int = 0
    cooldown_counter: int = 0
    signal_emitted_at: int = 0
    ready_for_monitoring: bool = False
    liquidity_map: dict = field(default_factory=dict)
    
@dataclass
class AMDSignal:
    signal_type: str
    timestamp: datetime
    direction: TradeDirection
    accumulation_range_high: float
    accumulation_range_low: float
    accumulation_range_size: float
    accumulation_duration: int
    accumulation_confidence: float
    manipulation_type: ManipulationType
    manipulation_sweep_level: float
    mss_type: MSSType
    mss_level: float
    bsl_level: float
    ssl_level: float
    market: str
    priority: str = "NORMAL"

class AccumulationEngine:
    global_invalid_reasons = {}
    
    TIMEFRAME = "1min"
    MIN_CANDLES = 10
    MAX_CANDLES = 120
    ATR_PERIOD = 20
    RANGE_ATR_MAX = 6.0
    RANGE_ATR_MIN = 0.3
    EQUAL_LEVEL_TOLERANCE = 0.0002
    BREAKOUT_BUFFER = 0.00051
    NET_DISPLACEMENT_MAX = 0.4
    MIN_SWEEP_RATIO = 0.05
    DISPLACEMENT_ATR_RATIO = 0.8
    MSS_TIMEOUT = 15
    MONITORING_TIMEOUT = 25
    COOLDOWN_CANDLES = 5
    MAX_EXPANSIONS_BEFORE_LOCK = 15
    CONFIDENCE_MIN = 0.4
    SIGNAL_TIMEOUT = 30
    
    def __init__(self, market: str, config: dict = None):
        self.market = market
        self.config = config or {}
        self.state_data = AccumulationState()
        self.history: List[Candle] = []
        self.atr_history: List[float] = []
        self.global_index = 0
        self.last_signal: Optional[AMDSignal] = None
        self.current_window: str = ""

    def _calculate_atr(self, candles: List[Candle], period: int = 20) -> Optional[float]:
        if len(candles) < period + 1:
            return None
        true_ranges = []
        for i in range(len(candles) - period, len(candles)):
            c = candles[i]
            prev_c = candles[i-1]
            tr = max(
                c.high - c.low,
                abs(c.high - prev_c.close),
                abs(c.low - prev_c.close)
            )
            true_ranges.append(tr)
        return statistics.mean(true_ranges)

    def _detect_swing_highs(self, candles: List[Candle]) -> List[SwingPoint]:
        swings = []
        for j in range(1, len(candles) - 1):
            if candles[j].high > candles[j-1].high and candles[j].high > candles[j+1].high:
                swings.append(SwingPoint(index=candles[j].index, level=candles[j].high, type="SWING_HIGH"))
        return swings

    def _detect_swing_lows(self, candles: List[Candle]) -> List[SwingPoint]:
        swings = []
        for j in range(1, len(candles) - 1):
            if candles[j].low < candles[j-1].low and candles[j].low < candles[j+1].low:
                swings.append(SwingPoint(index=candles[j].index, level=candles[j].low, type="SWING_LOW"))
        return swings

    def _detect_equal_highs(self, candles: List[Candle], tolerance_pct: float) -> List[LiquidityCluster]:
        if not candles:
            return []
        price = candles[-1].close
        tolerance_abs = price * tolerance_pct
        highs = [(c.index, c.high) for c in candles]
        sorted_highs = sorted(highs, key=lambda x: x[1], reverse=True)
        clusters = []
        used = set()
        for i in range(len(sorted_highs)):
            idx, value = sorted_highs[i]
            if idx in used:
                continue
            cluster = [(idx, value)]
            used.add(idx)
            for j in range(i + 1, len(sorted_highs)):
                idx2, value2 = sorted_highs[j]
                if idx2 in used:
                    continue
                if value - value2 <= tolerance_abs:
                    if abs(idx - idx2) >= 2:
                        cluster.append((idx2, value2))
                        used.add(idx2)
                else:
                    break
            if len(cluster) >= 2:
                avg_level = statistics.mean([v for _, v in cluster])
                clusters.append(LiquidityCluster(level=avg_level, touches=len(cluster), indices=[i for i, _ in cluster], type="BSL"))
        clusters.sort(key=lambda x: x.touches, reverse=True)
        return clusters

    def _detect_equal_lows(self, candles: List[Candle], tolerance_pct: float) -> List[LiquidityCluster]:
        if not candles:
            return []
        price = candles[-1].close
        tolerance_abs = price * tolerance_pct
        lows = [(c.index, c.low) for c in candles]
        sorted_lows = sorted(lows, key=lambda x: x[1])
        clusters = []
        used = set()
        for i in range(len(sorted_lows)):
            idx, value = sorted_lows[i]
            if idx in used:
                continue
            cluster = [(idx, value)]
            used.add(idx)
            for j in range(i + 1, len(sorted_lows)):
                idx2, value2 = sorted_lows[j]
                if idx2 in used:
                    continue
                if value2 - value <= tolerance_abs:
                    if abs(idx - idx2) >= 2:
                        cluster.append((idx2, value2))
                        used.add(idx2)
                else:
                    break
            if len(cluster) >= 2:
                avg_level = statistics.mean([v for _, v in cluster])
                clusters.append(LiquidityCluster(level=avg_level, touches=len(cluster), indices=[i for i, _ in cluster], type="SSL"))
        clusters.sort(key=lambda x: x.touches, reverse=True)
        return clusters

    def _detect_internal_liquidity(self, candles: List[Candle], range_high: float, range_low: float) -> dict:
        range_size = range_high - range_low
        buffer = range_size * 0.15
        internal_zone_high = range_high - buffer
        internal_zone_low = range_low + buffer
        swing_highs = self._detect_swing_highs(candles)
        swing_lows = self._detect_swing_lows(candles)
        return {
            "internal_bsl": [sh for sh in swing_highs if internal_zone_low < sh.level < internal_zone_high],
            "internal_ssl": [sl for sl in swing_lows if internal_zone_low < sl.level < internal_zone_high],
            "boundary_bsl": [sh for sh in swing_highs if sh.level >= internal_zone_high],
            "boundary_ssl": [sl for sl in swing_lows if sl.level <= internal_zone_low]
        }

    def _calculate_confidence(self, equal_highs: List[LiquidityCluster], equal_lows: List[LiquidityCluster], atr: float) -> float:
        range_size = self.state_data.range_high - self.state_data.range_low
        if range_size <= 0:
            return 0.0
        score = 0.4
        total_touches = sum([ch.touches for ch in equal_highs[:1]]) + sum([cl.touches for cl in equal_lows[:1]])
        min_candles = self.config.get("min_accumulation_candles", self.MIN_CANDLES)
        score += max(0, min(0.15, (self.state_data.candle_count - min_candles) * 0.01))
        score += max(0, min(0.15, (1.0 - (range_size / (atr * self.RANGE_ATR_MAX))) * 0.2))
        score -= self.state_data.expansion_count * 0.05
        if self.state_data.candles:
            net_disp = abs(self.state_data.candles[-1].close - self.state_data.candles[0].close) / range_size
            score -= net_disp * 0.3
        return max(0.0, min(1.0, score))

    def _validate_candle_behaviour(self, candles: List[Candle], range_high: float, range_low: float) -> tuple[bool, str]:
        range_size = range_high - range_low
        if range_size <= 0 or not candles:
            return False, "invalid_range_or_empty"
        N = len(candles)
        bodies = [abs(c.close - c.open) for c in candles]
        if statistics.mean(bodies) > range_size * 0.3:
            return False, "avg_body_too_large"
        if max(bodies) > range_size * 0.8:  # From part 5.6
            return False, "single_impulse_candle"
        if N > 1:
            directions = [1 if c.close > c.open else -1 for c in candles]
            changes = sum(1 for i in range(1, N) if directions[i] != directions[i-1])
            min_changes = self.config.get("min_directional_changes", 0.15)
            if changes / (N - 1) < min_changes:
                return False, "too_directional"
            max_run = current_run = 1
            for i in range(1, N):
                if directions[i] == directions[i-1]:
                    current_run += 1
                    max_run = max(max_run, current_run)
                else:
                    current_run = 1
            
            max_sustained_run = self.config.get("max_sustained_run", 8)
            if max_run >= max_sustained_run:
                return False, "sustained_run"
        return True, ""

    def _is_genuine_breakout(self) -> bool:
        if len(self.state_data.candles) < 5:
            return False
        # Check last 5 candles
        last_candles = self.state_data.candles[-5:]
        buffer = self.BREAKOUT_BUFFER * last_candles[-1].close
        
        # If all 5 closed above range_high + buffer
        if all(c.close > self.state_data.range_high + buffer for c in last_candles):
            return True
        # If all 5 closed below range_low - buffer
        if all(c.close < self.state_data.range_low - buffer for c in last_candles):
            return True
            
        return False

    def _transition(self, new_state: State, reason: str = ""):
        if new_state == State.INVALID:
            r = reason or "unknown"
            AccumulationEngine.global_invalid_reasons[r] = AccumulationEngine.global_invalid_reasons.get(r, 0) + 1
            self.state_data.state = State.INVALID
            self.state_data.cooldown_counter = 0
        elif new_state == State.RESET:
            self.state_data = AccumulationState(state=State.NO_RANGE)
        else:
            self.state_data.state = new_state

    def process_candle(self, c_open: float, c_high: float, c_low: float, c_close: float, c_time: datetime, is_in_macro: bool = False, window_name: str = ""):
        self.current_window = window_name
        candle = Candle(timestamp=c_time, open=c_open, high=c_high, low=c_low, close=c_close, index=self.global_index)
        self.global_index += 1
        self.history.append(candle)
        if len(self.history) > 100:
            self.history = self.history[-100:]
            
        atr = self._calculate_atr(self.history, self.ATR_PERIOD)
        if atr is None:
            return
        self.atr_history.append(atr)
            
        if self.state_data.state not in (State.NO_RANGE, State.INVALID, State.RESET, State.DISTRIBUTION):
            self.state_data.candles.append(candle)
            
        state = self.state_data.state
        if state == State.NO_RANGE:
            self._scan_for_range(atr)
        elif state == State.POSSIBLE_RANGE:
            self._validate_range(candle, atr)
        elif state == State.EARLY_ACCUMULATION:
            self._attempt_confirmation(candle, atr)
        elif state == State.CONFIRMED_ACCUMULATION:
            self._build_liquidity(candle, atr)
        elif state == State.LIQUIDITY_FORMED:
            self._check_macro_window(candle, is_in_macro)
        elif state == State.MONITORING_FOR_MANIPULATION:
            self._scan_manipulation(candle, atr, is_in_macro)
        elif state == State.MANIPULATION_STARTED:
            self._confirm_manipulation(candle)
        elif state == State.MANIPULATION_CONFIRMED:
            self._search_mss(candle, atr)
        elif state == State.MSS:
            self._search_displacement(candle, atr)
        elif state == State.DISPLACEMENT:
            self._emit_signal(candle)
        elif state == State.DISTRIBUTION:
            self._manage_distribution(candle)
        elif state == State.INVALID:
            self._run_cooldown()
        elif state == State.RESET:
            self._transition(State.NO_RANGE)

    def _scan_for_range(self, atr: float):
        W = 5
        if len(self.history) < W:
            return
        window = self.history[-W:]
        window_high = max(c.high for c in window)
        window_low = min(c.low for c in window)
        window_range = window_high - window_low
        
        market_base = self.market.lower()
        if market_base.endswith('m'):
            market_base = market_base[:-1]
            
        max_pips = self.config.get(f"max_acc_pips_{market_base}", self.config.get("max_acc_pips", None))
        if max_pips is not None:
            max_size = max_pips * 0.1
            if window_range > max_size:
                return
            if window_range < atr * self.RANGE_ATR_MIN:
                return
        else:
            if window_range < atr * self.RANGE_ATR_MIN or window_range > atr * self.RANGE_ATR_MAX:
                return
                
        net_disp = abs(window[-1].close - window[0].close) / window_range
        if net_disp >= 0.6:
            return
        directions = [1 if c.close > c.open else -1 for c in window]
        changes = sum(1 for i in range(1, W) if directions[i] != directions[i-1])
        if changes / (W-1) < 0.2:
            return
        self._transition(State.POSSIBLE_RANGE)
        self.state_data.range_high = window_high
        self.state_data.range_low = window_low
        self.state_data.start_index = window[0].index
        self.state_data.candle_count = W
        self.state_data.candles = window.copy()
        self.state_data.confidence = 0.1

    def _validate_range(self, candle: Candle, atr: float):
        self._update_range_boundaries(candle, atr)
        if self.state_data.state == State.INVALID:
            return
            
        if self._is_genuine_breakout():
            self._transition(State.INVALID, "genuine_breakout")
            return
            
        if self.state_data.candle_count >= 7:
            directions = [1 if c.close > c.open else -1 for c in self.state_data.candles]
            changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i-1])
            if changes >= 3:
                self._transition(State.EARLY_ACCUMULATION)
                self.state_data.confidence = 0.2

    def _attempt_confirmation(self, candle: Candle, atr: float):
        self._update_range_boundaries(candle, atr)
        if self.state_data.state == State.INVALID:
            return
            
        min_candles = self.config.get("min_accumulation_candles", self.MIN_CANDLES)
        if self.state_data.candle_count >= min_candles:
            net_disp = abs(self.state_data.candles[-1].close - self.state_data.candles[0].close) / (self.state_data.range_high - self.state_data.range_low)
            if net_disp >= 0.4:
                return
                
            equal_highs = self._detect_equal_highs(self.state_data.candles, self.EQUAL_LEVEL_TOLERANCE)
            equal_lows = self._detect_equal_lows(self.state_data.candles, self.EQUAL_LEVEL_TOLERANCE)
            
            if (len(equal_highs) > 0 and equal_highs[0].touches >= 2) or (len(equal_lows) > 0 and equal_lows[0].touches >= 2):
                is_valid, reason = self._validate_candle_behaviour(self.state_data.candles, self.state_data.range_high, self.state_data.range_low)
                if not is_valid:
                    self._transition(State.INVALID, reason)
                    return
                confidence = self._calculate_confidence(equal_highs, equal_lows, atr)
                if confidence >= self.CONFIDENCE_MIN:
                    self.state_data.equal_highs = equal_highs
                    self.state_data.equal_lows = equal_lows
                    self.state_data.confidence = confidence
                    self._transition(State.CONFIRMED_ACCUMULATION)

    def _build_liquidity(self, candle: Candle, atr: float):
        range_size = self.state_data.range_high - self.state_data.range_low
        high_zone = self.state_data.range_high - (range_size * 0.1)
        low_zone = self.state_data.range_low + (range_size * 0.1)
        
        if candle.high >= high_zone:
            self.state_data.high_touches += 1
        if candle.low <= low_zone:
            self.state_data.low_touches += 1
            
        self.state_data.candle_count += 1
        
        max_candles = self.config.get("max_accumulation_candles", self.MAX_CANDLES)
        if self.state_data.candle_count > max_candles:
            self._transition(State.INVALID, "timeout_building_liquidity")
            return
            
        if self._is_genuine_breakout():
            self._transition(State.INVALID, "genuine_breakout")
            return
            
        if self.state_data.high_touches >= 2 and self.state_data.low_touches >= 2:
            self.state_data.locked = True
            self.state_data.liquidity_map = self._detect_internal_liquidity(
                self.state_data.candles, self.state_data.range_high, self.state_data.range_low
            )
            self.state_data.bsl_target = self.state_data.equal_highs[0].level if self.state_data.equal_highs else self.state_data.range_high
            self.state_data.ssl_target = self.state_data.equal_lows[0].level if self.state_data.equal_lows else self.state_data.range_low
            self.state_data.confidence += 0.1
            self._transition(State.LIQUIDITY_FORMED)

    def _check_macro_window(self, candle: Candle, is_in_macro: bool):
        self.state_data.candle_count += 1
        max_candles = self.config.get("max_accumulation_candles", self.MAX_CANDLES)
        if self.state_data.candle_count > max_candles:
            self._transition(State.INVALID, "timeout_waiting_for_macro")
            return
        if self._is_genuine_breakout():
            self._transition(State.INVALID, "genuine_breakout")
            return
        if is_in_macro:
            self._transition(State.MONITORING_FOR_MANIPULATION)

    def _scan_manipulation(self, candle: Candle, atr: float, is_in_macro: bool):
        if not is_in_macro:
            self._transition(State.INVALID, "macro_ended")
            return
            
        range_size = self.state_data.range_high - self.state_data.range_low
        min_sweep = range_size * self.MIN_SWEEP_RATIO
        
        if candle.high > self.state_data.range_high + min_sweep and candle.high >= self.state_data.bsl_target:
            self.state_data.pending_manipulation = {
                "type": "UPSIDE",
                "sweep_candle": candle,
                "index": candle.index
            }
            self._transition(State.MANIPULATION_STARTED)
            return
            
        if candle.low < self.state_data.range_low - min_sweep and candle.low <= self.state_data.ssl_target:
            self.state_data.pending_manipulation = {
                "type": "DOWNSIDE",
                "sweep_candle": candle,
                "index": candle.index
            }
            self._transition(State.MANIPULATION_STARTED)
            return
            
        self.state_data.monitoring_candles += 1
        if self.state_data.monitoring_candles > self.MONITORING_TIMEOUT:
            self._transition(State.INVALID, "monitoring_timeout_no_manipulation")
            
        if self._is_genuine_breakout():
            self._transition(State.INVALID, "genuine_breakout")

    def _confirm_manipulation(self, candle: Candle):
        pending = self.state_data.pending_manipulation
        sweep_candle = pending["sweep_candle"]
        
        # Track how many candles we've waited for confirmation
        if "wait_count" not in pending:
            pending["wait_count"] = 0
        pending["wait_count"] += 1
        
        if pending["type"] == "UPSIDE":
            if candle.close < self.state_data.range_high:
                self.state_data.manipulation = Manipulation(
                    type=ManipulationType.UPSIDE_MANIPULATION,
                    sweep_level=max(sweep_candle.high, candle.high),
                    candle_index=pending["index"],
                    direction_after=TradeDirection.SHORT
                )
                self.state_data.confidence += 0.15
                self._transition(State.MANIPULATION_CONFIRMED)
            elif pending["wait_count"] >= 5:
                # Waited 5 candles for a close back inside range, didn't happen.
                self._transition(State.INVALID, "genuine_breakout_up")
            else:
                # Update sweep high if this candle went higher
                pending["sweep_candle"].high = max(pending["sweep_candle"].high, candle.high)
        elif pending["type"] == "DOWNSIDE":
            if candle.close > self.state_data.range_low:
                self.state_data.manipulation = Manipulation(
                    type=ManipulationType.DOWNSIDE_MANIPULATION,
                    sweep_level=min(sweep_candle.low, candle.low),
                    candle_index=pending["index"],
                    direction_after=TradeDirection.LONG
                )
                self.state_data.confidence += 0.15
                self._transition(State.MANIPULATION_CONFIRMED)
            elif pending["wait_count"] >= 5:
                # Waited 5 candles for a close back inside range, didn't happen.
                self._transition(State.INVALID, "genuine_breakout_down")
            else:
                # Update sweep low if this candle went lower
                pending["sweep_candle"].low = min(pending["sweep_candle"].low, candle.low)

    def _search_mss(self, candle: Candle, atr: float):
        self.state_data.post_manipulation_candles.append(candle)
        if len(self.state_data.post_manipulation_candles) > self.MSS_TIMEOUT:
            self._transition(State.INVALID, "mss_timeout")
            return
            
        if self.state_data.manipulation.type == ManipulationType.DOWNSIDE_MANIPULATION:
            if candle.low < self.state_data.manipulation.sweep_level:
                self._transition(State.INVALID, "manipulation_failed_new_low")
                return
            swing_highs = self._detect_swing_highs(self.state_data.post_manipulation_candles)
            if swing_highs and not self.state_data.mss_target_set:
                self.state_data.mss_target = swing_highs[0].level
                self.state_data.mss_target_set = True
            if self.state_data.mss_target_set and candle.close > self.state_data.mss_target:
                self.state_data.mss = MSS(type=MSSType.BULLISH_MSS, level=self.state_data.mss_target, confirmed=True)
                self.state_data.confidence += 0.1
                self._transition(State.MSS)
                
        elif self.state_data.manipulation.type == ManipulationType.UPSIDE_MANIPULATION:
            if candle.high > self.state_data.manipulation.sweep_level:
                self._transition(State.INVALID, "manipulation_failed_new_high")
                return
            swing_lows = self._detect_swing_lows(self.state_data.post_manipulation_candles)
            if swing_lows and not self.state_data.mss_target_set:
                self.state_data.mss_target = swing_lows[0].level
                self.state_data.mss_target_set = True
            if self.state_data.mss_target_set and candle.close < self.state_data.mss_target:
                self.state_data.mss = MSS(type=MSSType.BEARISH_MSS, level=self.state_data.mss_target, confirmed=True)
                self.state_data.confidence += 0.1
                self._transition(State.MSS)

    def _search_displacement(self, candle: Candle, atr: float):
        self.state_data.post_manipulation_candles.append(candle)
        body = abs(candle.close - candle.open)
        if body >= atr * self.DISPLACEMENT_ATR_RATIO:
            is_bullish = candle.close > candle.open
            expected = self.state_data.manipulation.direction_after == TradeDirection.LONG
            if (is_bullish and expected) or (not is_bullish and not expected):
                self.state_data.displacement = True
                self.state_data.confidence += 0.1
                self._transition(State.DISPLACEMENT)
                return
                
        if len(self.state_data.post_manipulation_candles) > self.MSS_TIMEOUT + 5:
            self._transition(State.INVALID, "displacement_timeout")

    def _get_earlier_session_bias(self) -> Optional[TradeDirection]:
        if len(self.history) < 20:
            return None
        start_price = self.history[0].close
        range_start_price = self.state_data.candles[0].close if self.state_data.candles else self.history[-1].close
        if range_start_price > start_price:
            return TradeDirection.LONG
        elif range_start_price < start_price:
            return TradeDirection.SHORT
        return None

    def _emit_signal(self, candle: Candle):
        sd = self.state_data
        
        priority = "NORMAL"
        if "Macro 3" in self.current_window:
            priority = "HIGH"
            
        final_confidence = sd.confidence
        prior_bias = self._get_earlier_session_bias()
        
        # Silver Bullet (Macro 5 or 6) counter-trend penalty
        if prior_bias and any(m in self.current_window for m in ["Macro 5", "Silver Bullet"]):
            if sd.manipulation.direction_after != prior_bias:
                final_confidence = max(0.0, final_confidence - 0.15)
                
        # Reversal (Macro 7 or 8 or 9 or 10) alignment boost
        if prior_bias and "Reversal" in self.current_window:
            if sd.manipulation.direction_after != prior_bias:
                final_confidence = min(1.0, final_confidence + 0.15)
        
        self.last_signal = AMDSignal(
            signal_type="AMD_READY",
            timestamp=candle.timestamp,
            direction=sd.manipulation.direction_after,
            accumulation_range_high=sd.range_high,
            accumulation_range_low=sd.range_low,
            accumulation_range_size=sd.range_high - sd.range_low,
            accumulation_duration=sd.candle_count,
            accumulation_confidence=final_confidence,
            manipulation_type=sd.manipulation.type,
            manipulation_sweep_level=sd.manipulation.sweep_level,
            mss_type=sd.mss.type,
            mss_level=sd.mss.level,
            bsl_level=sd.bsl_target,
            ssl_level=sd.ssl_target,
            market=self.market,
            priority=priority
        )
        self.state_data.signal_emitted_at = self.global_index
        self._transition(State.DISTRIBUTION)

    def _manage_distribution(self, candle: Candle):
        if self.global_index - self.state_data.signal_emitted_at > self.SIGNAL_TIMEOUT:
            self._transition(State.RESET)

    def _run_cooldown(self):
        self.state_data.cooldown_counter += 1
        if self.state_data.cooldown_counter >= self.COOLDOWN_CANDLES:
            self._transition(State.RESET)

    def _update_range_boundaries(self, candle: Candle, atr: float):
        market_base = self.market.lower()
        if market_base.endswith('m'):
            market_base = market_base[:-1]
            
        max_pips = self.config.get(f"max_acc_pips_{market_base}", self.config.get("max_acc_pips", None))
        max_size = max_pips * 0.1 if max_pips is not None else atr * self.RANGE_ATR_MAX
        
        if candle.high > self.state_data.range_high:
            if (candle.high - self.state_data.range_low) > max_size:
                self._transition(State.INVALID, "range_exceeded_max")
                return
            self.state_data.range_high = candle.high
            self.state_data.expansion_count += 1
            
        if candle.low < self.state_data.range_low:
            if (self.state_data.range_high - candle.low) > max_size:
                self._transition(State.INVALID, "range_exceeded_max")
                return
            self.state_data.range_low = candle.low
            self.state_data.expansion_count += 1
            
        self.state_data.candle_count += 1
        max_candles = self.config.get("max_accumulation_candles", self.MAX_CANDLES)
        if self.state_data.candle_count > max_candles:
            self._transition(State.INVALID, "timeout")
            return

        if self.state_data.expansion_count > self.MAX_EXPANSIONS_BEFORE_LOCK and self.state_data.state in (State.POSSIBLE_RANGE, State.EARLY_ACCUMULATION):
            self._transition(State.INVALID, "excessive_expansion")

    def consume_signal(self, window_name: str = "") -> Optional[AMDSignal]:
        if "Macro 1" in window_name:
            # Suppress signal emission during Macro 1 (Observe Only)
            return None
            
        sig = self.last_signal
        if sig:
            self.last_signal = None
            self._transition(State.RESET)
        return sig
