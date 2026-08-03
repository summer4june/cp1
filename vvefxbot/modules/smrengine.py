from enum import Enum
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta
import pytz

class SessionName(Enum):
    ASIAN = "ASIAN"
    LONDON = "LONDON"
    NEW_YORK = "NEW_YORK"

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

@dataclass
class SessionRecord:
    session_name: str
    trading_day: str
    high: float = -1.0
    low: float = float('inf')
    status: str = "IN_PROGRESS"
    high_consumed: bool = False
    low_consumed: bool = False

@dataclass
class SMRSignal:
    timestamp: datetime
    direction: TradeDirection
    session_swept: str
    sweep_type: str  # "HIGH" or "LOW"
    sweep_level: float
    mss_level: float
    ob_high: float
    ob_low: float

class SMREngine:
    """
    Implements HYDRA Leg B (SMR) Logic exactly as per specification:
    Session Tracking -> Sweep -> 2 Consecutive OBs -> Displacement w/ Momentum -> Retest
    """
    
    IST_TZ = pytz.timezone("Asia/Kolkata")
    
    SESSIONS = {
        SessionName.ASIAN.value: {"start": dt_time(2, 35), "end": dt_time(12, 30), "crosses_midnight": False},
        SessionName.LONDON.value: {"start": dt_time(12, 30), "end": dt_time(17, 30), "crosses_midnight": False},
        SessionName.NEW_YORK.value: {"start": dt_time(17, 30), "end": dt_time(2, 30), "crosses_midnight": True},
    }
    
    WINDOW_START = dt_time(19, 0)
    WINDOW_END = dt_time(19, 30)
    BODY_THRESHOLD = 0.60

    def __init__(self, pair: str):
        self.pair = pair
        self.sessions: Dict[str, SessionRecord] = {}
        
        self.history = []
        self.reset()

    def reset(self):
        self.state = "SCANNING"
        self.active_sweep_type = None
        self.active_sweep_session = None
        self.active_sweep_level = None
        
        self.extreme_point = None
        self.ob_data = None
        self.signal = None

    def _get_trading_day(self, dt_ist: datetime) -> str:
        t = dt_ist.time()
        if t >= dt_time(2, 35):
            return dt_ist.strftime("%Y-%m-%d")
        else:
            return (dt_ist - timedelta(days=1)).strftime("%Y-%m-%d")
            
    def _is_in_session(self, dt_ist: datetime, session_info: dict) -> bool:
        t = dt_ist.time()
        if not session_info["crosses_midnight"]:
            return session_info["start"] <= t < session_info["end"]
        else:
            return t >= session_info["start"] or t < session_info["end"]

    def _update_sessions(self, dt_ist: datetime, high: float, low: float):
        day = self._get_trading_day(dt_ist)
        for s_name, s_info in self.SESSIONS.items():
            key = f"{day}_{s_name}"
            if self._is_in_session(dt_ist, s_info):
                if key not in self.sessions:
                    self.sessions[key] = SessionRecord(s_name, day)
                rec = self.sessions[key]
                if high > rec.high:
                    rec.high = high
                if low < rec.low:
                    rec.low = low

    def _find_consecutive_opposite_obs(self, setup_direction: str) -> Optional[dict]:
        """
        Finds the most recent pair of 2 CONSECUTIVE opposite candles before the current extreme.
        For BEARISH setup (sell): find 2 consecutive BULLISH (up) candles before the drop.
        For BULLISH setup (buy): find 2 consecutive BEARISH (down) candles before the pump.
        """
        target_type = "BULLISH" if setup_direction == "BEARISH" else "BEARISH"
        
        def is_target_candle(candle):
            if target_type == "BEARISH":
                return candle['close'] < candle['open']
            else:
                return candle['close'] > candle['open']
                
        # We search from the candle *just before* the extreme point candle backward.
        # So we need to find where the extreme point happened.
        extreme_index = -1
        for i in range(len(self.history)-1, -1, -1):
            if setup_direction == "BEARISH" and self.history[i]['high'] == self.extreme_point:
                extreme_index = i
                break
            elif setup_direction == "BULLISH" and self.history[i]['low'] == self.extreme_point:
                extreme_index = i
                break
                
        if extreme_index <= 1:
            return None
            
        candles_before = self.history[:extreme_index]
        n = len(candles_before)
        
        for i in range(n - 1, 0, -1):
            candle_later = candles_before[i]
            candle_earlier = candles_before[i - 1]
            
            if is_target_candle(candle_later) and is_target_candle(candle_earlier):
                ob_1 = candle_earlier
                ob_2 = candle_later
                
                if setup_direction == "BEARISH":
                    return {
                        "ob_1": ob_1, "ob_2": ob_2,
                        "displacement_target": ob_2['low'],
                        "retest_zone_high": ob_2['high'],
                        "retest_zone_low": ob_2['low'],
                        "mss_level": ob_2['low']
                    }
                else:
                    return {
                        "ob_1": ob_1, "ob_2": ob_2,
                        "displacement_target": ob_2['high'],
                        "retest_zone_high": ob_2['high'],
                        "retest_zone_low": ob_2['low'],
                        "mss_level": ob_2['high']
                    }
        return None

    def _check_displacement(self, candle: dict, ob_data: dict, setup_direction: str) -> bool:
        body = abs(candle['close'] - candle['open'])
        total_range = candle['high'] - candle['low']
        if total_range == 0:
            return False
            
        body_ratio = body / total_range
        
        if setup_direction == "BEARISH":
            is_bearish = candle['close'] < candle['open']
            closes_below = candle['close'] < ob_data['displacement_target']
            has_momentum = body_ratio >= self.BODY_THRESHOLD
            return is_bearish and closes_below and has_momentum
        else:
            is_bullish = candle['close'] > candle['open']
            closes_above = candle['close'] > ob_data['displacement_target']
            has_momentum = body_ratio >= self.BODY_THRESHOLD
            return is_bullish and closes_above and has_momentum

    def process_candle(self, dt_utc: datetime, open_p: float, high: float, low: float, close: float):
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=pytz.utc)
        dt_ist = dt_utc.astimezone(self.IST_TZ)
        
        candle = {'time': dt_ist, 'open': open_p, 'high': high, 'low': low, 'close': close}
        self.history.append(candle)
        if len(self.history) > 200:
            self.history.pop(0)
            
        self._update_sessions(dt_ist, high, low)
        day = self._get_trading_day(dt_ist)
        
        if self.state == "SCANNING":
            for key, rec in self.sessions.items():
                if not rec.high_consumed and rec.high > 0 and high > rec.high:
                    self.state = "IDENTIFYING_OB"
                    self.active_sweep_type = "HIGH"
                    self.active_sweep_session = rec.session_name
                    self.active_sweep_level = rec.high
                    self.extreme_point = high
                    rec.high_consumed = True
                    break
                    
                if not rec.low_consumed and rec.low < float('inf') and low < rec.low:
                    self.state = "IDENTIFYING_OB"
                    self.active_sweep_type = "LOW"
                    self.active_sweep_session = rec.session_name
                    self.active_sweep_level = rec.low
                    self.extreme_point = low
                    rec.low_consumed = True
                    break

        elif self.state == "IDENTIFYING_OB":
            # Update extreme if it gets pushed further before we find OBs
            if self.active_sweep_type == "HIGH" and high > self.extreme_point:
                self.extreme_point = high
            elif self.active_sweep_type == "LOW" and low < self.extreme_point:
                self.extreme_point = low
                
            setup_dir = "BEARISH" if self.active_sweep_type == "HIGH" else "BULLISH"
            self.ob_data = self._find_consecutive_opposite_obs(setup_dir)
            
            if self.ob_data:
                self.state = "OB_IDENTIFIED"

        elif self.state == "OB_IDENTIFIED":
            setup_dir = "BEARISH" if self.active_sweep_type == "HIGH" else "BULLISH"
            
            # Check for Swing Update rule
            if setup_dir == "BEARISH" and high > self.extreme_point:
                self.extreme_point = high
                self.state = "IDENTIFYING_OB"
                return
            elif setup_dir == "BULLISH" and low < self.extreme_point:
                self.extreme_point = low
                self.state = "IDENTIFYING_OB"
                return
                
            if self._check_displacement(candle, self.ob_data, setup_dir):
                self.state = "OB_DISPLACED"

        elif self.state == "OB_DISPLACED":
            t = dt_ist.time()
            if self.WINDOW_START <= t < self.WINDOW_END:
                setup_dir = "BEARISH" if self.active_sweep_type == "HIGH" else "BULLISH"
                
                if setup_dir == "BEARISH":
                    # Retest of the bearish OB
                    if high >= self.ob_data["retest_zone_low"]:
                        self.signal = SMRSignal(
                            timestamp=dt_utc,
                            direction=TradeDirection.SHORT,
                            session_swept=self.active_sweep_session,
                            sweep_type="HIGH",
                            sweep_level=self.active_sweep_level,
                            mss_level=self.ob_data["mss_level"],
                            ob_high=self.ob_data["retest_zone_high"],
                            ob_low=self.ob_data["retest_zone_low"]
                        )
                        self.state = "SIGNAL_FIRED"
                else:
                    # Retest of the bullish OB
                    if low <= self.ob_data["retest_zone_high"]:
                        self.signal = SMRSignal(
                            timestamp=dt_utc,
                            direction=TradeDirection.LONG,
                            session_swept=self.active_sweep_session,
                            sweep_type="LOW",
                            sweep_level=self.active_sweep_level,
                            mss_level=self.ob_data["mss_level"],
                            ob_high=self.ob_data["retest_zone_high"],
                            ob_low=self.ob_data["retest_zone_low"]
                        )
                        self.state = "SIGNAL_FIRED"
            elif t >= self.WINDOW_END:
                self.reset()
                
    def get_signal(self):
        return self.signal
        
    def consume_signal(self):
        sig = self.signal
        self.signal = None
        return sig
