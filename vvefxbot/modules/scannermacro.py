"""
scannermacro.py — ICT Hydra Entry Model (Macro Strategy)
"""
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta, time as dt_time
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine

logger = get_logger("ScannerMacro")

class ScannerMacro:
    """
    Scanner for the ICT Hydra Entry Model (Macro Strategy).
    Runs strictly on M1 timeframe during specific IST macro windows.
    Detects Accumulation -> Manipulation (Sweep) -> Distribution (MSS) -> 61.8% Fib Entry.
    """

    _IST_OFFSET = timedelta(hours=5, minutes=30)

    # Macro Windows (IST)
    # Stored as (start_hour, start_minute, end_hour, end_minute, window_name, window_type)
    MACRO_WINDOWS = [
        (17, 50, 18, 20, "Macro 1", "Manipulation"),
        (18, 20, 18, 40, "Macro 2", "Continuation"),
        (18, 40, 19, 20, "Macro 3", "Manipulation"),
        (19, 20, 19, 40, "Macro 4", "Continuation"),
        (19, 40, 20, 20, "Silver Bullet 5", "Manipulation"),
        (20, 20, 20, 40, "Silver Bullet 6", "Continuation"),
        (22, 50, 23, 20, "Reversal 7", "Manipulation"),
        (23, 20, 23, 40, "Reversal 8", "Continuation"),
        (23, 40,  0, 20, "Reversal 9", "Manipulation"), # Crosses midnight!
        ( 0, 20,  0, 40, "Reversal 10", "Continuation")
    ]

    def __init__(self, config: Config, mt5: MT5Connector, state: StateEngine):
        self.config = config
        self.mt5 = mt5
        self.state = state
        self.macro_cfg = getattr(config, "macro_strategy", {})
        self._last_signal_time = {}

    def _get_active_macro(self, current_dt_ist: datetime):
        """Returns (window_name, window_type) if currently in a macro window, else None."""
        curr_t = current_dt_ist.time()
        
        for (sh, sm, eh, em, name, wtype) in self.MACRO_WINDOWS:
            start_t = dt_time(sh, sm)
            end_t = dt_time(eh, em)
            
            # Handle midnight crossing for Reversal 9 (23:40 - 00:20)
            if start_t > end_t:
                if curr_t >= start_t or curr_t < end_t:
                    return name, wtype
            else:
                if start_t <= curr_t < end_t:
                    return name, wtype
                    
        return None

    def scan(self, pair: str, session: str = None, killzone: str = None) -> dict | None:
        if not self.macro_cfg.get("enabled", False):
            return None

        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc + self._IST_OFFSET
        
        active_macro = self._get_active_macro(now_ist)
        if not active_macro:
            return None
            
        window_name, window_type = active_macro

        # Cooldown check (prevent spamming same pair in same macro window if recently signaled)
        last_time = self._last_signal_time.get(pair)
        if last_time and (now_utc - last_time).total_seconds() < 1200: # 20 min cooldown
            return None

        # Fetch M1 candles
        # We need enough history for the macro window + accumulation lookback
        df = self.mt5.get_candles(pair, "M1", count=150)
        if df is None or len(df) < 50:
            return None
            
        df = df.reset_index(drop=True)
        current_price = self.mt5.get_current_bid(pair)
        if not current_price:
            return None

        signal = self._detect_amd(df, pair, current_price, window_name, window_type)
        
        if signal:
            self._last_signal_time[pair] = now_utc
            
        return signal

    def _detect_amd(self, df: pd.DataFrame, pair: str, current_price: float, window_name: str, window_type: str) -> dict | None:
        """
        Detect Accumulation -> Manipulation -> Distribution -> Fib Entry.
        """
        acc_lookback = self.macro_cfg.get("accumulation_lookback", 20)
        fib_level = self.macro_cfg.get("fib_entry_level", 0.618)
        rr_target = self.macro_cfg.get("risk_reward", 3.0)
        
        n = len(df)
        # 1. Identify the recent swing extremes (potential sweep points) within the last 30 candles
        recent_window = 40
        recent_df = df.iloc[-recent_window:]
        
        lowest_idx = recent_df['low'].idxmin()
        highest_idx = recent_df['high'].idxmax()
        
        lowest_low = float(df.iloc[lowest_idx]['low'])
        highest_high = float(df.iloc[highest_idx]['high'])
        
        # Determine if we are sweeping SSL (Long) or BSL (Short)
        # A sweep implies the extreme broke out of a preceding accumulation range.
        
        # --- CHECK LONG SETUP ---
        # Accumulation range before the lowest_low
        if lowest_idx > acc_lookback:
            acc_df_long = df.iloc[lowest_idx - acc_lookback : lowest_idx]
            acc_low = float(acc_df_long['low'].min())
            acc_high = float(acc_df_long['high'].max())
            
            # Sweep check: did the lowest_low sweep below the accumulation low?
            if lowest_low < acc_low:
                # MSS check: Did price close above a recent swing high after the sweep?
                local_high = float(acc_df_long.iloc[-5:]['high'].max()) if len(acc_df_long) >= 5 else acc_high
                
                post_sweep_df = df.iloc[lowest_idx + 1:]
                mss_confirmed = any(post_sweep_df['close'] > local_high)
                
                if mss_confirmed:
                    # Find the highest point reached after the sweep
                    post_sweep_high = float(post_sweep_df['high'].max())
                    
                    # Calculate Fib 61.8% retracement (Discount Zone)
                    swing_range = post_sweep_high - lowest_low
                    if swing_range > 0:
                        # 0% is post_sweep_high, 100% is lowest_low. 
                        # 61.8% retracement down from high:
                        fib_618_price = post_sweep_high - (swing_range * fib_level)
                        
                        # Entry condition: price retraced to or below 61.8% but above sweep low
                        if lowest_low < current_price <= fib_618_price:
                            sl = lowest_low
                            sl_pips = (current_price - sl) / self._pip_size(pair)
                            tp_pips = sl_pips * rr_target
                            tp_price = current_price + (tp_pips * self._pip_size(pair))
                            
                            return self._build_signal("BUY", pair, current_price, sl, tp_price, window_name, window_type, sl_pips, tp_pips)

        # --- CHECK SHORT SETUP ---
        if highest_idx > acc_lookback:
            acc_df_short = df.iloc[highest_idx - acc_lookback : highest_idx]
            acc_high = float(acc_df_short['high'].max())
            acc_low = float(acc_df_short['low'].min())
            
            # Sweep check: did the highest_high sweep above the accumulation high?
            if highest_high > acc_high:
                # MSS check: Did price close below a recent swing low after the sweep?
                local_low = float(acc_df_short.iloc[-5:]['low'].min()) if len(acc_df_short) >= 5 else acc_low
                
                post_sweep_df = df.iloc[highest_idx + 1:]
                mss_confirmed = any(post_sweep_df['close'] < local_low)
                
                if mss_confirmed:
                    # Find the lowest point reached after the sweep
                    post_sweep_low = float(post_sweep_df['low'].min())
                    
                    # Calculate Fib 61.8% retracement (Premium Zone)
                    swing_range = highest_high - post_sweep_low
                    if swing_range > 0:
                        # 0% is post_sweep_low, 100% is highest_high
                        # 61.8% retracement up from low:
                        fib_618_price = post_sweep_low + (swing_range * fib_level)
                        
                        # Entry condition: price retraced to or above 61.8% but below sweep high
                        if fib_618_price <= current_price < highest_high:
                            sl = highest_high
                            sl_pips = (sl - current_price) / self._pip_size(pair)
                            tp_pips = sl_pips * rr_target
                            tp_price = current_price - (tp_pips * self._pip_size(pair))
                            
                            return self._build_signal("SELL", pair, current_price, sl, tp_price, window_name, window_type, sl_pips, tp_pips)

        return None

    def _build_signal(self, direction: str, pair: str, entry: float, sl: float, tp: float, w_name: str, w_type: str, sl_pips: float, tp_pips: float) -> dict:
        ticket_id = f"MACRO-{uuid.uuid4().hex[:8].upper()}"
        
        lot_size = float(self.macro_cfg.get("fixed_lot_size", 0.04))
        
        logger.info(f"[{pair}] MACRO HYDRA SIGNAL: {direction} | Window: {w_name} ({w_type}) | Entry: {entry} | SL: {sl} | TP: {tp}")

        return {
            "ticket": ticket_id,
            "pair": pair,
            "direction": direction,
            "entry_price": round(entry, 5),
            "sl_price": round(sl, 5),
            "tp1_price": round(tp, 5),
            "tp2_price": round(tp, 5),
            "sl_pips": round(sl_pips, 1),
            "tp_pips": round(tp_pips, 1),
            "score": 90.0,
            "detected_time": datetime.now(timezone.utc).isoformat(),
            "strategy": "MACRO-HYDRA",
            "setup_type": f"{w_name} ({w_type})",
            "fixed_lot_size": lot_size
        }

    def _pip_size(self, pair: str) -> float:
        if "JPY" in pair:
            return 0.01
        elif "XAU" in pair or "XAG" in pair:
            return 0.01  
        return 0.0001
