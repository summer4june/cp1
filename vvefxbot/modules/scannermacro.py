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
        # logger.debug(f"ScannerMacro.scan called for {pair} at IST: {self.mt5.current_time()}")
        if not self.macro_cfg.get("enabled", False):
            logger.info(f"[{pair}] ScannerMacro disabled in config. macro_cfg={self.macro_cfg}")
            return None

        # If a specific list of pairs is defined for this strategy, filter by it.
        # Otherwise, if it's not defined or empty, it allows all global pairs.
        allowed_pairs = self.macro_cfg.get("pairs", [])
        if allowed_pairs and pair not in allowed_pairs:
            logger.info(f"[{pair}] not in allowed_pairs: {allowed_pairs}")
            return None

        # Use mt5 connector time to support both live (real time) and backtest (simulated time)
        now_utc = self.mt5.current_time()
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now_ist = now_utc + self._IST_OFFSET
        
        # logger.debug(f"[{pair}] now_utc: {now_utc}, now_ist: {now_ist}")
        
        active_macro = self._get_active_macro(now_ist)
        if not active_macro:
            return None
            
        logger.info(f"[{pair}] Entering active macro window: {active_macro} at IST: {now_ist.time()}")
            
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

        signal = self._detect_amd(df, pair, current_price, window_name, window_type, now_utc)
        
        if signal:
            self._last_signal_time[pair] = now_utc
            
        return signal

    def _detect_amd(self, df: pd.DataFrame, pair: str, current_price: float, window_name: str, window_type: str, now_utc: datetime) -> dict | None:
        """
        Detect Accumulation -> Manipulation -> Distribution -> Fib Entry.
        Accumulation phase is strictly bounded to start at the opening of the active Macro Window.
        """
        fib_level = self.macro_cfg.get("fib_entry_level", 0.618)
        rr_target = self.macro_cfg.get("risk_reward", 3.0)
        
        # 1. Determine the start time of the active Macro Window
        now_ist = now_utc + self._IST_OFFSET
        curr_t = now_ist.time()
        
        macro_start_t = None
        for (sh, sm, eh, em, name, wtype) in self.MACRO_WINDOWS:
            if name == window_name:
                macro_start_t = dt_time(sh, sm)
                break
                
        if not macro_start_t:
            return None
            
        macro_start_dt = now_ist.replace(hour=macro_start_t.hour, minute=macro_start_t.minute, second=0, microsecond=0)
        if macro_start_t.hour > curr_t.hour:
            macro_start_dt -= timedelta(days=1)
            
        macro_start_utc = macro_start_dt - self._IST_OFFSET
        
        # 2. Filter candles to only those inside the current macro window
        if df['time'].dt.tz is None:
            df['time'] = df['time'].dt.tz_localize('UTC')
            
        macro_df = df[df['time'] >= macro_start_utc].copy()
        if len(macro_df) < 1:
            return None
            
        macro_df = macro_df.reset_index(drop=True)
        
        # 3. Apply Pair-specific Pip Limits for Accumulation Range
        pair_upper = pair.upper()
        if any(idx in pair_upper for idx in ["US30", "USTEC", "US100", "NAS100"]):
            max_pips = float(self.macro_cfg.get("max_acc_pips_us30_ustec", 600.0))
        elif "US500" in pair_upper or "SPX" in pair_upper:
            max_pips = float(self.macro_cfg.get("max_acc_pips_us500", 60.0))
        else:
            max_pips = float(self.macro_cfg.get("max_acc_pips_fx", 20.0))
            
        # 4. Define Accumulation Range using lookback before macro window
        acc_lookback = int(self.macro_cfg.get("accumulation_lookback", 20))
        
        # Get candles before the macro window
        pre_macro_df = df[df['time'] < macro_start_utc].tail(acc_lookback)
        if len(pre_macro_df) < acc_lookback:
            # Not enough history
            return None
            
        highest_wick = float(pre_macro_df['high'].max())
        lowest_wick = float(pre_macro_df['low'].min())
        
        accumulation_broken = False
        broken_idx = -1
        break_direction = None
        break_direction = None
        # Validate accumulation range pip size
        current_range_pips = (highest_wick - lowest_wick) / self._pip_size(pair)
        if current_range_pips > max_pips:
            # logger.info(f"[{pair}] MACRO INVALID: Accumulation range {current_range_pips:.1f} pips exceeded limit {max_pips}")
            return None

        for i in range(len(macro_df)):
            row = macro_df.iloc[i]
            close = float(row['close'])
            high = float(row['high'])
            low = float(row['low'])
            
            if close > highest_wick:
                accumulation_broken = True
                broken_idx = i
                break_direction = "SHORT" # Broke above accumulation -> manipulating highs -> look to sell
                break
            elif close < lowest_wick:
                accumulation_broken = True
                broken_idx = i
                break_direction = "LONG" # Broke below accumulation -> manipulating lows -> look to buy
                break
                
        if not accumulation_broken:
            # MACRO WAIT: Accumulation not broken yet
            return None
            
        # 5. Track Manipulation & MSS Phase
        if break_direction == "LONG":
            manipulation_df = macro_df.iloc[broken_idx:]
            lowest_low = float(manipulation_df['low'].min())
            lowest_low_idx = manipulation_df['low'].idxmin()
            
            acc_df = macro_df.iloc[:broken_idx]
            local_high = float(acc_df.iloc[-5:]['high'].max()) if len(acc_df) >= 5 else highest_wick
            
            post_lowest_df = manipulation_df.loc[lowest_low_idx + 1:]
            mss_confirmed = any(post_lowest_df['close'] > local_high)
            
            if mss_confirmed:
                post_sweep_high = float(post_lowest_df['high'].max())
                swing_range = post_sweep_high - lowest_low
                if swing_range > 0:
                    fib_618_price = post_sweep_high - (swing_range * fib_level)
                    if lowest_low < current_price <= fib_618_price:
                        sl = lowest_low
                        sl_pips = (fib_618_price - sl) / self._pip_size(pair)
                        tp_pips = sl_pips * rr_target
                        tp_price = fib_618_price + (tp_pips * self._pip_size(pair))
                        
                        return self._build_signal("BUY", pair, fib_618_price, sl, tp_price, window_name, window_type, sl_pips, tp_pips, now_utc)
                    else:
                        logger.info(f"[{pair}] MACRO WAIT: LONG MSS confirmed, waiting for 618 pull back. Current: {current_price}, Fib618: {fib_618_price}")
            else:
                logger.info(f"[{pair}] MACRO WAIT: LONG Manipulation sweeping low {lowest_low} but no MSS above {local_high} yet.")
                        
        elif break_direction == "SHORT":
            manipulation_df = macro_df.iloc[broken_idx:]
            highest_high = float(manipulation_df['high'].max())
            highest_high_idx = manipulation_df['high'].idxmax()
            
            acc_df = macro_df.iloc[:broken_idx]
            local_low = float(acc_df.iloc[-5:]['low'].min()) if len(acc_df) >= 5 else lowest_wick
            
            post_highest_df = manipulation_df.loc[highest_high_idx + 1:]
            mss_confirmed = any(post_highest_df['close'] < local_low)
            
            if mss_confirmed:
                post_sweep_low = float(post_highest_df['low'].min())
                swing_range = highest_high - post_sweep_low
                if swing_range > 0:
                    fib_618_price = post_sweep_low + (swing_range * fib_level)
                    if fib_618_price <= current_price < highest_high:
                        sl = highest_high
                        sl_pips = (sl - fib_618_price) / self._pip_size(pair)
                        tp_pips = sl_pips * rr_target
                        tp_price = fib_618_price - (tp_pips * self._pip_size(pair))
                        
                        return self._build_signal("SELL", pair, fib_618_price, sl, tp_price, window_name, window_type, sl_pips, tp_pips, now_utc)
                    else:
                        logger.info(f"[{pair}] MACRO WAIT: SHORT MSS confirmed, waiting for 618 pull back. Current: {current_price}, Fib618: {fib_618_price}")
            else:
                logger.info(f"[{pair}] MACRO WAIT: SHORT Manipulation sweeping high {highest_high} but no MSS below {local_low} yet.")

        return None

    def _build_signal(self, direction: str, pair: str, entry: float, sl: float, tp: float, w_name: str, w_type: str, sl_pips: float, tp_pips: float, now_utc: datetime) -> dict:
        score = 85.0
        
        # --- Spread Adjustment to Prices ---
        spread_val = 0.0
        try:
            current_spread_pips = self.mt5.get_current_spread(pair)
            if current_spread_pips > 0:
                spread_val = current_spread_pips * self._pip_size(pair)
        except Exception:
            pass
            
        sl = sl + spread_val
        tp = tp - spread_val
        
        ticket_id = f"MACRO-{uuid.uuid4().hex[:8].upper()}"
        
        lot_size = float(self.macro_cfg.get("fixed_lot_size", 0.04))
        
        spr = self.mt5.get_current_spread(pair)
        den = sl_pips + spr
        eff_rr = (tp_pips - spr) / den if den > 0 else 0.0
        
        logger.info(f"[{pair}] MACRO HYDRA SIGNAL: {direction} | Window: {w_name} ({w_type}) | Entry: {entry} | SL: {sl} | TP: {tp}")

        return {
            "signal_id": ticket_id,
            "pair": pair,
            "direction": direction,
            "entry_price": round(entry, 5),
            "sl_price": round(sl, 5),
            "tp1_price": round(tp, 5),
            "tp2_price": round(tp, 5),
            "sl_pips": round(sl_pips, 1),
            "tp_pips": round(tp_pips, 1),
            "spread_pips": spr,
            "effective_rr": round(eff_rr, 2),
            "score": 90.0,
            "detected_time": now_utc.isoformat(),
            "strategy": "MACRO-HYDRA",
            "setup_type": f"{w_name} ({w_type})",
            "fixed_lot_size": lot_size,
            "entry_mode": "FILTER"
        }

    def _pip_size(self, pair: str) -> float:
        p = pair.upper()
        if "JPY" in p:
            return 0.01
        elif "XAU" in p or "XAG" in p:
            return 0.01  
        elif any(idx in p for idx in ["US30", "US100", "US500", "USTEC", "NAS100", "SPX", "GER40", "UK100", "WS30"]):
            return 1.0
        return 0.0001
