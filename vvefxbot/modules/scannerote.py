import uuid
import pandas as pd
from datetime import datetime, timezone
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine

logger = get_logger("ScannerOTE")

class ScannerOTE:
    """
    Scanner for the OTE (Optimal Trade Entry) strategy.
    Runs on new M5 bars, filters trend with H1 EMA(50), 
    and checks if current price is in the 0.618 - 0.705 fib zone.
    """
    
    def __init__(self, config: Config, mt5: MT5Connector, state: StateEngine):
        self.config = config
        self.mt5 = mt5
        self.state = state
        self.last_bar_time = {}  # Tracks the last processed bar time per pair

    def is_new_bar(self, pair: str, tf: str) -> bool:
        """Checks if a new bar has opened on the given timeframe."""
        candles = self.mt5.get_candles(pair, tf, count=1)
        if candles.empty:
            return False
            
        current_time = candles.iloc[-1]['time']
        last_time = self.last_bar_time.get(pair)
        
        if last_time == current_time:
            return False
            
        self.last_bar_time[pair] = current_time
        return True

    def scan(self, pair: str, session: str, killzone: str) -> dict:
        """
        Executes the OTE scan logic.
        """
        logger.debug(f"[{pair}] Starting OTE scan | Session: {session} | KZ: {killzone}")
        # We assume config.ote_scanner exists since configengine will parse it
        ote_cfg = getattr(self.config, "ote_scanner", {})
        if not ote_cfg.get("enabled", False):
            return None

        tf_trigger = ote_cfg.get("timeframe_trigger", "M5")
        
        # 1. Check for new M5 bar
        if ote_cfg.get("use_new_bar_only", True):
            if not self.is_new_bar(pair, tf_trigger):
                return None
            logger.debug(f"[{pair}] New {tf_trigger} bar detected.")
                
        tf_signal = ote_cfg.get("timeframe_signal", "H1")
        fetch_count = ote_cfg.get("fetch_h1_candles", 100)
        min_candles = ote_cfg.get("min_h1_candles", 60)
        
        # 2. Fetch H1 candles
        candles = self.mt5.get_candles(pair, tf_signal, count=fetch_count)
        if len(candles) < min_candles:
            logger.debug(f"[{pair}] Insufficient {tf_signal} candles: {len(candles)}/{min_candles}")
            return None
            
        # Reverse to make index 0 the most recent completed bar (equivalent to ArraySetAsSeries)
        df = candles.iloc[::-1].reset_index(drop=True)
        
        # 3. Verify enough candles exist for EMA
        ema_period = ote_cfg.get("ema_period", 50)
        if len(df) < ema_period + 1:
            return None
            
        # 4. Calculate simple average of last `ema_period` closes (from index 1 to ema_period)
        closes = df.loc[1:ema_period, 'close']
        ema = closes.mean()
        
        close_1 = df.loc[1, 'close']
        
        # 5. Determine trend direction
        trend = 0
        if close_1 > ema:
            trend = 1
        elif close_1 < ema:
            trend = -1
            
        if trend == 0:
            logger.debug(f"[{pair}] No trend detected (close == ema).")
            return None
            
        if trend == 1 and not ote_cfg.get("allow_buy", True):
            logger.debug(f"[{pair}] Uptrend detected but allow_buy is False.")
            return None
        if trend == -1 and not ote_cfg.get("allow_sell", True):
            logger.debug(f"[{pair}] Downtrend detected but allow_sell is False.")
            return None
            
        direction_str = "BULLISH" if trend == 1 else "BEARISH"
        logger.debug(f"[{pair}] Trend: {direction_str} (Close: {close_1:.5f}, EMA({ema_period}): {ema:.5f})")
        
        # 6. Compute high/low from H1 bars range
        start_idx = ote_cfg.get("range_start_index", 5)
        end_idx = ote_cfg.get("range_end_index", 44)
        
        if len(df) <= end_idx:
            return None
            
        # Python ranges are start inclusive, end exclusive, but loc is inclusive.
        # MQL: for(int i=5; i<45; i++), so indices 5 to 44.
        range_slice = df.loc[start_idx:end_idx]
        high = range_slice['high'].max()
        low = range_slice['low'].min()
        
        if high == 0 or low == 0:
            logger.debug(f"[{pair}] Invalid range data (high/low = 0).")
            return None
            
        range_val = high - low
        if range_val <= 0:
            logger.debug(f"[{pair}] Range value <= 0: {range_val}")
            return None
            
        logger.debug(f"[{pair}] Range High: {high:.5f}, Low: {low:.5f}, Diff: {range_val:.5f}")
        # Get current price (using latest close from trigger timeframe)
        trigger_candles = self.mt5.get_candles(pair, tf_trigger, count=1)
        if trigger_candles.empty:
            return None
        current_price = trigger_candles.iloc[-1]['close']
        
        # 7. Compute fib position
        fib = (current_price - low) / range_val
        
        # 8. Check if inside OTE zone
        fib_min = ote_cfg.get("fib_min", 0.618)
        fib_max = ote_cfg.get("fib_max", 0.705)
        
        logger.debug(f"[{pair}] Current Price: {current_price:.5f} | Fib Retracement: {fib:.3f}")
        
        if not (fib_min <= fib <= fib_max):
            return None
            
        # 9 & 10. Direction
        direction = "BUY" if trend == 1 else "SELL"
        
        # 11. Build signal with SL/TP points
        sl_points = ote_cfg.get("sl_points", 150)
        tp_points = ote_cfg.get("tp_points", 450)
        
        # Convert points to pips (10 points = 1 pip)
        sl_pips = sl_points / 10.0
        tp_pips = tp_points / 10.0
        
        # Convert points to price difference
        point = 0.001 if "JPY" in pair else 0.00001
        sl_diff = sl_points * point
        tp_diff = tp_points * point
        
        sl_price = current_price - sl_diff if direction == "BUY" else current_price + sl_diff
        tp_price = current_price + tp_diff if direction == "BUY" else current_price - tp_diff
        
        spread_pips = self.mt5.get_current_spread(pair)
        
        # Build the standard signal dictionary
        signal_id = str(uuid.uuid4())
        score = ote_cfg.get("score", 65.0)
        
        logger.info(f"[{pair}] OTE Signal Found! {direction} | Fib: {fib:.3f} | Score: {score}")
        
        return {
            "signal_id": signal_id,
            "pair": pair,
            "session": session,
            "timeframe_bias": tf_signal,
            "timeframe_entry": tf_trigger,
            "direction": direction,
            "bias_summary": f"OTE (Fib: {fib:.3f})",
            "entry_price": current_price,
            "sl_price": sl_price,
            "tp1_price": tp_price,
            "tp2_price": tp_price,
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "spread_pips": spread_pips,
            "effective_rr": tp_pips / sl_pips if sl_pips > 0 else 0,
            "score": score,
            "detected_time": datetime.now(timezone.utc).isoformat(),
            "strategy": "OTE"
        }
