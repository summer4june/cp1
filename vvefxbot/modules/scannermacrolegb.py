"""
scannermacrolegb.py — ICT Hydra Entry Model (Macro Leg B Strategy)
"""
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine

from modules.smrengine import SMREngine, TradeDirection

logger = get_logger("ScannerMacroLegB")

class ScannerMacroLegB:
    """
    Scanner for the ICT Hydra Leg B Model.
    Tracks session highs/lows and enters on Retest of displaced OB between 19:00 - 19:30 IST.
    """

    _IST_OFFSET = timedelta(hours=5, minutes=30)

    def __init__(self, config: Config, mt5: MT5Connector, state: StateEngine):
        self.config = config
        self.mt5 = mt5
        self.state = state
        self.macro_cfg = getattr(config, "macro_leg_b_strategy", {})
        self.engines = {}
        self._last_signal_time = {}
        self.traded_days = set()

    def _pip_size(self, pair: str) -> float:
        p = pair.upper()
        if "JPY" in p: return 0.01
        if "XAU" in p or "XAG" in p: return 0.01
        if any(x in p for x in ["US500", "SPX"]): return 0.1
        if any(x in p for x in ["US30", "WS30", "GER40", "UK100"]): return 1.0
        if any(x in p for x in ["USTEC", "US100", "NAS100"]): return 0.1
        return 0.0001

    def scan(self, pair: str, session: str = None, killzone: str = None) -> dict | None:
        if not self.config.enabled_scanners.get("macro_leg_b", False):
            return None

        allowed_pairs = self.macro_cfg.get("pairs", [])
        if allowed_pairs and pair not in allowed_pairs:
            return None

        now_utc = self.mt5.current_time()
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now_ist = now_utc + self._IST_OFFSET
        
        # Fast exit if not in window, although engine tracks 24/7.
        # But we must feed it candles 24/7 to build session high/lows!
        # Actually, if we only feed it 24/7 we are doing a lot of work. We can just fetch 500 candles (covers a day)
        # when we are near the window to build state, or just fetch 288 M5 candles (1 day).
        
        # ── Guard: already traded today ──
        # Since strategy is strict: 19:00-19:30. We track by day.
        # Shift date back by 12 hours so the trading day is stable across midnight.
        logical_date = (now_ist - timedelta(hours=12)).strftime("%Y-%m-%d")
        check_key = f"{pair}_{logical_date}"

        if check_key in self.traded_days:
            # We don't need to spam logs, this will be called a lot.
            return None
        
        # Cooldown check
        last_time = self._last_signal_time.get(pair)
        if last_time and (now_utc - last_time).total_seconds() < 300:
            return None

        # Get 400 M5 candles (about 1.3 days) to build session high/lows reliably
        df = self.mt5.get_candles(pair, "M5", count=400)
        if df is None or len(df) < 100:
            return None
            
        df = df.reset_index(drop=True)
        current_price = self.mt5.get_current_bid(pair)
        if not current_price:
            return None

        # Rebuild engine state from recent history
        engine = SMREngine(pair)
        
        for i in range(len(df)):
            row = df.iloc[i]
            c_time_utc = row['time']
            engine.process_candle(
                dt_utc=c_time_utc,
                open_p=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close'])
            )

        signal_obj = engine.consume_signal()

        if not signal_obj:
            return None
            
        # Ensure signal is fresh
        last_candle_time = df.iloc[-1]['time']
        if last_candle_time.tzinfo is None:
            last_candle_time = last_candle_time.tz_localize('UTC')
            
        sig_time = signal_obj.timestamp
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)
            
        if abs((last_candle_time - sig_time).total_seconds()) > 600:
            logger.debug(f"[{pair}] MACRO LEG B SIGNAL REJECTED: Not fresh.")
            return None

        # Fib Entry Calculation (Leg B can optionally use fib, or direct OB. We'll use 50% OB as entry by default if fib is missing)
        fib_level = self.macro_cfg.get("fib_entry_level", 0.5)
        rr_target = self.macro_cfg.get("risk_reward", 3.0)
        
        entry_price = 0.0
        sl_price = 0.0
        direction_str = ""
        is_above_market = False
        
        if signal_obj.direction == TradeDirection.LONG:
            direction_str = "BUY"
            lowest_low = signal_obj.sweep_level
            # OB high to low is the zone. We enter at fib of that zone.
            zone_size = signal_obj.ob_high - signal_obj.ob_low
            entry_price = signal_obj.ob_high - (zone_size * fib_level)
            sl_buffer = self._pip_size(pair) * 2
            sl_price = lowest_low - sl_buffer
            is_above_market = entry_price < current_price
        else:
            direction_str = "SELL"
            highest_high = signal_obj.sweep_level
            zone_size = signal_obj.ob_high - signal_obj.ob_low
            entry_price = signal_obj.ob_low + (zone_size * fib_level)
            sl_buffer = self._pip_size(pair) * 2
            sl_price = highest_high + sl_buffer
            is_above_market = entry_price > current_price

        # Basic risk checks
        sl_dist = abs(entry_price - sl_price)
        min_sl = self._pip_size(pair) * 5
        if sl_dist < min_sl:
            sl_dist = min_sl
            if direction_str == "BUY": sl_price = entry_price - sl_dist
            else: sl_price = entry_price + sl_dist

        tp3_dist = sl_dist * rr_target
        tp1_price = entry_price + tp3_dist/3 if direction_str == "BUY" else entry_price - tp3_dist/3
        tp2_price = entry_price + (tp3_dist * 2/3) if direction_str == "BUY" else entry_price - (tp3_dist * 2/3)
        tp3_price = entry_price + tp3_dist if direction_str == "BUY" else entry_price - tp3_dist

        sig_id = str(uuid.uuid4())[:8]
        self._last_signal_time[pair] = now_utc
        # Note: traded_days is updated ONLY after signal dict is built to avoid
        # locking out the pair if something goes wrong downstream

        pip = self._pip_size(pair)
        sl_pips_val = round(sl_dist / pip, 1) if pip > 0 else 0.0
        spread_pips = self.mt5.get_current_spread(pair) or 0.0

        logger.info(
            f"[{pair}] \ud83d\udc0a HYDRA LEG B SMR SIGNAL: {direction_str} Limit at {entry_price:.5f} | "
            f"Sweep: {signal_obj.session_swept} {signal_obj.sweep_type} | SL: {sl_price:.5f} | TP3: {tp3_price:.5f}"
        )

        signal_dict = {
            "signal_id": sig_id,
            "pair": pair,
            "direction": direction_str,
            "entry_price": round(entry_price, 5),
            "sl_price": round(sl_price, 5),
            "tp1_price": round(tp1_price, 5),
            "tp2_price": round(tp2_price, 5),
            "tp3_price": round(tp3_price, 5),
            "sl_pips": sl_pips_val,
            "spread_pips": spread_pips,
            "is_above_market": is_above_market,
            "session": "MACRO_LEGB",
            "killzone": "MACRO_LEGB",
            "entry_leg": "B",
            "setup_type": "Macro_LegB",
            "strategy": "MACRO_LEG_B",
            "score": 100,
            "detected_time": now_utc.isoformat(),
            # Leg B fields required by Google Sheets
            "swing_low": signal_obj.sweep_level if signal_obj.sweep_type == "LOW" else signal_obj.mss_level,
            "swing_high": signal_obj.sweep_level if signal_obj.sweep_type == "HIGH" else signal_obj.mss_level,
            "ob_low": signal_obj.ob_low,
            "ob_high": signal_obj.ob_high,
            "timeframe_entry": "M5",
        }
        self.traded_days.add(check_key)
        return signal_dict
