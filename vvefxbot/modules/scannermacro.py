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

from modules.accumulationengine import AccumulationEngine, TradeDirection

logger = get_logger("ScannerMacro")

class ScannerMacro:
    """
    Scanner for the ICT Hydra Entry Model (Macro Strategy).
    Runs strictly on M1 timeframe during specific IST macro windows.
    Detects Accumulation -> Manipulation (Sweep) -> Distribution (MSS) -> 61.8% Fib Entry.
    Utilizes the standalone AccumulationEngine for state tracking.
    """

    _IST_OFFSET = timedelta(hours=5, minutes=30)

    # Macro Windows (IST)
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

    # Paired windows: manipulation → its continuation partner (by name)
    _MANIP_TO_CONTINUATION = {
        "Macro 1":        "Macro 2",
        "Macro 3":        "Macro 4",
        "Silver Bullet 5": "Silver Bullet 6",
        "Reversal 7":     "Reversal 8",
        "Reversal 9":     "Reversal 10",
    }

    def __init__(self, config: Config, mt5: MT5Connector, state: StateEngine):
        self.config = config
        self.mt5 = mt5
        self.state = state
        self.macro_cfg = getattr(config, "macro_strategy", {})
        self._last_signal_time = {}
        self.traded_macros = set()

    def _get_active_macro(self, current_dt_ist: datetime):
        """Returns (window_name, window_type, end_t) if currently in a macro window, else None."""
        curr_t = current_dt_ist.time()
        
        for i, (sh, sm, eh, em, name, wtype) in enumerate(self.MACRO_WINDOWS):
            start_t = dt_time(sh, sm)
            end_t = dt_time(eh, em)
            
            in_window = False
            if start_t > end_t:
                if curr_t >= start_t or curr_t < end_t:
                    in_window = True
            else:
                if start_t <= curr_t < end_t:
                    in_window = True
                    
            if in_window:
                expiration_t = end_t
                if wtype == "Manipulation" and i + 1 < len(self.MACRO_WINDOWS):
                    _, _, nx_eh, nx_em, _, nx_wtype = self.MACRO_WINDOWS[i+1]
                    if nx_wtype == "Continuation":
                        expiration_t = dt_time(nx_eh, nx_em)
                return name, wtype, expiration_t, start_t
                    
        return None

    def _get_manip_window_start(self, window_name: str) -> dt_time:
        """
        Return the IST start time of the MANIPULATION window that owns this
        window (for continuation windows, returns the paired manipulation start).
        """
        # Build reverse map: continuation -> manipulation start
        for i, (sh, sm, eh, em, name, wtype) in enumerate(self.MACRO_WINDOWS):
            if name == window_name:
                if wtype == "Manipulation":
                    return dt_time(sh, sm)
                # Continuation: find the preceding manipulation window
                if i > 0:
                    psh, psm = self.MACRO_WINDOWS[i-1][0], self.MACRO_WINDOWS[i-1][1]
                    return dt_time(psh, psm)
                return dt_time(sh, sm)
        return dt_time(0, 0)

    def scan(self, pair: str, session: str = None, killzone: str = None) -> dict | None:
        logger.debug(f"[{pair}] MACRO: scan() called. Enabled={self.macro_cfg.get('enabled')} Pairs={self.macro_cfg.get('pairs')}")
        if not self.macro_cfg.get("enabled", False):
            return None

        allowed_pairs = self.macro_cfg.get("pairs", [])
        if allowed_pairs and pair not in allowed_pairs:
            return None

        now_utc = self.mt5.current_time()
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now_ist = now_utc + self._IST_OFFSET
        
        active_macro = self._get_active_macro(now_ist)
        if not active_macro:
            return None
            
        window_name, window_type, end_t_ist, window_start_t = active_macro
        logger.debug(f"[{pair}] MACRO: Scan started | Window={window_name} ({window_type})")
        
        session_name = "NewYork Open"
        if "Reversal" in window_name:
            session_name = "London Close"

        # ── Guard: already traded this specific named window (or its manipulation partner) ──
        # Each manipulation+continuation pair shares the manipulation window's name as the key.
        # e.g. If Macro 1 (Manipulation) fired, block Macro 2 (Continuation) with same key.
        # Shift date by 4 hours to handle macros that cross midnight (e.g., Reversal 9 & 10)
        logical_date = (now_ist - timedelta(hours=4)).strftime("%Y-%m-%d")
        if window_type == "Continuation":
            # Find the paired manipulation window name
            manip_name = None
            for i, (sh, sm, eh, em, name, wtype) in enumerate(self.MACRO_WINDOWS):
                if name == window_name and i > 0:
                    manip_name = self.MACRO_WINDOWS[i-1][4]
                    break
            check_key = f"{pair}_{logical_date}_{manip_name or window_name}"
        else:
            check_key = f"{pair}_{logical_date}_{window_name}"

        if check_key in self.traded_macros:
            logger.debug(f"[{pair}] MACRO: Already traded window '{window_name}' today — skipping.")
            return None
        
        # Cooldown check
        last_time = self._last_signal_time.get(pair)
        if last_time and (now_utc - last_time).total_seconds() < 300:
            return None

        df = self.mt5.get_candles(pair, "M1", count=150)
        if df is None or len(df) < 50:
            return None
            
        df = df.reset_index(drop=True)
        current_price = self.mt5.get_current_bid(pair)
        if not current_price:
            return None

        # ── Restrict candle feed to the current macro pair's manipulation window start ──
        # This prevents setups from PRIOR macro windows (e.g. Macro 1 accumulation)
        # from producing a signal during a LATER window (e.g. Macro 3 or Macro 4).
        # The manipulation window start is the earliest valid candle for this setup.
        manip_start_t = self._get_manip_window_start(window_name)

        # Process candles through the state machine — only from the manipulation window start
        engine = AccumulationEngine(pair, config=self.macro_cfg)
        
        for i in range(len(df)):
            row = df.iloc[i]
            c_time_utc = row['time']
            if c_time_utc.tzinfo is None:
                c_time_utc = c_time_utc.tz_localize('UTC')
            c_time_ist = c_time_utc + self._IST_OFFSET

            # Skip bars that occurred before this macro pair's manipulation window started.
            # This ensures the AccumulationEngine only sees candles relevant to the current setup.
            if c_time_ist.time() < manip_start_t:
                continue

            active_c_macro = self._get_active_macro(c_time_ist)
            c_in_macro = active_c_macro is not None
            c_window_name = active_c_macro[0] if active_c_macro else ""
            
            engine.process_candle(
                float(row['open']), float(row['high']), float(row['low']), float(row['close']), c_time_utc, c_in_macro, c_window_name
            )

        signal_obj = engine.consume_signal(window_name)

        # ── Extra guard: reject if signal was detected outside the current macro window pair ──
        # e.g. if the signal's timestamp falls in Macro 1's window but we are now in Macro 3
        if signal_obj and signal_obj.timestamp:
            sig_time_utc = signal_obj.timestamp
            if sig_time_utc.tzinfo is None:
                sig_time_utc = sig_time_utc.replace(tzinfo=timezone.utc)
            sig_time_ist = sig_time_utc + self._IST_OFFSET
            sig_macro = self._get_active_macro(sig_time_ist)
            if sig_macro:
                sig_window_name = sig_macro[0]
                # The signal's window must be the current window or its paired manipulation partner
                allowed_windows = {window_name}
                if window_type == "Continuation":
                    # Also allow signal from this window's paired manipulation window
                    for idx, (sh, sm, eh, em, name, wtype) in enumerate(self.MACRO_WINDOWS):
                        if name == window_name and idx > 0:
                            allowed_windows.add(self.MACRO_WINDOWS[idx-1][4])
                            break
                elif window_type == "Manipulation":
                    # Also allow signal detected during this manipulation window itself
                    cont_name = self._MANIP_TO_CONTINUATION.get(window_name)
                    if cont_name:
                        allowed_windows.add(cont_name)

                if sig_window_name not in allowed_windows:
                    logger.info(
                        f"[{pair}] MACRO REJECTED: Signal from '{sig_window_name}' window "
                        f"does not match current active window '{window_name}'. Skipping."
                    )
                    signal_obj = None
        if not signal_obj:
            return None
            
        # Ensure signal is fresh
        last_candle_time = df.iloc[-1]['time']
        if last_candle_time.tzinfo is None:
            last_candle_time = last_candle_time.tz_localize('UTC')
            
        sig_time = signal_obj.timestamp
        if sig_time.tzinfo is None:
            sig_time = sig_time.replace(tzinfo=timezone.utc)
            
        if abs((last_candle_time - sig_time).total_seconds()) > 1800:
            logger.debug(f"[{pair}] MACRO SIGNAL REJECTED: Not fresh. Signal time: {sig_time}, Current: {last_candle_time}")
            return None

        # Fib Entry Calculation
        fib_level = self.macro_cfg.get("fib_entry_level", 0.618)
        rr_target = self.macro_cfg.get("risk_reward", 3.0)
        
        entry_price = 0.0
        sl_price = 0.0
        direction_str = ""
        # is_above_market: True = Sell Limit (entry above current price, needs price to retrace UP)
        #                  False = Buy Limit (entry below current price, needs price to retrace DOWN)
        is_above_market = False
        
        if signal_obj.direction == TradeDirection.LONG:
            direction_str = "BUY"
            lowest_low = signal_obj.manipulation_sweep_level
            swing_range = signal_obj.mss_level - lowest_low
            fib_618 = signal_obj.mss_level - (swing_range * fib_level)
            
            entry_price = fib_618
            sl_price = lowest_low - (self._get_sl_buffer_pips(pair) * self._pip_size(pair))
            # BUY Limit: entry is BELOW current price — price retraces down to fill us
            is_above_market = False
            
            if current_price <= fib_618:
                # Price already at or past our entry — fill at market, not as a limit
                logger.info(f"[{pair}] MACRO INSTANT FILL: LONG price {current_price} already at/below 618 Fib {fib_618:.5f}")
                is_above_market = False  # Market fill territory
                
        elif signal_obj.direction == TradeDirection.SHORT:
            direction_str = "SELL"
            highest_high = signal_obj.manipulation_sweep_level
            swing_range = highest_high - signal_obj.mss_level
            fib_618 = signal_obj.mss_level + (swing_range * fib_level)
            
            entry_price = fib_618
            sl_price = highest_high + (self._get_sl_buffer_pips(pair) * self._pip_size(pair))
            # SELL Limit: entry is ABOVE current price — price retraces up to fill us
            is_above_market = True
            
            if current_price >= fib_618:
                # Price already at or past our entry — fill at market, not as a limit
                logger.info(f"[{pair}] MACRO INSTANT FILL: SHORT price {current_price} already at/above 618 Fib {fib_618:.5f}")
                is_above_market = True   # Still above-market for the engine's trigger logic

        sl_pips = abs(entry_price - sl_price) / self._pip_size(pair)
        if sl_pips <= 0:
            return None
            
        tp_pips = sl_pips * rr_target
        tp_price = entry_price + (tp_pips * self._pip_size(pair)) if direction_str == "BUY" else entry_price - (tp_pips * self._pip_size(pair))
        
        # Collect swing and OB levels for reporting
        swing_high = signal_obj.mss_level if signal_obj.direction == TradeDirection.LONG else signal_obj.manipulation_sweep_level
        swing_low  = signal_obj.manipulation_sweep_level if signal_obj.direction == TradeDirection.LONG else signal_obj.mss_level
        ob_high    = entry_price + (self._get_sl_buffer_pips(pair) * self._pip_size(pair))
        ob_low     = sl_price

        signal = self._build_signal(
            direction_str, pair, entry_price, sl_price, tp_price,
            window_name, window_type, sl_pips, tp_pips, now_utc, end_t_ist, signal_obj.priority,
            is_above_market=is_above_market,
            swing_high=swing_high, swing_low=swing_low, ob_high=ob_high, ob_low=ob_low
        )
        
        if signal:
            self._last_signal_time[pair] = now_utc

            # Mark BOTH the manipulation window key AND its continuation as traded,
            # so neither window can fire again for this pair today.
            logical_date = (now_ist - timedelta(hours=4)).strftime("%Y-%m-%d")
            if window_type == "Continuation":
                # Find paired manipulation name
                manip_name = window_name
                for idx, (sh, sm, eh, em, name, wtype) in enumerate(self.MACRO_WINDOWS):
                    if name == window_name and idx > 0:
                        manip_name = self.MACRO_WINDOWS[idx-1][4]
                        break
                self.traded_macros.add(f"{pair}_{logical_date}_{manip_name}")
            else:
                self.traded_macros.add(f"{pair}_{logical_date}_{window_name}")
            
        return signal

    def _get_sl_buffer_pips(self, pair: str) -> float:
        pair_upper = pair.upper()
        if "US500" in pair_upper or "SPX" in pair_upper:
            return float(self.macro_cfg.get("sl_buffer_pips_us500", 5.0))
        if any(idx in pair_upper for idx in ["US30", "WS30"]):
            return float(self.macro_cfg.get("sl_buffer_pips_us30", 100.0))
        if any(idx in pair_upper for idx in ["USTEC", "US100", "NAS100", "UK100", "GER40"]):
            return float(self.macro_cfg.get("sl_buffer_pips_nasdaq", 10.0))
        return float(self.macro_cfg.get("sl_buffer_pips_fx", 0.0))

    def _build_signal(self, direction, pair, entry, sl, tp, window_name, window_type, sl_pips, tp_pips, now_utc, end_t_ist, priority, is_above_market=False, swing_high=None, swing_low=None, ob_high=None, ob_low=None):
        score = 90.0
        
        spread_val = 0.0
        try:
            current_spread_pips = self.mt5.get_current_spread(pair)
            if current_spread_pips > 0:
                spread_val = current_spread_pips * self._pip_size(pair)
        except Exception:
            pass
            
        sl = sl + spread_val if direction == "BUY" else sl - spread_val
        tp = tp - spread_val if direction == "BUY" else tp + spread_val
        
        ticket_id = f"MACRO-{uuid.uuid4().hex[:8].upper()}"
        
        spr = self.mt5.get_current_spread(pair) or 0.0
        den = sl_pips + spr
        eff_rr = (tp_pips - spr) / den if den > 0 else 0.0
        
        now_ist = now_utc + self._IST_OFFSET
        end_dt_ist = datetime.combine(now_ist.date(), end_t_ist)
        end_dt_ist = end_dt_ist.replace(tzinfo=timezone.utc)
        
        if end_dt_ist.time() < now_ist.time():
            end_dt_ist += timedelta(days=1)
        
        expiration_utc = end_dt_ist - self._IST_OFFSET
        
        rr = self.macro_cfg.get("risk_reward", 3.0)
        pip = self._pip_size(pair)
        
        tp1_pips = sl_pips * 1.0
        tp2_pips = sl_pips * 2.0
        tp3_pips = sl_pips * rr
        
        if direction == "BUY":
            tp1_price = entry + (tp1_pips * pip)
            tp2_price = entry + (tp2_pips * pip)
            tp3_price = entry + (tp3_pips * pip)
        else:
            tp1_price = entry - (tp1_pips * pip)
            tp2_price = entry - (tp2_pips * pip)
            tp3_price = entry - (tp3_pips * pip)
        
        session_name = "NewYork Open"
        if "Reversal" in window_name:
            session_name = "London Close"
        
        logger.info(f"[{pair}] MACRO HYDRA SIGNAL: {direction} | Window: {window_name} ({window_type}) | Entry: {entry} | SL: {sl} | TP1: {tp1_price} | TP2: {tp2_price} | TP3: {tp3_price} | Expires: {expiration_utc.strftime('%H:%M:%S')} UTC")

        lot_size = self.macro_cfg.get("fixed_lot_size", 0.04)

        return {
            "signal_id": ticket_id,
            "pair": pair,
            "direction": direction,
            "entry_price": round(entry, 5),
            "sl_price": round(sl, 5),
            "tp_price": round(tp3_price, 5),
            "tp1_price": round(tp1_price, 5),
            "tp2_price": round(tp2_price, 5),
            "tp3_price": round(tp3_price, 5),
            "sl_pips": round(sl_pips, 1),
            "tp_pips": round(tp3_pips, 1),
            "tp1_pips": round(tp1_pips, 1),
            "tp2_pips": round(tp2_pips, 1),
            "tp3_pips": round(tp3_pips, 1),
            "spread_pips": spr,
            "effective_rr": round(eff_rr, 2),
            "score": score,
            "strategy": "MACRO",
            "setup_type": f"{window_name} ({window_type})",
            "window_name": window_name,
            "window_type": window_type,
            "session": session_name,
            "killzone": session_name,
            "entry_leg": "MACRO",
            "timeframe_entry": "M1",
            "timestamp": now_utc.isoformat(),
            "detected_time": now_utc.isoformat(),
            "fixed_lot_size": lot_size,
            "entry_mode": "FILTER",
            "expiration_time": expiration_utc.isoformat(),
            "is_above_market": is_above_market,
            "priority": priority,
            # Swing / OB levels for Google Sheet reporting
            "swing_high": swing_high,
            "swing_low": swing_low,
            "ob_high": ob_high,
            "ob_low": ob_low,
        }

    def _pip_size(self, pair: str) -> float:
        p = pair.upper()
        if "JPY" in p:
            return 0.01
        elif "XAU" in p or "XAG" in p:
            return 0.01  
        elif any(idx in p for idx in ["US500", "SPX", "USTEC", "US100", "NAS100"]):
            return 0.1
        elif any(idx in p for idx in ["US30", "GER40", "UK100", "WS30"]):
            return 1.0
        return 0.0001
