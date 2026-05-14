import MetaTrader5 as mt5
import pandas as pd
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from .logger import get_logger
from .configengine import Config

logger = get_logger("MT5Connector")

# Safe Retcode Mappings with numeric fallbacks for different MT5 versions
RETCODE_DONE = getattr(mt5, 'TRADE_RETCODE_DONE', 10009)
RETCODE_PLACED = getattr(mt5, 'TRADE_RETCODE_PLACED', 10008)
RETCODE_BUSY = getattr(mt5, 'TRADE_RETCODE_BUSY', 10004)
RETCODE_REQUOTE = getattr(mt5, 'TRADE_RETCODE_REQUOTE', 10006)
RETCODE_TOO_MANY_REQUESTS = getattr(mt5, 'TRADE_RETCODE_TOO_MANY_REQUESTS', 10024)
RETCODE_LOCKED = getattr(mt5, 'TRADE_RETCODE_LOCKED', 10028)
RETCODE_CONNECTION = getattr(mt5, 'TRADE_RETCODE_CONNECTION', 10031)
RETCODE_MARKET_CLOSED = getattr(mt5, 'TRADE_RETCODE_MARKET_CLOSED', 10018)

# Filling Mode Flags (for symbol_info().filling_mode)
SYM_FILLING_FOK = getattr(mt5, 'SYMBOL_FILLING_FOK', 1)
SYM_FILLING_IOC = getattr(mt5, 'SYMBOL_FILLING_IOC', 2)

# Order Filling Types (for request["type_filling"])
ORD_FILLING_FOK = getattr(mt5, 'ORDER_FILLING_FOK', 0)
ORD_FILLING_IOC = getattr(mt5, 'ORDER_FILLING_IOC', 1)
ORD_FILLING_RETURN = getattr(mt5, 'ORDER_FILLING_RETURN', 2)

# Order Types
ORD_BUY = getattr(mt5, 'ORDER_TYPE_BUY', 0)
ORD_SELL = getattr(mt5, 'ORDER_TYPE_SELL', 1)

# Trade Actions
ACTION_DEAL = getattr(mt5, 'TRADE_ACTION_DEAL', 1)
ACTION_SLTP = getattr(mt5, 'TRADE_ACTION_SLTP', 6)

# Timeframes
TF_M1 = getattr(mt5, 'TIMEFRAME_M1', 1)
TF_M15 = getattr(mt5, 'TIMEFRAME_M15', 15)
TF_H1 = getattr(mt5, 'TIMEFRAME_H1', 16385)

# Other Constants
TIME_GTC = getattr(mt5, 'ORDER_TIME_GTC', 0)

class MT5Connector:
    """Connector class for MetaTrader 5 terminal interaction."""
    
    def __init__(self, config: Config):
        """
        Initializes the MT5Connector with configuration.
        
        Args:
            config (Config): Validated configuration dataclass.
        """
        self.config = config
        self.tf_map = {
            "M1": TF_M1,
            "M15": TF_M15,
            "H1": TF_H1
        }

    def connect(self) -> bool:
        """
        Login using credentials from configuration.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        if not mt5.initialize():
            logger.error(f"MT5 initialization failed: {mt5.last_error()}")
            return False

        authorized = mt5.login(
            login=self.config.mt5_login,
            password=self.config.mt5_password,
            server=self.config.mt5_server
        )

        if authorized:
            account_info = mt5.account_info()
            logger.info(f"MT5 connected: {account_info.login if account_info else self.config.mt5_login}")
            return True
        else:
            logger.error(f"MT5 login failed: {mt5.last_error()}")
            return False

    def disconnect(self):
        """Cleanly shutdown MT5 connection."""
        mt5.shutdown()
        logger.info("MT5 connection closed.")

    def get_candles(self, symbol: str, timeframe: str, count: int = 100) -> pd.DataFrame:
        """
        Fetch candle data for a symbol and timeframe.
        
        Args:
            symbol (str): Trading pair symbol.
            timeframe (str): One of "M1", "M15", "H1".
            count (int): Number of candles to fetch.
            
        Returns:
            pd.DataFrame: Candle data or empty DataFrame on failure.
        """
        mt5_tf = self.tf_map.get(timeframe)
        if mt5_tf is None:
            logger.error(f"Invalid timeframe requested: {timeframe}")
            return pd.DataFrame()

        rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)
        
        if rates is None:
            logger.warning(f"Failed to get candles for {symbol}. Attempting one-time reconnect.")
            if self.connect():
                rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)
            
        if rates is None or len(rates) == 0:
            logger.error(f"Could not retrieve candles for {symbol} after retry.")
            return pd.DataFrame()

        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        return df[['time', 'open', 'high', 'low', 'close', 'tick_volume']]

    def get_current_spread(self, symbol: str) -> float:
        """
        Calculate current spread in pips.
        
        Args:
            symbol (str): Trading pair symbol.
            
        Returns:
            float: Spread in pips, or -1.0 on error.
        """
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            logger.error(f"Symbol info unavailable for {symbol}")
            return -1.0
        
        spread_points = symbol_info.spread
        
        # JPY pairs usually have 2 or 3 decimal places (0.01 is 1 pip)
        # Others usually have 4 or 5 (0.0001 is 1 pip)
        if "JPY" in symbol.upper():
            return spread_points * symbol_info.point / 0.01
        else:
            return spread_points * symbol_info.point / 0.0001

    def _get_filling_mode(self, symbol: str) -> int:
        """
        Detects the best supported filling mode for the given symbol.
        
        Priority: SYM_FILLING_FOK -> SYM_FILLING_IOC -> ORD_FILLING_RETURN
        """
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            return ORD_FILLING_RETURN

        # Check supported filling flags
        filling_modes = symbol_info.filling_mode
        
        if filling_modes & SYM_FILLING_FOK:
            return ORD_FILLING_FOK
        elif filling_modes & SYM_FILLING_IOC:
            return ORD_FILLING_IOC
        else:
            return ORD_FILLING_RETURN

    def get_account_balance(self) -> float:
        """
        Return current account balance.
        
        Returns:
            float: Account balance or 0.0 on error.
        """
        account_info = mt5.account_info()
        if account_info:
            return float(account_info.balance)
        logger.error("Failed to retrieve account info for balance.")
        return 0.0

    def is_connected(self) -> bool:
        """
        Check if terminal is connected.
        
        Returns:
            bool: True if connected.
        """
        terminal_info = mt5.terminal_info()
        return terminal_info.connected if terminal_info else False

    def place_order(self, symbol: str, order_type: str, lot: float, sl: float, tp: float, comment: str = "VvE") -> Dict[str, Any]:
        """
        Place a market order with SL and TP.
        
        Args:
            symbol (str): Trading pair.
            order_type (str): "BUY" or "SELL".
            lot (float): Position size.
            sl (float): Stop loss price.
            tp (float): Take profit price.
            comment (str): Order comment.
            
        Returns:
            dict: {"success": bool, "ticket": int, "error": str}
        """
        if order_type.upper() == "BUY":
            mt5_type = ORD_BUY
            tick = mt5.symbol_info_tick(symbol)
            price = tick.ask if tick else 0.0
        elif order_type.upper() == "SELL":
            mt5_type = ORD_SELL
            tick = mt5.symbol_info_tick(symbol)
            price = tick.bid if tick else 0.0
        else:
            return {"success": False, "ticket": -1, "error": f"Invalid order type: {order_type}"}

        request = {
            "action": ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": mt5_type,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": 20,
            "magic": 123456,
            "comment": comment,
            "type_time": TIME_GTC,
            "type_filling": self._get_filling_mode(symbol),
        }

        retries = 3
        for i in range(retries):
            result = mt5.order_send(request)
            if result is None:
                err_msg = f"order_send returned None. MT5 Error: {mt5.last_error()}"
                logger.error(err_msg)
                return {"success": False, "ticket": -1, "error": err_msg}

            if result.retcode in [RETCODE_DONE, RETCODE_PLACED]:
                logger.info(f"Order placed: {symbol} {order_type} {lot} @ {result.price}, Ticket: {result.order}")
                return {"success": True, "ticket": result.order, "error": ""}
            
            # Retry on busy/requote/too many requests/locked
            if result.retcode in [RETCODE_REQUOTE, RETCODE_BUSY, RETCODE_TOO_MANY_REQUESTS, RETCODE_LOCKED]:
                logger.warning(f"Order retry {i+1}/{retries} due to retcode: {result.retcode} ({result.comment})")
                time.sleep(1.0)
                # Update price for next attempt
                tick = mt5.symbol_info_tick(symbol)
                if tick:
                    request["price"] = tick.ask if order_type.upper() == "BUY" else tick.bid
                continue
            else:
                logger.error(f"Order failed: {result.retcode} - {result.comment}")
                return {"success": False, "ticket": -1, "error": str(result.comment)}

        return {"success": False, "ticket": -1, "error": "Max retries exceeded"}

    def close_partial(self, ticket: int, lot: float) -> Dict[str, str]:
        """
        Close a partial lot of an open position.
        
        Args:
            ticket (int): Order ticket (position ID).
            lot (float): Lot size to close.
            
        Returns:
            dict: {"success": bool, "error": str}
        """
        position = mt5.positions_get(ticket=ticket)
        if not position:
            return {"success": False, "error": "Position not found"}
        
        pos = position[0]
        symbol = pos.symbol
        order_type = ORD_SELL if pos.type == ORD_BUY else ORD_BUY
        tick = mt5.symbol_info_tick(symbol)
        price = tick.bid if tick and pos.type == ORD_BUY else (tick.ask if tick else 0.0)

        request = {
            "action": ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": order_type,
            "position": ticket,
            "price": price,
            "deviation": 20,
            "magic": 123456,
            "comment": "Close Partial",
            "type_time": TIME_GTC,
            "type_filling": self._get_filling_mode(symbol),
        }

        result = mt5.order_send(request)
        if result and result.retcode == RETCODE_DONE:
            logger.info(f"Partial close successful for ticket {ticket}")
            return {"success": True, "error": ""}
        
        err_msg = f"Partial close failed: {result.comment if result else 'Unknown error'}"
        logger.error(err_msg)
        return {"success": False, "error": err_msg}

    def modify_sl(self, ticket: int, new_sl: float) -> Dict[str, str]:
        """
        Modify Stop Loss of an open position.
        
        Args:
            ticket (int): Order ticket (position ID).
            new_sl (float): New stop loss price.
            
        Returns:
            dict: {"success": bool, "error": str}
        """
        position = mt5.positions_get(ticket=ticket)
        if not position:
            return {"success": False, "error": "Position not found"}
        
        pos = position[0]
        request = {
            "action": ACTION_SLTP,
            "symbol": pos.symbol,
            "sl": new_sl,
            "tp": pos.tp,
            "position": ticket,
        }

        result = mt5.order_send(request)
        if result and result.retcode == RETCODE_DONE:
            logger.info(f"SL modified for ticket {ticket} to {new_sl}")
            return {"success": True, "error": ""}
        
        err_msg = f"SL modification failed: {result.comment if result else 'Unknown error'}"
        logger.error(err_msg)
        return {"success": False, "error": err_msg}
