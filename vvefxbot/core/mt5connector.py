try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
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
ORD_BUY_LIMIT = getattr(mt5, 'ORDER_TYPE_BUY_LIMIT', 2)
ORD_SELL_LIMIT = getattr(mt5, 'ORDER_TYPE_SELL_LIMIT', 3)
ORD_BUY_STOP = getattr(mt5, 'ORDER_TYPE_BUY_STOP', 4)
ORD_SELL_STOP = getattr(mt5, 'ORDER_TYPE_SELL_STOP', 5)

# Trade Actions
ACTION_DEAL = getattr(mt5, 'TRADE_ACTION_DEAL', 1)
ACTION_PENDING = getattr(mt5, 'TRADE_ACTION_PENDING', 5)
ACTION_SLTP = getattr(mt5, 'TRADE_ACTION_SLTP', 6)
ACTION_REMOVE = getattr(mt5, 'TRADE_ACTION_REMOVE', 8)

# Timeframes
TF_M1  = getattr(mt5, 'TIMEFRAME_M1',  1)
TF_M5  = getattr(mt5, 'TIMEFRAME_M5',  5)
TF_M15 = getattr(mt5, 'TIMEFRAME_M15', 15)
TF_H1  = getattr(mt5, 'TIMEFRAME_H1',  16385)
TF_H4  = getattr(mt5, 'TIMEFRAME_H4',  16388)
TF_D1  = getattr(mt5, 'TIMEFRAME_D1',  16408)

# Other Constants
TIME_GTC = getattr(mt5, 'ORDER_TIME_GTC', 0)
TIME_SPECIFIED_DAY = getattr(mt5, 'ORDER_TIME_SPECIFIED_DAY', 3)

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
            "M1":  TF_M1,
            "M5":  TF_M5,
            "M15": TF_M15,
            "H1":  TF_H1,
            "H4":  TF_H4,
            "D1":  TF_D1,
        }

    def connect(self) -> bool:
        """
        Login using credentials from configuration, or attach if already running.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        # STEP 1: Try attaching to an already running terminal first.
        # This prevents IPC timeouts if the user has manually logged into Exness.
        if hasattr(self.config, 'mt5_path') and self.config.mt5_path:
            attached = mt5.initialize(path=self.config.mt5_path)
        else:
            attached = mt5.initialize()
            
        if attached:
            account_info = mt5.account_info()
            if account_info and account_info.login == self.config.mt5_login:
                logger.info(f"MT5 attached successfully (already logged in): {account_info.login}")
                return True

        # STEP 2: We are attached but wrong account, OR not attached at all.
        logger.info(f"Attempting to force login to {self.config.mt5_server}...")
        
        init_kwargs = {
            "login": self.config.mt5_login,
            "password": self.config.mt5_password,
            "server": self.config.mt5_server
        }
        if hasattr(self.config, 'mt5_path') and self.config.mt5_path:
            init_kwargs["path"] = self.config.mt5_path

        initialized = mt5.initialize(**init_kwargs)
        
        # Verify if initialization timed out or returned False but actually worked in the background
        if not initialized:
            error_code, error_msg = mt5.last_error()
            if error_code == -10005 or error_code == 1:  # IPC timeout or already successful
                import time
                if error_code == -10005:
                    logger.warning("IPC timeout during initialize(). Waiting 3s to check if terminal successfully logged in anyway...")
                    time.sleep(3)
                account_info = mt5.account_info()
                if account_info and account_info.login == self.config.mt5_login:
                    logger.info(f"MT5 connected successfully (bypassed init error {error_code}): {account_info.login}")
                    return True
            logger.error(f"MT5 initialization failed: {error_code}, {error_msg}")
            return False

        # If it initialized successfully, explicitly call login to be 100% sure
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
            # Verify if login timed out or returned False but actually worked in the background
            error_code, error_msg = mt5.last_error()
            if error_code == -10005 or error_code == 1:  # IPC timeout or already successful
                import time
                if error_code == -10005:
                    logger.warning("IPC timeout during login(). Waiting 3s to check if terminal successfully logged in anyway...")
                    time.sleep(3)
                account_info = mt5.account_info()
                if account_info and account_info.login == self.config.mt5_login:
                    logger.info(f"MT5 connected successfully (bypassed login error {error_code}): {account_info.login}")
                    return True

            logger.error(f"MT5 login failed: {error_code}, {error_msg}")
            return False

    def disconnect(self):
        """Cleanly shutdown MT5 connection."""
        mt5.shutdown()
        logger.info("MT5 connection closed.")

    def preload_history(self):
        """
        Force MT5 terminal to download historical data for all pairs and required timeframes 
        immediately on startup to act exactly like the backtesting environment.

        Timeframe requirements per scanner:
          D1  : 21 candles (20-day bias range) + 7 candles (ADR) → fetch 50 for safety
          H4  : 120 candles (20 trading days of H4 for OB detection in Strategy B)
          H1  : 480 candles (20 trading days of H1 for OB detection + 0GMT price)
          M15 : 2880 candles (30 days × 96 bars/day — general use)
          M1  : up to elapsed-minutes-since-midnight for tested-level check (cap 1500 for speed)
        """
        logger.info("Pre-loading historical data for all pairs and all required timeframes. This may take a few seconds...")
        import time
        for pair in self.config.pairs:
            # Select symbol into Market Watch first
            mt5.symbol_select(pair, True)

            mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_D1,  0, 50)    # 50 D1 candles
            mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H4,  0, 200)   # 200 H4 candles = 33 days
            mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H1,  0, 720)   # 720 H1 candles = 30 days
            mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 0, 3000)  # 3000 M15 candles = 31 days
            mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M1,  0, 1500)  # 1500 M1 candles = 25 hours

        time.sleep(3.0)  # Give MT5 time to complete background broker downloads
        logger.info("Historical data pre-load complete for all timeframes (D1/H4/H1/M15/M1).")


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

        # Trigger a broker sync if we received significantly fewer candles than requested.
        # Threshold: 90% of requested count, minimum 10 candles (or `count` if count < 10).
        # This avoids panic-retries for large requests where 1-2 missing bars is normal,
        # while still catching genuine data gaps (e.g. 2 bars returned for an H1 480 request).
        if count < 10:
            min_acceptable = count
        else:
            min_acceptable = max(10, int(count * 0.90))
            
        if rates is None or len(rates) < min_acceptable:
            logger.warning(f"Missing {timeframe} candles for {symbol} (Got {len(rates) if rates is not None else 0}/{count}, need {min_acceptable}). Forcing MT5 terminal to sync with broker...")

            
            # Step 1: Ensure symbol is actually visible in Market Watch
            success = mt5.symbol_select(symbol, True)
            if not success:
                logger.error(f"FATAL: MT5 does not recognize symbol '{symbol}'. Does your broker use a suffix (e.g. {symbol}m or {symbol}.a)?")
                return pd.DataFrame()
            
            # Step 2: Request a large chunk of data to trigger a background download from the broker
            _ = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, 3000)
            
            import time
            time.sleep(1.5)  # Wait for MT5 to fetch data
            
            # Step 3: Try getting the requested candles again
            rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)
            
            # If it still fails severely, try reconnecting
            if rates is None or len(rates) < min_acceptable:
                logger.warning(f"Sync failed for {symbol}. Attempting one-time reconnect...")
                if self.connect():
                    import time
                    time.sleep(1.0)
                    rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, count)

            
        if rates is None or len(rates) == 0:
            logger.error(f"Could not retrieve {timeframe} candles for {symbol} after retry.")
            return pd.DataFrame()

        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
        return df[['time', 'open', 'high', 'low', 'close', 'tick_volume']]

    def current_time(self) -> datetime:
        """Return the current time in UTC. Abstracted for backtest compatibility."""
        return datetime.now(timezone.utc)

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
        
        # JPY and Gold (XAU) pairs usually have 0.01 pip size
        # Others usually have 0.0001 pip size
        if "JPY" in symbol.upper() or "XAU" in symbol.upper():
            return spread_points * symbol_info.point / 0.01
        else:
            return spread_points * symbol_info.point / 0.0001

    def get_tick(self, symbol: str) -> Optional[Dict[str, float]]:
        """Returns the current tick (bid/ask/time) for a symbol."""
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            return {"bid": tick.bid, "ask": tick.ask, "time": float(tick.time)}
        return None

    def get_symbol_point(self, symbol: str) -> float:
        """Returns the point value for a symbol."""
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info:
            return symbol_info.point
        # Fallback heuristic if symbol_info fails
        if "JPY" in symbol.upper() or "XAU" in symbol.upper():
            return 0.01
        return 0.00001

    def get_volume_step(self, symbol: str) -> float:
        """
        Returns the minimum volume step for a symbol (e.g. 0.01 on most brokers).
        Used to round partial lots to a valid broker-supported size.
        Falls back to 0.01 if symbol info is unavailable.
        """
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info and hasattr(symbol_info, "volume_step") and symbol_info.volume_step > 0:
            return float(symbol_info.volume_step)
        return 0.01  # Safe universal fallback

    def get_current_bid(self, symbol: str) -> Optional[float]:
        """
        Returns the current bid price for a symbol.
        Returns None if price data is unavailable.
        """
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            return float(tick.bid)
        return None

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

        info = mt5.symbol_info(symbol)
        if not info:
            return {"success": False, "ticket": -1, "error": f"Symbol {symbol} not found"}
            
        digits = info.digits
        price = round(float(price), digits)
        sl = round(float(sl), digits)
        tp = round(float(tp), digits) if tp else 0.0

        request = {
            "action": ACTION_DEAL,
            "symbol": symbol,
            "volume": float(lot),
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

    def place_pending_order(self, symbol: str, order_type: str, lot: float, entry_price: float, sl: float, tp: float, comment: str = "VvE_Limit", expiration_ts: int = None) -> Dict[str, Any]:
        """
        Place a pending order (Limit or Stop). Automatically chooses based on current market price.
        Supports custom expiration_ts (Unix timestamp).
        """
        tick = mt5.symbol_info_tick(symbol)
        if not tick:
            return {"success": False, "ticket": -1, "error": f"Could not get tick for {symbol}"}

        if order_type.upper() == "BUY":
            mt5_type = ORD_BUY_LIMIT if entry_price < tick.ask else ORD_BUY_STOP
        elif order_type.upper() == "SELL":
            mt5_type = ORD_SELL_LIMIT if entry_price > tick.bid else ORD_SELL_STOP
        else:
            return {"success": False, "ticket": -1, "error": f"Invalid order type for limit/stop: {order_type}"}

        # Expiration for pending orders set to end of next day
        info = mt5.symbol_info(symbol)
        if not info:
            return {"success": False, "ticket": -1, "error": f"Symbol {symbol} not found"}
            
        digits = info.digits
        entry_price = round(float(entry_price), digits)
        sl = round(float(sl), digits)
        tp = round(float(tp), digits) if tp else 0.0

        # Set expiration to the end of tomorrow (broker time) if not provided
        if expiration_ts is not None:
            expiration_time = expiration_ts
            # If using an exact timestamp, use TIME_SPECIFIED
            type_time = mt5.ORDER_TIME_SPECIFIED
        else:
            expiration_time = int((datetime.now() + timedelta(days=2)).replace(hour=0, minute=0, second=0).timestamp())
            type_time = mt5.ORDER_TIME_SPECIFIED_DAY

        request = {
            "action": ACTION_PENDING,
            "symbol": symbol,
            "volume": float(lot),
            "type": mt5_type,
            "price": entry_price,
            "sl": sl,
            "tp": tp,
            "deviation": 20,
            "magic": 123456,
            "comment": comment,
            "type_time": type_time,
            "expiration": expiration_time,
            "type_filling": self._get_filling_mode(symbol),
        }

        retries = 3
        for i in range(retries):
            result = mt5.order_send(request)
            if result is None:
                err_msg = f"order_send (pending) returned None. MT5 Error: {mt5.last_error()}"
                logger.error(err_msg)
                return {"success": False, "ticket": -1, "error": err_msg}

            if result.retcode in [RETCODE_DONE, RETCODE_PLACED]:
                logger.info(f"Pending order placed: {symbol} {order_type} LIMIT {lot} @ {result.price}, Ticket: {result.order}")
                return {"success": True, "ticket": result.order, "error": ""}
            
            if result.retcode in [RETCODE_REQUOTE, RETCODE_BUSY, RETCODE_TOO_MANY_REQUESTS, RETCODE_LOCKED]:
                logger.warning(f"Pending order retry {i+1}/{retries} due to retcode: {result.retcode} ({result.comment})")
                time.sleep(1.0)
                continue
            else:
                logger.error(f"Pending order failed: {result.retcode} - {result.comment}")
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
        pos_identifier = pos.identifier
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
            
            # Exness and some other brokers change the ticket upon partial close
            # We must detect the new ticket for the remaining volume using position identifier
            new_ticket = ticket
            import time
            for _ in range(10):  # Retry up to 10 times (5 seconds total)
                time.sleep(0.5)
                active_positions = mt5.positions_get(symbol=symbol)
                if active_positions:
                    found_updated = False
                    for p in active_positions:
                        if getattr(p, "identifier", None) == pos_identifier:
                            new_ticket = p.ticket
                            # If ticket changed, or volume decreased, it's updated!
                            if p.ticket != ticket or p.volume < pos.volume:
                                found_updated = True
                            break
                    if found_updated:
                        break
                        
            return {"success": True, "error": "", "new_ticket": new_ticket}
        
        err_msg = f"Partial close failed: {result.comment if result else 'Unknown error'}"
        logger.error(err_msg)
        return {"success": False, "error": err_msg, "new_ticket": ticket}

    def close_all_positions(self) -> None:
        """
        Close all currently open positions instantly.
        Iterates over all active MT5 positions and closes their full volume.
        """
        positions = mt5.positions_get()
        if not positions:
            logger.info("close_all_positions: No open positions to close.")
            return

        for pos in positions:
            res = self.close_partial(pos.ticket, pos.volume)
            if not res["success"]:
                logger.error(f"Failed to panic-close ticket {pos.ticket}: {res['error']}")
            else:
                logger.info(f"Panic-closed ticket {pos.ticket} successfully.")

        # Additionally, cancel pending orders
        orders = mt5.orders_get()
        if orders:
            for order in orders:
                request = {
                    "action": ACTION_REMOVE,
                    "order": order.ticket
                }
                res = mt5.order_send(request)
                if res and res.retcode == RETCODE_DONE:
                    logger.info(f"Panic-cancelled pending order {order.ticket}")
                else:
                    logger.error(f"Failed to cancel pending order {order.ticket}")

    def get_historical_profit(self, ticket: int) -> float:
        """
        Retrieve the actual realized profit/loss for a closed ticket from MT5 history.
        
        Args:
            ticket (int): The position/ticket ID.
            
        Returns:
            float: Total profit/loss in account currency. Returns 0.0 if not found.
        """
        # First, try fetching by position ID directly (most accurate)
        deals = mt5.history_deals_get(position=ticket)
        
        if not deals:
            # Fallback: fetch last 5 days of deals and manually filter (prevents MT5 group filter bug & handles ticket changes)
            from_date = datetime.now() - timedelta(days=5)
            all_deals = mt5.history_deals_get(from_date, datetime.now())
            if all_deals:
                # If the ticket passed is a NEW ticket (from partial close), its true 'position_id' is the original ticket.
                true_position_id = ticket
                for d in all_deals:
                    if getattr(d, 'order', 0) == ticket or getattr(d, 'ticket', 0) == ticket:
                        true_position_id = getattr(d, 'position_id', ticket)
                        if true_position_id > 0:
                            break
                            
                deals = [d for d in all_deals if getattr(d, 'position_id', 0) == true_position_id]

        if not deals:
            return 0.0

        total_profit = 0.0
        for deal in deals:
            total_profit += (getattr(deal, 'profit', 0.0) + getattr(deal, 'commission', 0.0) + getattr(deal, 'swap', 0.0))
            
        return round(total_profit, 2)

    def modify_sl(self, ticket: int, new_sl: float) -> Dict[str, Any]:
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
