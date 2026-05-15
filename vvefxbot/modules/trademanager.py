import time
import threading
import traceback
import MetaTrader5 as mt5
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine
from modules.telegrambridge import TelegramBridge
from modules.reportgoogle import GoogleSheetReporter

logger = get_logger("TradeManager")

_MONITOR_INTERVAL_SECONDS = 5
_DAILY_LOSS_THRESHOLD_PCT = 20.0  # % of trading_pool_size


class TradeManager:
    """Background trade monitor: manages TP1/BE/TP2 flow and loss detection."""

    def __init__(
        self,
        config: Config,
        mt5connector: MT5Connector,
        state_engine: StateEngine,
        telegram_bridge: TelegramBridge,
        reporter: GoogleSheetReporter = None
    ):
        """
        Initializes the TradeManager.

        Args:
            config (Config): Validated configuration dataclass.
            mt5connector (MT5Connector): Live MT5 connection.
            state_engine (StateEngine): Persistence engine.
            telegram_bridge (TelegramBridge): For sending trade alerts.
        """
        self.config = config
        self.mt5 = mt5connector
        self.state = state_engine
        self.telegram = telegram_bridge
        self.reporter = reporter

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _pip_size(self, pair: str) -> float:
        """Return pip size for the pair."""
        return 0.01 if "JPY" in pair.upper() else 0.0001

    def _get_current_price(self, pair: str, direction: str) -> Optional[float]:
        """
        Return the relevant current price for direction-based TP comparison.

        BUY  → use ask (broker fills long at ask, monitors with bid but we use close)
        SELL → use bid

        Args:
            pair (str): Trading symbol.
            direction (str): "BUY" or "SELL".

        Returns:
            float | None: Current price or None on error.
        """
        candles = self.mt5.get_candles(pair, "M1", count=1)
        if candles.empty:
            return None
        return float(candles.iloc[-1]["close"])

    def _ticket_exists_in_mt5(self, ticket: int) -> bool:
        """Return True if the ticket is still an open MT5 position."""
        positions = mt5.positions_get(ticket=ticket)
        return positions is not None and len(positions) > 0

    def _get_profit_for_ticket(self, ticket: int) -> float:
        """Retrieve current profit for an open position ticket, or 0.0."""
        positions = mt5.positions_get(ticket=ticket)
        if positions:
            return float(positions[0].profit)
        return 0.0

    def _price_reached(self, current: float, target: float, direction: str) -> bool:
        """
        Check if price has reached the target level.

        BUY  → price must be >= target
        SELL → price must be <= target
        """
        if direction == "BUY":
            return current >= target
        return current <= target

    # ------------------------------------------------------------------
    # DAILY GUARDS
    # ------------------------------------------------------------------

    def _handle_loss_guards(self, today: str, loss_usd: float, result: str, pair: str) -> None:
        """
        Update daily loss counters and trigger guards.

        Args:
            today (str): ISO date string.
            loss_usd (float): Absolute loss in USD (positive value).
            result (str): "LOSS" or "BREAKEVEN".
            pair (str): Pair for logging.
        """
        if result == "LOSS":
            self.state.add_daily_loss(today, abs(loss_usd))
            self.state.increment_consecutive_losses(today)
        else:
            self.state.reset_consecutive_losses(today)

        daily = self.state.get_daily_state(today)

        # Consecutive loss check
        consec = daily.get("consecutive_losses", 0)
        if consec >= 3:
            alert = f"⚠️ 3 consecutive losses reached for {pair}. Session paused."
            logger.warning(alert)
            self.telegram.send_alert(alert)

        # Daily loss threshold check
        max_loss = self.config.trading_pool_size * (_DAILY_LOSS_THRESHOLD_PCT / 100.0)
        if daily.get("total_loss_usd", 0.0) >= max_loss:
            self.state.disable_bot_today(today)
            alert = f"🛑 Daily loss limit reached ({daily['total_loss_usd']:.2f} USD). Bot disabled for today."
            logger.warning(alert)
            self.telegram.send_alert(alert)

    # ------------------------------------------------------------------
    # CORE MONITORING
    # ------------------------------------------------------------------

    def monitor_trade(self, trade: Dict[str, Any]) -> None:
        """
        Apply TP1/BE/TP2 and loss detection logic to a single open trade.

        Args:
            trade (dict): Row from trades_executed with status='OPEN'.
        """
        pair = trade.get("pair") or ""
        direction = trade.get("direction") or "BUY"
        trade_id = trade["trade_id"]
        ticket_id_raw = str(trade["ticket_id"])
        tp1_price = float(trade["tp1"])
        tp2_price = float(trade["tp2"])
        entry_price = float(trade["executed_price"])
        lot_total = float(trade["lot_total"])
        tp1_hit = int(trade.get("tp1_hit", 0))
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Determine case: split order uses "ticket1,ticket2" notation
        is_split = "," in ticket_id_raw
        tickets = [int(t.strip()) for t in ticket_id_raw.split(",")]
        ticket1 = tickets[0]
        ticket2 = tickets[1] if is_split else None

        current_price = self._get_current_price(pair, direction)
        if current_price is None:
            logger.warning(f"[{pair}] Could not get current price for trade {trade_id}")
            return

        # ── CASE B: Split order ────────────────────────────────────────
        if is_split and ticket2 is not None:
            self._handle_case_b(
                trade, trade_id, pair, direction,
                ticket1, ticket2, current_price,
                tp1_price, tp2_price, entry_price,
                tp1_hit, today
            )
        else:
            # ── CASE A: Single order ───────────────────────────────────
            self._handle_case_a(
                trade, trade_id, pair, direction,
                ticket1, current_price,
                tp1_price, tp2_price, entry_price,
                lot_total, tp1_hit, today
            )

    def _handle_case_a(
        self,
        trade: Dict, trade_id: str, pair: str, direction: str,
        ticket: int, current_price: float,
        tp1_price: float, tp2_price: float, entry_price: float,
        lot_total: float, tp1_hit: int, today: str
    ) -> None:
        """
        Manage a single-ticket trade (Case A).

        TP1 → move SL to BE; TP2 → close full position.
        """
        ticket_alive = self._ticket_exists_in_mt5(ticket)

        # ── TP1 not yet hit: check if price reached TP1 ─────────────
        if tp1_hit == 0:
            if self._price_reached(current_price, tp1_price, direction):
                # Close half lot at TP1 to lock in partial profit
                half_lot = max(0.01, round(lot_total / 2, 2))
                tp1_close = self.mt5.close_partial(ticket, half_lot)
                if tp1_close["success"]:
                    partial_profit = self._get_profit_for_ticket(ticket)
                    # Move SL to breakeven on the remaining half
                    be_result = self.mt5.modify_sl(ticket, entry_price)
                    self.state.update_trade_tp1_hit(trade_id)
                    self.state.update_trade_be_moved(trade_id)
                    self.state.insert_event(trade_id, "TP1_HIT", current_price)
                    self.state.insert_event(trade_id, "BE_MOVED", entry_price)
                    msg = (
                        f"📊 TP1 Hit — Closed {half_lot} lot | SL → BE | "
                        f"{pair} | Ticket: {ticket} | Partial P&L: {partial_profit:.2f} USD"
                    )
                    logger.info(msg)
                    self.telegram.send_alert(msg)
                    if not be_result["success"]:
                        logger.error(f"[{pair}] SL→BE modification failed: {be_result['error']}")
                else:
                    logger.error(f"[{pair}] TP1 partial close failed: {tp1_close['error']}")
            elif not ticket_alive:
                # Position closed externally before TP1 — likely SL hit
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket, current_price, entry_price, direction, today
                )
            return

        # ── TP1 already hit: watch for TP2 (close remaining half) ────
        if tp1_hit == 1:
            if self._price_reached(current_price, tp2_price, direction):
                half_lot = max(0.01, round(lot_total / 2, 2))
                close_result = self.mt5.close_partial(ticket, half_lot)
                if close_result["success"]:
                    profit = self.mt5.get_historical_profit(ticket)
                    self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
                    self.state.insert_event(trade_id, "TP2_HIT", current_price)
                    self.state.add_daily_profit(today, profit)
                    self.state.reset_consecutive_losses(today)
                    msg = f"✅ TP2 Hit — Trade CLOSED WIN | {pair} | +{profit:.2f} USD"
                    logger.info(msg)
                    self.telegram.send_alert(msg)
                    
                    # Update Google Sheet
                    if self.reporter:
                        updated_trade = self.state.get_trade(trade_id)
                        signal = self.state.get_signal(trade.get("signal_id", ""))
                        self.reporter.log_trade(updated_trade, signal)
                else:
                    logger.error(f"[{pair}] TP2 close failed: {close_result['error']}")
            elif not ticket_alive:
                # Closed externally — price was at BE or above/below
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket, current_price, entry_price, direction, today
                )

    def _handle_case_b(
        self,
        trade: Dict, trade_id: str, pair: str, direction: str,
        ticket1: int, ticket2: int, current_price: float,
        tp1_price: float, tp2_price: float, entry_price: float,
        tp1_hit: int, today: str
    ) -> None:
        """
        Manage a split-ticket trade (Case B, lot_total=0.02).

        ticket1 closes at TP1; ticket2 rides to TP2 with SL at BE.
        """
        ticket1_alive = self._ticket_exists_in_mt5(ticket1)
        ticket2_alive = self._ticket_exists_in_mt5(ticket2)

        # ── TP1 phase: close ticket1 at TP1 ──────────────────────────
        if tp1_hit == 0:
            if self._price_reached(current_price, tp1_price, direction):
                if ticket1_alive:
                    close_result = self.mt5.close_partial(ticket1, 0.01)
                    if close_result["success"]:
                        # Move SL on ticket2 to BE
                        self.mt5.modify_sl(ticket2, entry_price)
                        self.state.update_trade_tp1_hit(trade_id)
                        self.state.update_trade_be_moved(trade_id)
                        self.state.insert_event(trade_id, "TP1_HIT", current_price)
                        self.state.insert_event(trade_id, "BE_MOVED", entry_price)
                        msg = f"📊 TP1 Hit (ticket {ticket1} closed) + SL→BE on {ticket2} | {pair}"
                        logger.info(msg)
                        self.telegram.send_alert(msg)
                    else:
                        logger.error(f"[{pair}] Case B TP1 close failed: {close_result['error']}")
            elif not ticket1_alive:
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket1, current_price, entry_price, direction, today
                )
            return

        # ── TP2 phase: close ticket2 at TP2 ──────────────────────────
        if tp1_hit == 1:
            if self._price_reached(current_price, tp2_price, direction):
                if ticket2_alive:
                    profit = self._get_profit_for_ticket(ticket2)
                    close_result = self.mt5.close_partial(ticket2, 0.01)
                    if close_result["success"]:
                        self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
                        self.state.insert_event(trade_id, "TP2_HIT", current_price)
                        self.state.add_daily_profit(today, profit)
                        self.state.reset_consecutive_losses(today)
                        msg = f"✅ TP2 Hit — Trade CLOSED WIN | {pair} | +{profit:.2f} USD"
                        logger.info(msg)
                        self.telegram.send_alert(msg)

                        # Update Google Sheet
                        if self.reporter:
                            updated_trade = self.state.get_trade(trade_id)
                            signal = self.state.get_signal(trade.get("signal_id", ""))
                            self.reporter.log_trade(updated_trade, signal)
                    else:
                        logger.error(f"[{pair}] Case B TP2 close failed: {close_result['error']}")
            elif not ticket2_alive:
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket2, current_price, entry_price, direction, today
                )

    def _handle_unexpected_close(
        self,
        trade: Dict, trade_id: str, pair: str, ticket: int,
        current_price: float, entry_price: float, direction: str, today: str
    ) -> None:
        """
        Handle a position closed outside the normal TP flow (SL hit or manual close).

        Determines LOSS vs BREAKEVEN and applies daily guards.
        """
        # Fetch real profit from MT5 history
        profit_usd = self.mt5.get_historical_profit(ticket)
        
        # Determine result based on profit and BE status
        if profit_usd >= -0.01 and int(trade.get("be_moved", 0)):
            result = "BREAKEVEN"
        else:
            result = "WIN" if profit_usd > 0 else "LOSS"

        event_type = "SL_HIT" if result == "LOSS" else "MANUAL_CLOSE"

        self.state.update_trade_status(trade_id, "CLOSED", result, round(profit_usd, 2))
        self.state.insert_event(trade_id, event_type, current_price)

        msg = f"❌ Trade CLOSED {result} | {pair} | {profit_usd:.2f} USD"
        logger.info(msg)
        self.telegram.send_alert(msg)

        # Update Google Sheet
        if self.reporter:
            updated_trade = self.state.get_trade(trade_id)
            signal = self.state.get_signal(trade.get("signal_id", ""))
            self.reporter.log_trade(updated_trade, signal)

        self._handle_loss_guards(today, profit_usd, result, pair)

    # ------------------------------------------------------------------
    # MAIN LOOP
    # ------------------------------------------------------------------

    def monitor_all_trades(self) -> None:
        """Fetch all open trades and monitor each one."""
        open_trades = self.state.get_open_trades()
        for trade in open_trades:
            try:
                self.monitor_trade(trade)
            except Exception as e:
                err = traceback.format_exc()
                logger.error(f"Error monitoring trade {trade.get('trade_id')}: {e}\n{err}")

    def start_monitoring(self) -> None:
        """Start the trade monitoring loop in a daemon background thread."""
        def _loop():
            """Internal monitor loop — runs every 5 seconds, never crashes the bot."""
            logger.info("TradeManager monitoring thread started.")
            while True:
                try:
                    self.monitor_all_trades()
                except Exception as e:
                    logger.error(f"TradeManager loop error: {e}\n{traceback.format_exc()}")
                time.sleep(_MONITOR_INTERVAL_SECONDS)

        thread = threading.Thread(target=_loop, daemon=True, name="TradeMonitor")
        thread.start()
