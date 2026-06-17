"""
trademanager.py — VvE FxBOT Trade Monitoring and Management

Manages open positions toward TP1/TP2/SL outcomes.

Partial TP Management (generic, works for any strategy):
    - Controlled by config keys: partial_tp_enabled, partial_tp_fraction, breakeven_buffer_pips
    - At TP1: close `partial_tp_fraction` of the lot (default 50%)
    - Immediately after: move SL to BE + breakeven_buffer_pips (default 30 pips)
    - At TP2: close remaining position fully
    - All lot calculations use broker volume_step for safe rounding
    - Restart-safe: tp1_hit and be_moved are persisted in DB; never double-triggers

Event log (trade_management_events table):
    TP1_HIT  — partial close executed
    BE_MOVED — SL moved to breakeven buffer
    TP2_HIT  — remaining position closed
    SL_HIT   — position closed at SL (loss)
    MANUAL_CLOSE — detected external close
"""

import math
import time
import threading
import traceback
import MetaTrader5 as mt5
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine
from modules.telegrambridge import TelegramBridge
from modules.reportgoogle import GoogleSheetReporter
from modules.vaultengine import VaultEngine

logger = get_logger("TradeManager")

_MONITOR_INTERVAL_SECONDS = 5
_DAILY_LOSS_THRESHOLD_PCT = 20.0  # % of trading_pool_size


class TradeManager:
    """Background trade monitor: manages partial TP / BE / TP2 flow and loss detection."""

    def __init__(
        self,
        config: Config,
        mt5connector: MT5Connector,
        state_engine: StateEngine,
        telegram_bridge: TelegramBridge,
        reporter: GoogleSheetReporter = None,
        vault_engine: VaultEngine = None
    ):
        self.config = config
        self.mt5 = mt5connector
        self.state = state_engine
        self.telegram = telegram_bridge
        self.reporter = reporter
        self.vault = vault_engine

        # Read partial-TP config with safe defaults
        tm_cfg = getattr(config, "trade_management", {})
        self._partial_tp_enabled   = bool(tm_cfg.get("partial_tp_enabled", True))
        self._partial_tp_fraction  = float(tm_cfg.get("partial_tp_fraction", 0.5))
        self._be_buffer_pips       = float(tm_cfg.get("breakeven_buffer_pips", 30))

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _pip_size(self, pair: str) -> float:
        """Return pip size for the pair (correct per instrument type)."""
        p = pair.upper()
        if "JPY" in p or "XAU" in p:
            return 0.01
        return 0.0001

    def _pips_to_price(self, pair: str, pips: float) -> float:
        """Convert pips to a price distance."""
        return self._pip_size(pair) * pips

    def _round_to_volume_step(self, symbol: str, lot: float) -> float:
        """
        Round a lot size DOWN to the nearest broker-supported volume step.
        E.g. 0.005 with step 0.01 → 0.01 (but we floor, not round, so 0.005 → 0.00).
        We handle the minimum lot guard by returning max(volume_step, floored_lot).

        Uses math.floor to avoid sending oversized partial close requests.
        """
        step = self.mt5.get_volume_step(symbol)
        if step <= 0:
            step = 0.01
        # Floor to volume step: int(lot / step) * step
        floored = math.floor(lot / step) * step
        floored = round(floored, 10)  # eliminate float noise
        if floored < step:
            logger.warning(
                f"[{symbol}] Partial lot {lot:.5f} floored to {floored:.5f} "
                f"< step {step:.5f}. Broker may not support this volume."
            )
        return round(floored, 8)

    def _get_current_price(self, pair: str, direction: str) -> Optional[float]:
        """
        Return live bid/ask price for real-time TP/SL monitoring.
        BUY trades compare against ASK (the price at which position would close at profit).
        SELL trades compare against BID.
        Falls back to last M1 close if tick is unavailable.
        """
        try:
            tick = mt5.symbol_info_tick(pair)
            if tick is not None:
                # For BUY: profit comes when bid >= TP (bid is what market will pay us)
                # For SELL: profit comes when ask <= TP (ask is what market charges to close)
                return float(tick.bid) if direction == "BUY" else float(tick.ask)
        except Exception:
            pass
        # Fallback: last M1 close
        candles = self.mt5.get_candles(pair, "M1", count=1)
        if candles is None or candles.empty:
            return None
        return float(candles.iloc[-1]["close"])

    def _ticket_exists_in_mt5(self, ticket: int) -> bool:
        """Return True if the ticket is still an open MT5 position."""
        positions = mt5.positions_get(ticket=ticket)
        return positions is not None and len(positions) > 0

    def _order_exists_in_mt5(self, ticket: int) -> bool:
        """Return True if the ticket is still a pending MT5 order."""
        orders = mt5.orders_get(ticket=ticket)
        return orders is not None and len(orders) > 0

    def _get_open_volume(self, ticket: int) -> Optional[float]:
        """Return the current open volume for a ticket (from live MT5), or None."""
        positions = mt5.positions_get(ticket=ticket)
        if positions and len(positions) > 0:
            return float(positions[0].volume)
        return None

    def _get_profit_for_ticket(self, ticket: int) -> float:
        """Retrieve current floating profit for an open position."""
        positions = mt5.positions_get(ticket=ticket)
        if positions:
            return float(positions[0].profit)
        return 0.0

    def _price_reached(self, current: float, target: float, direction: str) -> bool:
        """
        True if price has crossed the target in the trade direction.
        CRITICAL: Returns False if target is 0 or negative — prevents accidental
        TP2/TP3 triggers when those prices are unset (default 0.0 in DB).
        """
        if target is None or target <= 0.0:
            return False  # Guard: unset target must NEVER trigger
        if direction == "BUY":
            return current >= target
        return current <= target

    # ------------------------------------------------------------------
    # TELEGRAM HELPER
    # ------------------------------------------------------------------

    def _send_alert(self, msg: str) -> None:
        """Send a Telegram alert, guarding against self.telegram being None."""
        if self.telegram is None:
            logger.warning(f"TradeManager: Telegram not wired. Dropping: {msg[:80]}")
            return
        try:
            self.telegram.send_alert(msg)
        except Exception as e:
            logger.error(f"TradeManager: send_alert failed: {e}")

    # ------------------------------------------------------------------
    # DAILY GUARDS
    # ------------------------------------------------------------------

    def _handle_loss_guards(self, today: str, loss_usd: float, result: str, pair: str) -> None:
        """Update daily loss counters and trigger guards."""
        if result == "LOSS":
            self.state.add_daily_loss(today, abs(loss_usd))
            self.state.increment_consecutive_losses(today)
        else:
            self.state.reset_consecutive_losses(today)

        daily = self.state.get_daily_state(today)

        consec = daily.get("consecutive_losses", 0)
        if consec >= 3:
            alert = f"⚠️ 3 consecutive losses reached for {pair}. Session paused."
            logger.warning(alert)
            self._send_alert(alert)

        max_loss = self.config.trading_pool_size * (_DAILY_LOSS_THRESHOLD_PCT / 100.0)
        if daily.get("total_loss_usd", 0.0) >= max_loss:
            self.state.disable_bot_today(today)
            alert = (
                f"🛑 Daily loss limit reached ({daily['total_loss_usd']:.2f} USD). "
                f"Bot disabled for today."
            )
            logger.warning(alert)
            self._send_alert(alert)

    # ------------------------------------------------------------------
    # CORE MONITORING
    # ------------------------------------------------------------------

    def monitor_trade(self, trade: Dict[str, Any]) -> None:
        """
        Apply TP1/BE/TP2 and loss detection logic to a single open trade.

        Args:
            trade (dict): Row from trades_executed with status='OPEN'.
        """
        pair        = trade.get("pair") or ""
        direction   = trade.get("direction") or "BUY"
        trade_id    = trade["trade_id"]
        ticket_id_raw = str(trade["ticket_id"])
        tp1_price   = float(trade["tp1"])
        tp2_price   = float(trade["tp2"])
        tp3_price   = float(trade.get("tp3", 0.0) or 0.0)
        entry_price = float(trade["executed_price"])
        lot_total   = float(trade["lot_total"])
        tp1_hit     = int(trade.get("tp1_hit", 0))
        tp2_hit     = int(trade.get("tp2_hit", 0))
        today       = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Determine case: split order uses "ticket1,ticket2" notation
        is_split = "," in ticket_id_raw
        tickets  = [int(t.strip()) for t in ticket_id_raw.split(",")]
        ticket1  = tickets[0]
        ticket2  = tickets[1] if is_split else None

        current_price = self._get_current_price(pair, direction)
        if current_price is None:
            logger.warning(f"[{pair}] Could not get current price for trade {trade_id}")
            return

        if is_split and ticket2 is not None:
            # ── CASE B: Split order (legacy MMXM) ─────────────────────
            self._handle_case_b(
                trade, trade_id, pair, direction,
                ticket1, ticket2, current_price,
                tp1_price, tp2_price, entry_price,
                tp1_hit, today
            )
        else:
            # ── CASE A: Single order (ZGMT + any single-ticket strategy)
            self._handle_case_a(
                trade, trade_id, pair, direction,
                ticket1, current_price,
                tp1_price, tp2_price, tp3_price, entry_price,
                lot_total, tp1_hit, tp2_hit, today
            )

    # ------------------------------------------------------------------
    # CASE A — Single ticket with configurable partial TP
    # ------------------------------------------------------------------

    def _handle_case_a(
        self,
        trade: Dict, trade_id: str, pair: str, direction: str,
        ticket: int, current_price: float,
        tp1_price: float, tp2_price: float, tp3_price: float, entry_price: float,
        lot_total: float, tp1_hit: int, tp2_hit: int, today: str
    ) -> None:
        """
        Manage a single-ticket trade (Case A).

        When partial_tp_enabled=True:
            TP1 → close partial_tp_fraction of lot → move SL to BE + buffer pips
            TP2 → close 50% of REMAINING lot → move SL to TP1
            TP3 → close FULL remaining lot

        When partial_tp_enabled=False:
            TP1 → only move SL to BE (no partial close)
            TP2 → close full lot
        """
        ticket_alive = self._ticket_exists_in_mt5(ticket)

        # ── TP1 not yet hit ──────────────────────────────────────────
        if tp1_hit == 0:
            if self._price_reached(current_price, tp1_price, direction):
                self._execute_tp1(
                    trade, trade_id, pair, direction,
                    ticket, current_price, entry_price,
                    lot_total, today
                )
            elif not ticket_alive:
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket,
                    current_price, entry_price, direction, today
                )
            return

        # ── TP1 already hit, TP2 not yet hit ────────────────────────
        if tp1_hit == 1 and tp2_hit == 0:
            if self._price_reached(current_price, tp2_price, direction):
                self._execute_tp2(
                    trade, trade_id, pair, ticket, current_price, tp1_price,
                    lot_total, today, tp3_price
                )
            elif not ticket_alive:
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket,
                    current_price, entry_price, direction, today
                )
            return

        # ── TP2 already hit, TP3 not yet hit ────────────────────────
        if tp1_hit == 1 and tp2_hit == 1 and tp3_price > 0:
            if self._price_reached(current_price, tp3_price, direction):
                self._execute_tp3(
                    trade, trade_id, pair, ticket, current_price,
                    lot_total, today
                )
            elif not ticket_alive:
                self._handle_unexpected_close(
                    trade, trade_id, pair, ticket,
                    current_price, entry_price, direction, today
                )
            return

        # ── Fallback guard: position closed but state didn't catch it ──
        if not ticket_alive:
            logger.warning(f"[{pair}] Fallback: Ticket {ticket} dead but not caught by TP flow.")
            self._handle_unexpected_close(
                trade, trade_id, pair, ticket,
                current_price, entry_price, direction, today
            )

    def _execute_tp1(
        self,
        trade: Dict, trade_id: str, pair: str, direction: str,
        ticket: int, current_price: float, entry_price: float,
        lot_total: float, today: str
    ) -> None:
        """
        Execute TP1 action:
          - If partial_tp_enabled: close fraction of open lot, move SL to BE+buffer
          - Else: only move SL to BE+buffer (no partial close)
        Guards against double-execution via tp1_hit DB flag.
        """
        if self._partial_tp_enabled:
            # ── Partial close ─────────────────────────────────────────
            # Use live volume from MT5, not stored lot_total, for accuracy
            live_volume = self._get_open_volume(ticket)
            if live_volume is None:
                logger.warning(f"[{pair}] Could not read live volume for ticket {ticket}. Skipping TP1.")
                return

            raw_partial = live_volume * self._partial_tp_fraction
            partial_lot = self._round_to_volume_step(pair, raw_partial)

            if partial_lot <= 0:
                logger.warning(
                    f"[{pair}] Partial lot {raw_partial:.5f} rounded to zero "
                    f"(volume step too large). Skipping TP1 partial close."
                )
                # Still mark TP1 so we don't keep retrying
                self.state.update_trade_tp1_hit(trade_id)
                self.state.insert_event(trade_id, "TP1_HIT", current_price)
                
                msg = (
                    f"📊 *TP1 Hit — No Partial Close*\n"
                    f"Pair: `{pair}` | Ticket: `{ticket}`\n"
                    f"Price: `{current_price:.5f}`\n"
                    f"_Remaining lot ({live_volume:.4f}) is too small to split._"
                )
                self._send_alert(msg)
                
                if self.reporter:
                    updated_trade = self.state.get_trade(trade_id)
                    signal = self.state.get_signal(trade.get("signal_id", ""))
                    self.reporter.log_trade(updated_trade, signal)

            else:
                tp1_close = self.mt5.close_partial(ticket, partial_lot)
                if tp1_close["success"]:
                    new_ticket = tp1_close.get("new_ticket", ticket)
                    if str(new_ticket) != str(ticket):
                        self.state.update_trade_ticket(trade_id, str(new_ticket))
                        ticket_display = f"{ticket} -> {new_ticket}"
                        ticket = new_ticket
                    else:
                        ticket_display = str(ticket)

                    partial_profit = self._get_profit_for_ticket(ticket)
                    remaining_lot = round(live_volume - partial_lot, 8)
                    self.state.update_trade_tp1_hit(trade_id)
                    self.state.insert_event(trade_id, "TP1_HIT", current_price)
                    msg = (
                        f"📊 *TP1 Hit — Partial Close*\n"
                        f"Pair: `{pair}` | Ticket: `{ticket_display}`\n"
                        f"Closed: `{partial_lot:.4f}` lot ({self._partial_tp_fraction*100:.0f}%) | Remaining: `{remaining_lot:.4f}` lot\n"
                        f"Price: `{current_price:.5f}` | Float P&L: `{partial_profit:.2f} USD`"
                    )
                    logger.info(f"[{pair}] TP1 Hit: closed {partial_lot:.4f} | remaining {remaining_lot:.4f} | Ticket {ticket_display}")
                    self._send_alert(msg)
                else:
                    logger.error(
                        f"[{pair}] TP1 partial close failed: {tp1_close['error']}. "
                        f"Will retry next monitor cycle."
                    )
                    return  # Don't move SL or mark tp1_hit if close failed
        else:
            # ── No partial close: just mark TP1 reached ───────────────
            self.state.update_trade_tp1_hit(trade_id)
            self.state.insert_event(trade_id, "TP1_HIT", current_price)
            logger.info(f"[{pair}] TP1 reached (partial_tp_enabled=False) — SL -> BE+buffer only.")
            self._send_alert(
                f"📊 *TP1 Reached — Moving SL to Breakeven*\n"
                f"Pair: `{pair}` | Ticket: `{ticket}` | Price: `{current_price:.5f}`"
            )

        # ── Move SL to BE + buffer pips ───────────────────────────────
        buffer_price = self._pips_to_price(pair, self._be_buffer_pips)
        if direction == "BUY":
            new_sl = round(entry_price + buffer_price, 5)
        else:
            new_sl = round(entry_price - buffer_price, 5)

        be_result = self.mt5.modify_sl(ticket, new_sl)
        if be_result["success"]:
            self.state.update_trade_be_moved(trade_id)
            self.state.update_trade_current_sl(trade_id, new_sl)
            self.state.insert_event(trade_id, "BE_MOVED", new_sl)
            msg = (
                f"🔒 *SL Moved to Breakeven*\n"
                f"Pair: `{pair}` | Ticket: `{ticket}`\n"
                f"New SL: `{new_sl:.5f}` (Entry +{self._be_buffer_pips:.0f} pips buffer)"
            )
            logger.info(f"[{pair}] SL -> BE+{self._be_buffer_pips:.0f}pips @ {new_sl:.5f}")
            self._send_alert(msg)
        else:
            logger.error(
                f"[{pair}] SL->BE modification failed: {be_result['error']}"
            )

    def _execute_tp2(
        self,
        trade: Dict, trade_id: str, pair: str,
        ticket: int, current_price: float, tp1_price: float,
        lot_total: float, today: str, tp3_price: float
    ) -> None:
        """
        Close the full remaining position at TP2 if no TP3 is set.
        If TP3 exists and partial TP is enabled, close 50% of REMAINING lot and move SL to TP1.
        """
        live_volume = self._get_open_volume(ticket)
        if live_volume is None or live_volume <= 0:
            # Position may have closed externally — handle gracefully
            logger.warning(f"[{pair}] TP2: No open volume on ticket {ticket}. Marking closed.")
            profit = self.mt5.get_historical_profit(ticket)
            self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
            self.state.insert_event(trade_id, "TP2_HIT", current_price)
            self.state.add_daily_profit(today, profit)
            self.state.reset_consecutive_losses(today)
            return

        if self._partial_tp_enabled and tp3_price > 0:
            # ── 1:3 FORMAT: close 50% of REMAINING lot ─────────────────
            raw_partial = live_volume * 0.5
            close_lot = self._round_to_volume_step(pair, raw_partial)
            partial_lot = self._round_to_volume_step(pair, raw_partial)
            if partial_lot <= 0:
                logger.warning(f"[{pair}] Partial lot rounded to zero. Skipping TP2 close.")
                self.state.update_trade_tp2_hit(trade_id)
                self.state.insert_event(trade_id, "TP2_HIT", current_price)
                
                # Move SL to TP1 even if partial close is skipped
                be_result = self.mt5.modify_sl(ticket, tp1_price)
                sl_msg = ""
                if be_result["success"]:
                    self.state.update_trade_current_sl(trade_id, tp1_price)
                    self.state.insert_event(trade_id, "SL_MOVED_TP1", tp1_price)
                    sl_msg = f"\nSL moved to TP1: `{tp1_price:.5f}`"
                else:
                    logger.error(f"[{pair}] SL->TP1 modification failed on zero-lot TP2: {be_result['error']}")

                msg = (
                    f"✅ *TP2 Hit — No Partial Close*\n"
                    f"Pair: `{pair}` | Ticket: `{ticket}`\n"
                    f"Price: `{current_price:.5f}`{sl_msg}\n"
                    f"_Remaining lot ({live_volume:.4f}) is too small to split. Riding TP3..._"
                )
                self._send_alert(msg)
                
                if self.reporter:
                    updated_trade = self.state.get_trade(trade_id)
                    signal = self.state.get_signal(trade.get("signal_id", ""))
                    self.reporter.log_trade(updated_trade, signal)

            else:
                close_result = self.mt5.close_partial(ticket, close_lot)
                if close_result["success"]:
                    new_ticket = close_result.get("new_ticket", ticket)
                    if str(new_ticket) != str(ticket):
                        self.state.update_trade_ticket(trade_id, str(new_ticket))
                        ticket_display = f"{ticket} -> {new_ticket}"
                        ticket = new_ticket
                    else:
                        ticket_display = str(ticket)

                    remaining_lot = round(live_volume - close_lot, 8)
                    self.state.update_trade_tp2_hit(trade_id)
                    self.state.insert_event(trade_id, "TP2_HIT", current_price)
                    
                    # Move SL to TP1
                    be_result = self.mt5.modify_sl(ticket, tp1_price)
                    if be_result["success"]:
                        self.state.update_trade_current_sl(trade_id, tp1_price)
                        self.state.insert_event(trade_id, "SL_MOVED_TP1", tp1_price)
                    
                    msg = (
                        f"✅ *TP2 Hit — Partial Close*\n"
                        f"Pair: `{pair}` | Ticket: `{ticket_display}`\n"
                        f"Closed: `{close_lot:.4f}` lot | Remaining: `{remaining_lot:.4f}` lot\n"
                        f"SL moved to TP1: `{tp1_price:.5f}` | Riding TP3..."
                    )
                    logger.info(f"[{pair}] TP2 Hit: closed {close_lot:.4f} | remaining {remaining_lot:.4f} | Ticket {ticket_display} | SL->TP1")
                    self._send_alert(msg)
                else:
                    logger.error(f"[{pair}] TP2 partial close failed: {close_result['error']}")
        else:
            # ── Original 1:2 FORMAT: close FULL remaining lot ──────────
            close_lot = self._round_to_volume_step(pair, live_volume)
            if close_lot <= 0:
                close_lot = live_volume  # Last resort: use raw value

            close_result = self.mt5.close_partial(ticket, close_lot)
            if close_result["success"]:
                profit = self.mt5.get_historical_profit(ticket)
                self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
                self.state.insert_event(trade_id, "TP2_HIT", current_price)
                self.state.add_daily_profit(today, profit)
                self.state.reset_consecutive_losses(today)
                msg = (
                    f"✅ *TP2 Hit — Trade CLOSED WIN* 🎉\n"
                    f"Pair: `{pair}` | Ticket: `{ticket}`\n"
                    f"Lot Closed: `{close_lot:.4f}` | Price: `{current_price:.5f}`\n"
                    f"Total P&L: `+{profit:.2f} USD`"
                )
                logger.info(f"[{pair}] TP2 Hit: CLOSED WIN | +{profit:.2f} USD")
                self._send_alert(msg)
                if self.reporter:
                    updated_trade = self.state.get_trade(trade_id)
                    signal = self.state.get_signal(trade.get("signal_id", ""))
                    self.reporter.log_trade(updated_trade, signal)
            else:
                logger.error(f"[{pair}] TP2 close failed: {close_result['error']}")

    def _execute_tp3(
        self,
        trade: Dict, trade_id: str, pair: str,
        ticket: int, current_price: float,
        lot_total: float, today: str
    ) -> None:
        """
        Close the full remaining position at TP3.
        """
        live_volume = self._get_open_volume(ticket)
        if live_volume is None or live_volume <= 0:
            logger.warning(f"[{pair}] TP3: No open volume on ticket {ticket}. Marking closed.")
            profit = self.mt5.get_historical_profit(ticket)
            self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
            self.state.insert_event(trade_id, "TP3_HIT", current_price)
            self.state.add_daily_profit(today, profit)
            self.state.reset_consecutive_losses(today)
            return

        close_lot = self._round_to_volume_step(pair, live_volume)
        if close_lot <= 0:
            close_lot = live_volume  # Last resort: use raw value

        close_result = self.mt5.close_partial(ticket, close_lot)
        if close_result["success"]:
            profit = self.mt5.get_historical_profit(ticket)
            self.state.update_trade_status(trade_id, "CLOSED", "WIN", profit)
            self.state.insert_event(trade_id, "TP3_HIT", current_price)
            self.state.add_daily_profit(today, profit)
            self.state.reset_consecutive_losses(today)
            msg = (
                f"🏆 *TP3 Hit — Trade FULLY CLOSED WIN* 🔥\n"
                f"Pair: `{pair}` | Ticket: `{ticket}`\n"
                f"Lot Closed: `{close_lot:.4f}` | Price: `{current_price:.5f}`\n"
                f"Total P&L: `+{profit:.2f} USD`"
            )
            logger.info(f"[{pair}] TP3 Hit: FULLY CLOSED WIN | +{profit:.2f} USD")
            self._send_alert(msg)
            if self.reporter:
                updated_trade = self.state.get_trade(trade_id)
                signal = self.state.get_signal(trade.get("signal_id", ""))
                self.reporter.log_trade(updated_trade, signal)
        else:
            logger.error(f"[{pair}] TP3 close failed: {close_result['error']}")

    # ------------------------------------------------------------------
    # CASE B — Split-ticket (legacy MMXM 0.02 lot, unchanged behaviour)
    # ------------------------------------------------------------------

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
        Behaviour is unchanged for backward compatibility.
        """
        ticket1_alive = self._ticket_exists_in_mt5(ticket1)
        ticket2_alive = self._ticket_exists_in_mt5(ticket2)

        # ── TP1 phase: close ticket1 at TP1 ──────────────────────────
        if tp1_hit == 0:
            if self._price_reached(current_price, tp1_price, direction):
                if ticket1_alive:
                    close_result = self.mt5.close_partial(ticket1, 0.01)
                    if close_result["success"]:
                        # Move SL on ticket2 to breakeven (no buffer for split order — original behaviour)
                        self.mt5.modify_sl(ticket2, entry_price)
                        self.state.update_trade_tp1_hit(trade_id)
                        self.state.update_trade_be_moved(trade_id)
                        self.state.insert_event(trade_id, "TP1_HIT", current_price)
                        self.state.insert_event(trade_id, "BE_MOVED", entry_price)
                        msg = (
                            f"📊 TP1 Hit (ticket {ticket1} closed) + SL->BE on {ticket2} | {pair}"
                        )
                        logger.info(msg)
                        self._send_alert(msg)
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
                        msg = (
                            f"✅ *TP2 Hit — Trade CLOSED WIN* 🎉\n"
                            f"Pair: `{pair}` | P&L: `+{profit:.2f} USD`"
                        )
                        logger.info(f"[{pair}] Case B TP2 Hit: CLOSED WIN | +{profit:.2f} USD")
                        self._send_alert(msg)
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

    # ------------------------------------------------------------------
    # UNEXPECTED CLOSE (SL or manual)
    # ------------------------------------------------------------------

    def _handle_unexpected_close(
        self,
        trade: Dict, trade_id: str, pair: str, ticket: int,
        current_price: float, entry_price: float, direction: str, today: str
    ) -> None:
        """
        Handle a position closed outside the normal TP flow (SL hit or manual close).
        Determines LOSS vs BREAKEVEN and applies daily guards.
        """
        profit_usd = self.mt5.get_historical_profit(ticket)

        if profit_usd >= -0.01 and int(trade.get("be_moved", 0)) and profit_usd < 5.0:
            result = "BREAKEVEN"
            event_type = "BREAKEVEN"
        else:
            result = "WIN" if profit_usd > 0 else "LOSS"
            if result == "WIN":
                if float(trade.get("tp3", 0.0)) > 0:
                    event_type = "TP3_HIT"
                else:
                    event_type = "TP2_HIT"
            else:
                event_type = "SL_HIT"

        self.state.update_trade_status(trade_id, "CLOSED", result, round(profit_usd, 2))
        self.state.insert_event(trade_id, event_type, current_price)

        if event_type in ["TP2_HIT", "TP3_HIT"]:
            msg = (
                f"✅ *{event_type.replace('_', ' ')} — Trade CLOSED WIN*\n"
                f"Pair: `{pair}` | Ticket: `{ticket}` | Price: `{current_price:.5f}`\n"
                f"P&L: `+{profit_usd:.2f} USD`"
            )
        elif result == "BREAKEVEN":
            msg = (
                f"🟧 *Trade CLOSED BREAKEVEN*\n"
                f"Pair: `{pair}` | Ticket: `{ticket}` | Price: `{current_price:.5f}`\n"
                f"P&L: `{profit_usd:.2f} USD` _(SL at breakeven)_"
            )
        else:
            msg = (
                f"❌ *Trade CLOSED {result}*\n"
                f"Pair: `{pair}` | Ticket: `{ticket}` | Price: `{current_price:.5f}`\n"
                f"P&L: `{profit_usd:.2f} USD`"
            )
        
        logger.info(f"[{pair}] Trade {trade_id} closed: {result} | {profit_usd:.2f} USD")
        self._send_alert(msg)

        if self.reporter:
            updated_trade = self.state.get_trade(trade_id)
            signal = self.state.get_signal(trade.get("signal_id", ""))
            self.reporter.log_trade(updated_trade, signal)

        self._handle_loss_guards(today, profit_usd, result, pair)

    # ------------------------------------------------------------------
    # MAIN LOOP
    # ------------------------------------------------------------------

    def monitor_all_trades(self) -> None:
        """Fetch all open and pending trades and monitor each one."""
        pending_trades = self.state.get_pending_trades()
        for trade in pending_trades:
            try:
                self.monitor_pending_trade(trade)
            except Exception as e:
                err = traceback.format_exc()
                logger.error(f"Error monitoring pending trade {trade.get('trade_id')}: {e}\n{err}")

        open_trades = self.state.get_open_trades()
        total_unrealized_pnl = 0.0

        for trade in open_trades:
            try:
                self.monitor_trade(trade)
                
                # Add to unrealized PnL for Vault
                ticket_id_raw = str(trade["ticket_id"])
                tickets = [int(t.strip()) for t in ticket_id_raw.split(",")]
                for tk in tickets:
                    total_unrealized_pnl += self._get_profit_for_ticket(tk)
            except Exception as e:
                err = traceback.format_exc()
                logger.error(f"Error monitoring trade {trade.get('trade_id')}: {e}\n{err}")

        # Real-time Vault Drawdown check
        if self.vault:
            try:
                panic = self.vault.check_drawdown(self.state, self.mt5, total_unrealized_pnl)
                if panic:
                    self._send_alert("🚨 VAULT DRAWDOWN PANIC 🚨\n-20% Limit Reached. All positions closed and bot disabled for today.")
            except Exception as e:
                logger.error(f"Error checking vault drawdown: {e}")

    def monitor_pending_trade(self, trade: Dict[str, Any]) -> None:
        """
        Check if a PENDING order has triggered or been cancelled.
        """
        ticket = int(trade["ticket_id"])
        trade_id = trade["trade_id"]
        pair = trade["pair"]

        if self._order_exists_in_mt5(ticket):
            # Still pending, nothing to do
            return

        import MetaTrader5 as mt5
        # The order is no longer in active orders. Check if it was filled.
        history_orders = mt5.history_orders_get(ticket=ticket)
        if history_orders and len(history_orders) > 0:
            h_order = history_orders[0]
            if h_order.state == mt5.ORDER_STATE_FILLED:
                # Order was filled! Find the position ID.
                position_id = getattr(h_order, "position_id", ticket)
                
                # Check if this position is actually open in MT5
                if self._ticket_exists_in_mt5(position_id):
                    if str(position_id) != str(ticket):
                        self.state.update_trade_ticket(trade_id, str(position_id))
                        ticket_display = f"{ticket} -> {position_id}"
                    else:
                        ticket_display = str(ticket)
                    
                    executed_price = trade.get("executed_price", 0.0)
                    sl = trade.get("sl", 0.0)
                    tp1 = trade.get("tp1", 0.0)
                    msg = (
                        f"🔔 *Pending Order TRIGGERED — Now OPEN*\n"
                        f"Pair: `{pair}` | Ticket: `{ticket_display}`\n"
                        f"Entry: `{executed_price:.5f}` | SL: `{sl:.5f}` | TP1: `{tp1:.5f}`\n"
                        f"TradeManager is now managing this position."
                    )
                    logger.info(f"[{pair}] Pending order triggered: ticket {ticket_display}")
                    self._send_alert(msg)
                    self.state.update_trade_status(trade_id, "OPEN", "PENDING_TRIGGERED", 0.0)
                    
                    if self.reporter:
                        updated_trade = self.state.get_trade(trade_id)
                        signal = self.state.get_signal(trade.get("signal_id", ""))
                        self.reporter.log_trade(updated_trade, signal)
                    return
                else:
                    # It was filled, but the position is already closed!
                    profit_usd = self.mt5.get_historical_profit(position_id)
                    result = "WIN" if profit_usd > 0 else "LOSS" if profit_usd < 0 else "BREAKEVEN"
                    icon = "✅" if result == "WIN" else "❌" if result == "LOSS" else "🟧"
                    msg = (
                        f"{icon} *Pending Order TRIGGERED & INSTANTLY CLOSED*\n"
                        f"Pair: `{pair}` | Result: `{result}` | P&L: `{profit_usd:.2f} USD`"
                    )
                    logger.info(f"[{pair}] Pending triggered & instantly closed: {result} ({profit_usd:.2f} USD)")
                    self._send_alert(msg)
                    self.state.update_trade_status(trade_id, "CLOSED", result, profit_usd)
                    
                    if self.reporter:
                        updated_trade = self.state.get_trade(trade_id)
                        signal = self.state.get_signal(trade.get("signal_id", ""))
                        self.reporter.log_trade(updated_trade, signal)
                    return
                    
            elif h_order.state in [mt5.ORDER_STATE_CANCELED, mt5.ORDER_STATE_REJECTED, mt5.ORDER_STATE_EXPIRED]:
                state_name = {
                    mt5.ORDER_STATE_CANCELED: "CANCELLED",
                    mt5.ORDER_STATE_REJECTED: "REJECTED",
                    mt5.ORDER_STATE_EXPIRED: "EXPIRED"
                }.get(h_order.state, "CANCELLED")
                msg = (
                    f"🗑️ *Pending Order {state_name}*\n"
                    f"Pair: `{pair}` | Ticket: `{ticket}`"
                )
                logger.info(f"[{pair}] Pending order {state_name}: ticket {ticket}")
                self._send_alert(msg)
                self.state.update_trade_status(trade_id, "CLOSED", "CANCELLED", 0.0)
                
                if self.reporter:
                    updated_trade = self.state.get_trade(trade_id)
                    signal = self.state.get_signal(trade.get("signal_id", ""))
                    self.reporter.log_trade(updated_trade, signal)
                return

        # Fallback if it's somehow completely missing
        msg = (
            f"❓ *Pending Order Missing from MT5*\n"
            f"Pair: `{pair}` | Ticket: `{ticket}` _(Marked as CANCELLED)_"
        )
        logger.info(f"[{pair}] Pending order missing from MT5: ticket {ticket}")
        self._send_alert(msg)
        self.state.update_trade_status(trade_id, "CLOSED", "CANCELLED", 0.0)
        
        if self.reporter:
            updated_trade = self.state.get_trade(trade_id)
            signal = self.state.get_signal(trade.get("signal_id", ""))
            self.reporter.log_trade(updated_trade, signal)

    def start_monitoring(self) -> None:
        """Start the trade monitoring loop in a daemon background thread."""
        def _loop():
            logger.info("TradeManager monitoring thread started.")
            while True:
                try:
                    self.monitor_all_trades()
                except Exception as e:
                    logger.error(f"TradeManager loop error: {e}\n{traceback.format_exc()}")
                time.sleep(_MONITOR_INTERVAL_SECONDS)

        thread = threading.Thread(target=_loop, daemon=True, name="TradeMonitor")
        thread.start()
