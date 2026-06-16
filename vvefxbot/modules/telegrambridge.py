import threading
import pytz
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, Any, Optional
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from core.logger import get_logger
from core.configengine import Config
from core.stateengine import StateEngine

logger = get_logger("TelegramBridge")

_IST = pytz.timezone("Asia/Kolkata")
_SIGNAL_EXPIRY_MINUTES = 15

_SKIP_REASONS = [
    "Spread High",
    "News Window",
    "Weak Displacement",
    "Fake Sweep",
    "Late Entry",
    "Bad Session",
    "Structure Unclear",
    "Manual Reject",
]


class TelegramBridge:
    """Telegram notification and execution approval bridge for VvE FxBOT."""

    def __init__(
        self,
        config: Config,
        state_engine: StateEngine,
        execution_callback: Callable[[str], None],
        reporter: Optional[Any] = None,
    ):
        """
        Initializes the TelegramBridge.

        Args:
            config (Config): Validated configuration dataclass (contains token + chat_id).
            state_engine (StateEngine): Persistence engine.
            execution_callback (Callable): Called with signal_id when user presses YES.
        """
        self.config = config
        self.state = state_engine
        self.execution_callback = execution_callback
        self.reporter = reporter

        self.bot = telebot.TeleBot(self.config.telegram_token, threaded=False)
        self.chat_ids = self.config.telegram_chat_ids

        # Thread-safe storage: signal_id → {signal_dict, lot_size, timestamp}
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

        self._register_handlers()

    # ------------------------------------------------------------------
    # HANDLER REGISTRATION
    # ------------------------------------------------------------------

    def _answer_callback_safe(self, call_id: str):
        """Safely answer callback queries, ignoring 'too old' timeouts."""
        try:
            self.bot.answer_callback_query(call_id)
        except Exception as e:
            logger.debug(f"Could not answer callback query {call_id} (may be too old): {e}")

    def _register_handlers(self):
        """Register inline keyboard callback handlers with the bot."""

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("YES_"))
        def on_yes(call):
            """Handle YES EXECUTE button press."""
            signal_id = call.data[len("YES_"):]
            self._answer_callback_safe(call.id)
            self.handle_yes_callback(signal_id, call.message.chat.id, call.message.message_id)

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("NO_"))
        def on_no(call):
            """Handle NO SKIP button press."""
            signal_id = call.data[len("NO_"):]
            self._answer_callback_safe(call.id)
            self.handle_no_callback(signal_id, call.message.chat.id, call.message.message_id)

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("REASON_"))
        def on_reason(call):
            """Handle skip reason button press."""
            # Format: REASON_{signal_id}_{reason}
            parts = call.data[len("REASON_"):].split("_", 1)
            if len(parts) == 2:
                signal_id, reason = parts
                self._answer_callback_safe(call.id)
                self.handle_reason_callback(signal_id, reason, call.message.chat.id, call.message.message_id)

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("MANUAL_REASON_"))
        def on_manual_reason(call):
            """Handle Manual Reason button press."""
            signal_id = call.data[len("MANUAL_REASON_"):]
            self._answer_callback_safe(call.id)
            self.handle_manual_reason_prompt(signal_id, call.message.chat.id, call.message.message_id)

    # ------------------------------------------------------------------
    # PUBLIC METHODS
    # ------------------------------------------------------------------

    def start_listener(self):
        """Start Telegram polling in a background daemon thread (non-blocking)."""
        def _poll():
            """Internal polling loop with error recovery."""
            logger.info("Telegram polling thread started.")
            while True:
                try:
                    self.bot.polling(none_stop=True, timeout=30, long_polling_timeout=30)
                except Exception as e:
                    logger.error(f"Telegram polling error: {e}. Restarting in 5s.")
                    import time
                    time.sleep(5)
                    
        def _cleanup():
            """Internal loop to auto-reject signals older than 15 minutes."""
            import time
            from datetime import datetime, timezone, timedelta
            logger.info("Telegram cleanup thread started.")
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    expired_ids = []
                    with self._lock:
                        for sig_id, data in self._pending.items():
                            age = now - data["timestamp"]
                            if age > timedelta(minutes=_SIGNAL_EXPIRY_MINUTES):
                                expired_ids.append(sig_id)
                                
                    for sig_id in expired_ids:
                        self.handle_reason_callback(sig_id, "Ignore", None)
                except Exception as e:
                    logger.error(f"Telegram cleanup error: {e}")
                time.sleep(60)

        thread = threading.Thread(target=_poll, daemon=True, name="TelegramListener")
        thread.start()
        
        cleanup_thread = threading.Thread(target=_cleanup, daemon=True, name="TelegramCleanup")
        cleanup_thread.start()

    def broadcast_message(self, text: str) -> None:
        """Sends a plain text message to all configured chat IDs."""
        for chat_id in self.chat_ids:
            try:
                self.bot.send_message(chat_id, text, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Failed to broadcast message to {chat_id}: {e}")

    def send_signal(self, signal: Dict[str, Any], lot_size: float, usd_metrics: Dict[str, float] = None) -> bool:
        """
        Format and send an A+ signal with YES/NO approval buttons.

        Args:
            signal (dict): Full signal dictionary from Scanner.
            lot_size (float): Calculated lot size from RiskEngine.
            usd_metrics (dict): Exact SL/TP and margin calculations in USD.

        Returns:
            bool: True if sent successfully.
        """
        signal_id = signal["signal_id"]
        timestamp_ist = (
            datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")
        )

        usd = usd_metrics or {}
        sl_usd = usd.get("sl_usd", 0.0)
        tp1_usd = usd.get("tp1_usd", 0.0)
        tp2_usd = usd.get("tp2_usd", 0.0)
        tp3_usd = usd.get("tp3_usd", 0.0)
        margin_usd = usd.get("margin_usd", 0.0)
        
        tp3_price_str = signal.get("tp3_price", "") if signal.get("tp3_price") else "N/A"

        display_kz = signal.get("killzone", "None")

        text = (
            f"Time : `{timestamp_ist}`\n\n"
            f"Pair: `{signal['pair']}`\n"
            f"Killzone: `{display_kz}`\n"
            f"Direction: `{signal['direction']}`\n"
            f"Entry Leg: `{signal.get('entry_leg', 'A')}`\n\n"
            f"Entry: `{signal['entry_price']}`\n"
            f"SL: `{signal['sl_price']}`\n"
            f"TP1: `{signal['tp1_price']}`\n"
            f"TP2: `{signal['tp2_price']}`\n"
            f"TP3: `{tp3_price_str}`\n\n"
            f"SL USD: `${sl_usd:.2f}`\n"
            f"TP1 USD : `${tp1_usd:.2f}`\n"
            f"TP2 USD : `${tp2_usd:.2f}`\n"
            f"TP3 USD : `${tp3_usd:.2f}`\n\n"
            f"Margin will use : `${margin_usd:.2f}`\n"
            f"Lot: `{lot_size}`\n"
            f"Spread: `{signal['spread_pips']}`\n"
            f"Eff RR: `{signal['effective_rr']}`\n"
            f"Score: `{signal['score']}`\n\n"
            f"Signal ID: `{signal_id}`"
        )

        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ YES EXECUTE", callback_data=f"YES_{signal_id}"),
            InlineKeyboardButton("❌ NO SKIP", callback_data=f"NO_{signal_id}"),
        )

        try:
            sent_messages = []
            for cid in self.chat_ids:
                try:
                    msg = self.bot.send_message(cid, text, parse_mode="Markdown", reply_markup=markup)
                    sent_messages.append((cid, msg.message_id))
                except Exception as e:
                    logger.error(f"Failed to send signal to chat {cid}: {e}")
                    
            with self._lock:
                self._pending[signal_id] = {
                    "signal": signal,
                    "lot_size": lot_size,
                    "timestamp": datetime.now(timezone.utc),
                    "messages": sent_messages
                }
            logger.info(f"Signal sent to Telegram broadcast list: {signal_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to process signal broadcast: {e}")
            return False

    def send_alert(self, message: str) -> None:
        """
        Send a plain text alert message to all chat IDs.

        Args:
            message (str): Alert content (daily stop, loss streak, errors, etc.).
        """
        for cid in self.chat_ids:
            try:
                self.bot.send_message(cid, message)
            except Exception as e:
                logger.error(f"Failed to send Telegram alert to {cid}: {e}")

    def handle_yes_callback(self, signal_id: str, trigger_chat_id: int, message_id: int = None) -> None:
        """
        Handle YES EXECUTE button press.

        Validates signal is pending and not expired (> 15 min). Then calls
        execution_callback(signal_id) and removes from pending.

        Args:
            signal_id (str): Signal UUID.
            trigger_chat_id (int): Chat ID of the user who clicked YES.
            message_id (int): Message ID of the original signal message (for button removal).
        """
        with self._lock:
            pending = self._pending.get(signal_id)

        if not pending:
            self.bot.send_message(trigger_chat_id, f"⚠️ Signal `{signal_id}` not found or already processed.", parse_mode="Markdown")
            return

        age = datetime.now(timezone.utc) - pending["timestamp"]
        if age > timedelta(minutes=_SIGNAL_EXPIRY_MINUTES):
            # Log as expired using the standard rejection pathway
            self.handle_reason_callback(signal_id, "Expired (Late YES)", trigger_chat_id, message_id)
            return

        # Remove before calling execution to avoid double-execution
        with self._lock:
            self._pending.pop(signal_id, None)

        if pending and "messages" in pending:
            for cid, mid in pending["messages"]:
                try:
                    self.bot.edit_message_reply_markup(chat_id=cid, message_id=mid, reply_markup=None)
                except Exception:
                    pass

        try:
            self.execution_callback(signal_id)
            for cid in self.chat_ids:
                try:
                    self.bot.send_message(
                        cid,
                        f"✅ Trade execution triggered for `{signal_id}`",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            logger.info(f"Execution callback triggered for signal: {signal_id}")
        except Exception as e:
            logger.error(f"Execution callback failed for {signal_id}: {e}")
            for cid in self.chat_ids:
                try:
                    self.bot.send_message(cid, f"❌ Execution failed: {e}")
                except Exception:
                    pass

    def handle_no_callback(self, signal_id: str, trigger_chat_id: int, message_id: int = None) -> None:
        """
        Handle NO SKIP button press.

        Edits the original signal message to replace YES/NO buttons with skip reason keyboard.
        """
        with self._lock:
            pending = self._pending.get(signal_id)
            
        if not pending:
            self.bot.send_message(trigger_chat_id, f"⚠️ Signal `{signal_id}` not found or already processed.", parse_mode="Markdown")
            return
            
        markup = InlineKeyboardMarkup(row_width=2)
        buttons = [
            InlineKeyboardButton(reason, callback_data=f"REASON_{signal_id}_{reason}")
            for reason in _SKIP_REASONS
        ]
        buttons.append(InlineKeyboardButton("✍️ Manual Reason", callback_data=f"MANUAL_REASON_{signal_id}"))
        markup.add(*buttons)

        # First, update the specific message where the user clicked NO
        try:
            if trigger_chat_id and message_id:
                self.bot.edit_message_reply_markup(chat_id=trigger_chat_id, message_id=message_id, reply_markup=markup)
        except Exception as e:
            logger.error(f"Failed to edit skip reason keyboard for trigger chat: {e}")

        # Next, clear the YES/NO buttons from any other chats this signal was broadcast to
        if pending and "messages" in pending:
            for cid, mid in pending["messages"]:
                # Only clear if it's a different message ID to avoid overwriting the markup we just set
                if str(mid) != str(message_id):
                    try:
                        self.bot.edit_message_reply_markup(chat_id=cid, message_id=mid, reply_markup=None)
                    except Exception:
                        pass

    def handle_reason_callback(self, signal_id: str, reason: str, trigger_chat_id: Optional[int], message_id: int = None) -> None:
        """
        Handle skip reason button press or manual reason text.

        Logs the skip to state_engine, clears buttons on original message, and sends confirmation.

        Args:
            signal_id (str): Signal UUID.
            reason (str): Human-readable skip reason.
            trigger_chat_id (int): Chat ID of the user who clicked, or None for auto-rejections.
            message_id (int): Message ID of the original signal message (for button removal).
        """
        try:
            # Retrieve stored spread/score from pending
            with self._lock:
                pending = self._pending.pop(signal_id, None)

            # Clear keyboards for everyone if they haven't been cleared yet
            if pending and "messages" in pending:
                for cid, mid in pending["messages"]:
                    try:
                        self.bot.edit_message_reply_markup(chat_id=cid, message_id=mid, reply_markup=None)
                    except Exception:
                        pass

            if not pending and reason != "Ignore" and reason != "Expired (Late YES)":
                # If it's already popped, and it's not our internal auto-rejections, ignore it
                if trigger_chat_id:
                    self.bot.send_message(trigger_chat_id, f"⚠️ Signal `{signal_id}` already processed or expired.", parse_mode="Markdown")
                return

            spread = pending["signal"].get("spread_pips", 0.0) if pending else 0.0
            score = pending["signal"].get("score", 0.0) if pending else 0.0

            self.state.insert_skip(signal_id, reason, spread, score)
            
            # Identify who rejected it for the log
            user_msg = "by a user" if trigger_chat_id else "automatically"
            logger.info(f"Signal {signal_id} skipped {user_msg}: {reason}")

            # Optionally log denied signal to Denied Google Sheet
            if pending and self.reporter:
                signal_dict = pending.get("signal")
                if signal_dict:
                    self.reporter.log_denied_trade(signal_dict, reason)

            # Broadcast skip to all users
            for cid in self.chat_ids:
                try:
                    self.bot.send_message(
                        cid,
                        f"❌ Signal `{signal_id}` skipped: *{reason}*",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed to record skip reason for {signal_id}: {e}")

    def handle_manual_reason_prompt(self, signal_id: str, trigger_chat_id: int, message_id: int = None) -> None:
        """
        Prompt the user to type a manual reason.
        """
        try:
            # Optionally clear the reason keyboard while they are typing to prevent double clicks
            with self._lock:
                pending = self._pending.get(signal_id)
            if pending and "messages" in pending:
                for cid, mid in pending["messages"]:
                    try:
                        self.bot.edit_message_reply_markup(chat_id=cid, message_id=mid, reply_markup=None)
                    except Exception:
                        pass

            msg = self.bot.send_message(
                trigger_chat_id,
                "Please type your manual reason for rejecting this trade:",
                parse_mode="Markdown"
            )
            self.bot.register_next_step_handler(msg, self._receive_manual_reason, signal_id, trigger_chat_id)
        except Exception as e:
            logger.error(f"Failed to send manual reason prompt: {e}")

    def _receive_manual_reason(self, message, signal_id: str, trigger_chat_id: int) -> None:
        """Callback to receive the typed text for manual rejection."""
        if not message.text:
            self.bot.send_message(trigger_chat_id, "No text provided. Skip action cancelled.")
            return
            
        reason = message.text.strip()
        self.handle_reason_callback(signal_id, reason, trigger_chat_id)
