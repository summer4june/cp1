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

        self.bot = telebot.TeleBot(self.config.telegram_token, threaded=False)
        self.chat_id = self.config.telegram_chat_id

        # Thread-safe storage: signal_id → {signal_dict, lot_size, timestamp}
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

        self._register_handlers()

    # ------------------------------------------------------------------
    # HANDLER REGISTRATION
    # ------------------------------------------------------------------

    def _register_handlers(self):
        """Register inline keyboard callback handlers with the bot."""

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("YES_"))
        def on_yes(call):
            """Handle YES EXECUTE button press."""
            signal_id = call.data[len("YES_"):]
            self.bot.answer_callback_query(call.id)
            self.handle_yes_callback(signal_id)

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("NO_"))
        def on_no(call):
            """Handle NO SKIP button press."""
            signal_id = call.data[len("NO_"):]
            self.bot.answer_callback_query(call.id)
            self.handle_no_callback(signal_id)

        @self.bot.callback_query_handler(func=lambda c: c.data.startswith("REASON_"))
        def on_reason(call):
            """Handle skip reason button press."""
            # Format: REASON_{signal_id}_{reason}
            parts = call.data[len("REASON_"):].split("_", 1)
            if len(parts) == 2:
                signal_id, reason = parts
                self.bot.answer_callback_query(call.id)
                self.handle_reason_callback(signal_id, reason)

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

        thread = threading.Thread(target=_poll, daemon=True, name="TelegramListener")
        thread.start()

    def send_signal(self, signal: Dict[str, Any], lot_size: float) -> bool:
        """
        Format and send an A+ signal with YES/NO approval buttons.

        Args:
            signal (dict): Full signal dictionary from ScannerMMXM.
            lot_size (float): Calculated lot size from RiskEngine.

        Returns:
            bool: True if sent successfully.
        """
        signal_id = signal["signal_id"]
        timestamp_ist = (
            datetime.now(_IST).strftime("%Y-%m-%d %H:%M:%S")
        )

        text = (
            "🤖 *VvE FxBOT — A+ SIGNAL DETECTED*\n\n"
            f"📊 Pair: `{signal['pair']}`\n"
            f"🕐 Session: `{signal['session']}`\n"
            f"📈 Direction: `{signal['direction']}`\n"
            f"⚡ Setup: `{signal.get('strategy', 'ICT MMXM')}`\n\n"
            f"Entry: `{signal['entry_price']}`\n"
            f"SL: `{signal['sl_price']}` (`{signal['sl_pips']}` pips)\n"
            f"TP1: `{signal['tp1_price']}` (1R)\n"
            f"TP2: `{signal['tp2_price']}` (2R)\n\n"
            f"Risk: `{self.config.risk_percent}%`\n"
            f"Lot: `{lot_size}`\n"
            f"Spread: `{signal['spread_pips']}` pips\n"
            f"Eff. RR: `{signal['effective_rr']}`\n"
            f"Score: `{signal['score']}/100`\n\n"
            f"🕐 `{timestamp_ist}` IST\n"
            f"Signal ID: `{signal_id}`"
        )

        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ YES EXECUTE", callback_data=f"YES_{signal_id}"),
            InlineKeyboardButton("❌ NO SKIP", callback_data=f"NO_{signal_id}"),
        )

        try:
            self.bot.send_message(self.chat_id, text, parse_mode="Markdown", reply_markup=markup)
            with self._lock:
                self._pending[signal_id] = {
                    "signal": signal,
                    "lot_size": lot_size,
                    "timestamp": datetime.now(timezone.utc),
                }
            logger.info(f"Signal sent to Telegram: {signal_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send signal to Telegram: {e}")
            return False

    def send_alert(self, message: str) -> None:
        """
        Send a plain text alert message.

        Args:
            message (str): Alert content (daily stop, loss streak, errors, etc.).
        """
        try:
            self.bot.send_message(self.chat_id, message)
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")

    def handle_yes_callback(self, signal_id: str) -> None:
        """
        Handle YES EXECUTE button press.

        Validates signal is pending and not expired (> 15 min). Then calls
        execution_callback(signal_id) and removes from pending.

        Args:
            signal_id (str): Signal UUID.
        """
        with self._lock:
            pending = self._pending.get(signal_id)

        if not pending:
            self.bot.send_message(self.chat_id, f"⚠️ Signal `{signal_id}` not found or already processed.", parse_mode="Markdown")
            return

        age = datetime.now(timezone.utc) - pending["timestamp"]
        if age > timedelta(minutes=_SIGNAL_EXPIRY_MINUTES):
            with self._lock:
                self._pending.pop(signal_id, None)
            self.bot.send_message(
                self.chat_id,
                f"⏰ Signal expired: `{signal_id}`\nSignals are only valid for {_SIGNAL_EXPIRY_MINUTES} minutes.",
                parse_mode="Markdown"
            )
            logger.warning(f"Signal expired on YES press: {signal_id}")
            return

        # Remove before calling execution to avoid double-execution
        with self._lock:
            self._pending.pop(signal_id, None)

        try:
            self.execution_callback(signal_id)
            self.bot.send_message(
                self.chat_id,
                f"✅ Trade execution triggered for `{signal_id}`",
                parse_mode="Markdown"
            )
            logger.info(f"Execution callback triggered for signal: {signal_id}")
        except Exception as e:
            logger.error(f"Execution callback failed for {signal_id}: {e}")
            self.bot.send_message(self.chat_id, f"❌ Execution failed: {e}")

    def handle_no_callback(self, signal_id: str) -> None:
        """
        Handle NO SKIP button press.

        Removes signal from pending and sends skip reason keyboard.

        Args:
            signal_id (str): Signal UUID.
        """
        with self._lock:
            self._pending.pop(signal_id, None)

        markup = InlineKeyboardMarkup(row_width=2)
        buttons = [
            InlineKeyboardButton(reason, callback_data=f"REASON_{signal_id}_{reason}")
            for reason in _SKIP_REASONS
        ]
        markup.add(*buttons)

        try:
            self.bot.send_message(
                self.chat_id,
                "Select skip reason:",
                reply_markup=markup
            )
        except Exception as e:
            logger.error(f"Failed to send skip reason keyboard: {e}")

    def handle_reason_callback(self, signal_id: str, reason: str) -> None:
        """
        Handle skip reason button press.

        Logs the skip to state_engine and sends confirmation.

        Args:
            signal_id (str): Signal UUID.
            reason (str): Human-readable skip reason.
        """
        try:
            # Retrieve stored spread/score from pending if available,
            # or fall back to 0.0 if the signal has already been cleared
            with self._lock:
                pending = self._pending.pop(signal_id, None)

            spread = pending["signal"].get("spread_pips", 0.0) if pending else 0.0
            score = pending["signal"].get("score", 0.0) if pending else 0.0

            self.state.insert_skip(signal_id, reason, spread, score)
            logger.info(f"Signal {signal_id} skipped: {reason}")

            self.bot.send_message(
                self.chat_id,
                f"❌ Signal `{signal_id}` skipped: *{reason}*",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Failed to record skip reason for {signal_id}: {e}")
