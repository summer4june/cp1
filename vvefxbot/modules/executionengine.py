import uuid
import traceback
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from core.stateengine import StateEngine
from modules.riskengine import RiskEngine
from modules.telegrambridge import TelegramBridge

logger = get_logger("ExecutionEngine")

_FAIL = lambda error: {"success": False, "error": error}


class ExecutionEngine:
    """Handles trade execution triggered by Telegram YES approval."""

    def __init__(
        self,
        config: Config,
        mt5connector: MT5Connector,
        risk_engine: RiskEngine,
        state_engine: StateEngine,
        telegram_bridge: TelegramBridge,
    ):
        """
        Initializes the ExecutionEngine.

        Args:
            config (Config): Validated configuration dataclass.
            mt5connector (MT5Connector): Live MT5 connection.
            risk_engine (RiskEngine): Risk and lot calculation engine.
            state_engine (StateEngine): Persistence engine.
            telegram_bridge (TelegramBridge): For sending execution alerts.
        """
        self.config = config
        self.mt5 = mt5connector
        self.risk = risk_engine
        self.state = state_engine
        self.telegram = telegram_bridge

    # ------------------------------------------------------------------
    # CORRELATION HELPER
    # ------------------------------------------------------------------

    def _get_correlation_group(self, pair: str) -> str | None:
        """
        Return the correlation group key for a given pair.

        Args:
            pair (str): Trading symbol.

        Returns:
            str | None: Group key ("A", "B", "C", ...) or None if not in any group.
        """
        for group_key, members in self.config.correlation_groups.items():
            if pair in members:
                return group_key
        return None

    # ------------------------------------------------------------------
    # MAIN EXECUTION METHOD
    # ------------------------------------------------------------------

    def execute_signal(self, signal_id: str) -> Dict[str, Any]:
        """
        Execute a trade for a given signal ID following the 13-step flow.

        Args:
            signal_id (str): UUID of the signal to execute.

        Returns:
            dict: {"success": bool, "ticket": int, "lot": float} on success,
                  {"success": False, "error": str} on failure.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # ── STEP 1: Fetch signal ──────────────────────────────────────
        signal = self.state.get_signal(signal_id)
        if not signal:
            logger.error(f"Signal not found in DB: {signal_id}")
            return _FAIL("Signal not found")

        pair = signal["pair"]
        direction = signal["direction"]
        spread_pips_at_signal = signal.get("spread_pips", 0.0)
        score = signal.get("score", 0.0)

        # ── STEP 2: Re-check spread at execution time ─────────────────
        current_spread = self.mt5.get_current_spread(pair)
        if current_spread < 0:
            logger.error(f"EXECUTION_BLOCKED: could not read spread for {pair}")
            return _FAIL("Spread unavailable at execution")

        spread_limit = self.config.spread_limits.get(pair, 9999.0)
        if current_spread > spread_limit:
            msg = f"EXECUTION_BLOCKED: spread too high at execution ({current_spread} > {spread_limit})"
            logger.warning(f"[{pair}] {msg}")
            return _FAIL(msg)

        # Update live spread into the signal dict for risk checks
        signal["spread_pips"] = current_spread

        # ── STEP 3: Risk checks ───────────────────────────────────────
        open_trades = self.state.get_open_trades()
        risk_result = self.risk.run_all_checks(signal, open_trades)
        if not risk_result["pass"]:
            reason = risk_result["failed_check"]
            logger.warning(f"[{pair}] Execution blocked by risk check: {reason}")
            self.state.insert_skip(signal_id, reason, current_spread, score)
            return _FAIL(f"Risk check failed: {reason}")

        # ── STEP 4: Correlation filter ────────────────────────────────
        from modules.correlationfilter import CorrelationFilter
        correlation_filter = CorrelationFilter(self.config)
        allowed, reason = correlation_filter.can_trade(pair, open_trades, signal["direction"])
        if not allowed:
            msg = f"Correlation block: {reason}"
            logger.warning(f"[{pair}] {msg}")
            self.state.insert_skip(signal_id, reason, current_spread, score)
            return _FAIL(msg)

        # ── STEP 5: Daily guards ──────────────────────────────────────
        if self.state.is_bot_disabled_today(today):
            logger.warning(f"[{pair}] Bot disabled for today. Blocking execution.")
            return _FAIL("Bot disabled today")

        pair_trades_today = self.state.get_pair_trades_today(pair)
        if pair_trades_today >= self.config.max_trades_pair_day:
            msg = f"Max trades per pair reached ({pair_trades_today}/{self.config.max_trades_pair_day})"
            logger.warning(f"[{pair}] {msg}")
            return _FAIL(msg)

        daily_state = self.state.get_daily_state(today)
        if daily_state.get("total_trades", 0) >= self.config.max_trades_day:
            msg = f"Daily trade limit reached ({daily_state['total_trades']}/{self.config.max_trades_day})"
            logger.warning(f"[{pair}] {msg}")
            return _FAIL(msg)

        # ── STEP 6: Lot size ──────────────────────────────────────────
        sl_pips = signal["sl_pips"]
        lot_size = risk_result["lot_size"]
        risk_amount = self.config.trading_pool_size * (self.config.risk_percent / 100.0)

        # ── STEP 7: Place order ───────────────────────────────────────
        order_result = self.mt5.place_order(
            symbol=pair,
            order_type=direction,
            lot=lot_size,
            sl=signal["sl_price"],
            tp=signal["tp2_price"],
            comment="VvE"
        )

        if not order_result["success"]:
            logger.error(f"[{pair}] Order placement failed: {order_result['error']}")
            return _FAIL(f"Order failed: {order_result['error']}")

        ticket = order_result["ticket"]
        executed_price = signal["entry_price"]  # Best estimate; actual fill logged below

        # Try to get the real executed price from MT5
        try:
            import MetaTrader5 as mt5_lib
            positions = mt5_lib.positions_get(ticket=ticket)
            if positions:
                executed_price = positions[0].price_open
        except Exception:
            pass  # Fallback to entry price estimate

        # ── STEP 8: Slippage check (post-placement) ───────────────────
        if not self.risk.check_slippage(signal["entry_price"], executed_price, pair):
            logger.warning(f"[{pair}] SLIPPAGE_TOO_HIGH — closing ticket {ticket} immediately.")
            close_result = self.mt5.close_partial(ticket, lot_size)
            if not close_result["success"]:
                logger.error(f"[{pair}] Failed to close high-slippage order: {close_result['error']}")
            return _FAIL("Slippage too high — order closed")

        # ── STEP 9: Persist trade to DB ───────────────────────────────
        now_utc = datetime.now(timezone.utc).isoformat()
        trade_id = str(uuid.uuid4())

        self.state.insert_trade({
            "trade_id": trade_id,
            "signal_id": signal_id,
            "ticket_id": ticket,
            "pair": signal["pair"],
            "direction": signal["direction"],
            "executed_price": executed_price,
            "sl": signal["sl_price"],
            "tp1": signal["tp1_price"],
            "tp2": signal["tp2_price"],
            "lot_total": lot_size,
            "risk_amount": risk_amount,
            "execution_time": now_utc,
            "status": "OPEN",
            "result": None,
            "profit_usd": 0.0,
        })

        # ── STEP 10: Set pair cooldown ────────────────────────────────
        cooldown_until = datetime.now(timezone.utc) + timedelta(minutes=15)
        self.state.set_pair_cooldown(pair, cooldown_until)

        # ── STEP 11: Increment counters ───────────────────────────────
        self.state.increment_pair_trades(pair)
        self.state.increment_daily_trades(today)

        # ── STEP 12: Telegram confirmation ────────────────────────────
        alert = f"✅ Trade OPEN | {pair} {direction} | Ticket: {ticket}"
        self.telegram.send_alert(alert)
        logger.info(alert)

        # ── STEP 13: Return success ───────────────────────────────────
        return {"success": True, "ticket": ticket, "lot": lot_size}
