import time
import threading
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from core.configengine import ConfigEngine
from core.logger import get_logger
from core.stateengine import StateEngine
from core.mt5connector import MT5Connector
from modules.sessionengine import SessionEngine
from modules.correlationfilter import CorrelationFilter
from modules.riskengine import RiskEngine
from modules.reportgoogle import GoogleSheetReporter
from modules.trademanager import TradeManager
from modules.executionengine import ExecutionEngine
from modules.telegrambridge import TelegramBridge
from modules.scannermmxm import ScannerMMXM

logger = get_logger("Main")


def heartbeat_loop(connector: MT5Connector, interval: int = 60):
    """
    Daemon thread: log MT5 connection status and balance every interval seconds.
    Attempts reconnect if disconnected.

    Args:
        connector (MT5Connector): Live MT5 connection.
        interval (int): Sleep interval in seconds.
    """
    while True:
        try:
            connected = connector.is_connected()
            balance = connector.get_account_balance()
            status = "connected" if connected else "disconnected"
            logger.info(f"HEARTBEAT | MT5: {status} | Balance: {balance}")
            if not connected:
                logger.warning("Heartbeat detected disconnection. Attempting reconnect...")
                connector.connect()
        except Exception as e:
            logger.error(f"Heartbeat error: {e}\n{traceback.format_exc()}")
        time.sleep(interval)


def session_monitor_loop(session_engine: SessionEngine, interval: int = 60):
    """
    Daemon thread: log session and killzone status every interval seconds.

    Args:
        session_engine (SessionEngine): Active session engine.
        interval (int): Sleep interval in seconds.
    """
    while True:
        try:
            session_engine.log_session_status()
        except Exception as e:
            logger.error(f"Session monitor error: {e}")
        time.sleep(interval)


def scan_pair(
    pair: str,
    session: str,
    killzone: str,
    session_engine: SessionEngine,
    scanner: ScannerMMXM,
    risk_engine: RiskEngine,
    correlation_filter: CorrelationFilter,
    state_engine: StateEngine,
    telegram_bridge: TelegramBridge,
    today: str,
):
    """
    Scan a single pair for an MMXM signal and gate it through all pre-checks.
    Exceptions are caught so one pair cannot affect others.

    Args:
        pair (str): Trading symbol.
        session (str | None): Active session name.
        killzone (str | None): Active killzone name.
        session_engine (SessionEngine): Session gate reference.
        scanner (ScannerMMXM): Signal detection engine.
        risk_engine (RiskEngine): Risk gating engine.
        correlation_filter (CorrelationFilter): Correlation gating engine.
        state_engine (StateEngine): Persistence engine.
        telegram_bridge (TelegramBridge): For sending signals to Telegram.
        today (str): ISO date string for daily guard lookups.
    """
    try:
        # Pair-level session check
        if not session_engine.is_pair_allowed(pair, session):
            return

        # Cooldown check
        if state_engine.is_pair_on_cooldown(pair):
            logger.debug(f"[{pair}] On cooldown, skipping scan.")
            return

        # Avoid window check
        if session_engine.is_avoid_window():
            logger.debug(f"[{pair}] Avoid window active, skipping scan.")
            return

        # Run scanner
        signal = scanner.scan(pair, session, killzone)
        if signal is None:
            return

        signal_id = signal["signal_id"]
        spread_pips = signal.get("spread_pips", 0.0)
        score = signal.get("score", 0.0)

        # Pre-send: risk checks
        open_trades = state_engine.get_open_trades()
        risk_result = risk_engine.run_all_checks(signal, open_trades)
        if not risk_result["pass"]:
            reason = risk_result["failed_check"]
            logger.info(f"[{pair}] Signal {signal_id} skipped — risk: {reason}")
            state_engine.insert_skip(signal_id, reason, spread_pips, score)
            return

        # Pre-send: correlation check
        direction = signal.get("direction", "")
        allowed, corr_reason = correlation_filter.can_trade(pair, open_trades, direction)
        if not allowed:
            logger.info(f"[{pair}] Signal {signal_id} skipped — correlation: {corr_reason}")
            state_engine.insert_skip(signal_id, corr_reason, spread_pips, score)
            return

        # All checks passed — send signal to Telegram for human approval
        lot_size = risk_result["lot_size"]
        state_engine.insert_signal(signal)
        sent = telegram_bridge.send_signal(signal, lot_size)
        if sent:
            logger.info(f"[{pair}] A+ signal {signal_id} sent to Telegram. Awaiting approval.")
        else:
            logger.error(f"[{pair}] Failed to send signal {signal_id} to Telegram.")

    except Exception as e:
        logger.error(f"[{pair}] Exception during pair scan: {e}\n{traceback.format_exc()}")


def main():
    """Main entry point — initialises all engines, starts threads, and runs the scan loop."""
    logger.info("=" * 60)
    logger.info("VvE FxBOT Phase 1 starting...")
    logger.info("=" * 60)

    # ── 1. Config ────────────────────────────────────────────────────
    config_engine = ConfigEngine()
    config = config_engine.get_config()
    logger.info("Config loaded successfully.")

    # ── 3. StateEngine ───────────────────────────────────────────────
    state_engine = StateEngine()
    logger.info("StateEngine initialised.")

    # ── 4. MT5Connector ──────────────────────────────────────────────
    mt5_connector = MT5Connector(config)
    if not mt5_connector.connect():
        logger.warning("Initial MT5 connection failed — heartbeat will retry.")

    # ── 5. SessionEngine ─────────────────────────────────────────────
    session_engine = SessionEngine(config)
    logger.info("SessionEngine initialised.")

    # ── 6. CorrelationFilter ─────────────────────────────────────────
    correlation_filter = CorrelationFilter(config)
    logger.info("CorrelationFilter initialised.")

    # ── 7. RiskEngine ────────────────────────────────────────────────
    risk_engine = RiskEngine(config, mt5_connector)
    logger.info("RiskEngine initialised.")

    # ── 8. GoogleSheetReporter ───────────────────────────────────────
    sheet_reporter = GoogleSheetReporter(config)
    if not sheet_reporter.connect():
        logger.warning("Google Sheet connection failed — trade logging disabled until reconnect.")

    # ── 9. TradeManager ──────────────────────────────────────────────
    trade_manager = TradeManager(config, mt5_connector, state_engine, None)  # Telegram set below
    logger.info("TradeManager initialised.")

    # ── 10. ExecutionEngine (pre-wired — Telegram set below) ─────────
    execution_engine = ExecutionEngine(
        config=config,
        mt5connector=mt5_connector,
        risk_engine=risk_engine,
        state_engine=state_engine,
        telegram_bridge=None  # Set after TelegramBridge initialisation
    )
    logger.info("ExecutionEngine initialised.")

    # ── 11. TelegramBridge ───────────────────────────────────────────
    telegram_bridge = TelegramBridge(
        config=config,
        state_engine=state_engine,
        execution_callback=execution_engine.execute_signal,
    )
    # Back-patch telegram reference into engines that need it
    execution_engine.telegram = telegram_bridge
    trade_manager.telegram = telegram_bridge

    telegram_bridge.start_listener()
    logger.info("TelegramBridge listener started.")

    # ── Start TradeManager monitoring after Telegram is wired ────────
    trade_manager.start_monitoring()
    logger.info("TradeManager monitoring thread started.")

    # ── 12. ScannerMMXM ─────────────────────────────────────────────
    scanner = ScannerMMXM(config, mt5_connector, state_engine)
    logger.info("ScannerMMXM initialised.")

    # ── 13. Heartbeat thread ─────────────────────────────────────────
    hb_thread = threading.Thread(
        target=heartbeat_loop, args=(mt5_connector,), daemon=True, name="Heartbeat"
    )
    hb_thread.start()
    logger.info("Heartbeat thread started (60s interval).")

    # ── 14. Session logger thread ────────────────────────────────────
    sess_thread = threading.Thread(
        target=session_monitor_loop, args=(session_engine,), daemon=True, name="SessionMonitor"
    )
    sess_thread.start()
    logger.info("Session monitor thread started (60s interval).")

    logger.info("All systems online. Starting main scan loop.")

    # ── 15. Main scan loop ───────────────────────────────────────────
    try:
        while True:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            # Bot disabled guard — first check every iteration
            if state_engine.is_bot_disabled_today(today):
                logger.warning("Bot is disabled for today. Sleeping 60s.")
                time.sleep(60)
                continue

            session = session_engine.get_active_session()
            killzone = session_engine.get_active_killzone()

            # Only scan during a killzone
            if not killzone:
                time.sleep(config.scan_frequency_seconds)
                continue

            pairs = session_engine.get_allowed_pairs(session)
            if not pairs:
                time.sleep(config.scan_frequency_seconds)
                continue

            # Concurrent pair scan — one exception per pair is isolated
            num_workers = max(1, len(pairs))
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                for pair in pairs:
                    executor.submit(
                        scan_pair,
                        pair, session, killzone,
                        session_engine, scanner, risk_engine,
                        correlation_filter, state_engine,
                        telegram_bridge, today,
                    )
            # ThreadPoolExecutor __exit__ waits for all futures to finish

            time.sleep(config.scan_frequency_seconds)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — bot shutting down gracefully.")
    except Exception as e:
        logger.critical(f"Critical error in main loop: {e}\n{traceback.format_exc()}")
    finally:
        mt5_connector.disconnect()
        logger.info("MT5 disconnected. VvE FxBOT stopped.")


if __name__ == "__main__":
    main()
