"""Tests for the Backtesting System."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from backtest.connector import BacktestConnector
from backtest.engine import BacktestEngine
from core.configengine import Config

@pytest.fixture
def mock_config():
    return Config(
        strategy_mode="MMXM",
        enabled_scanners={"mmxm": True, "ote": False, "zgmt": False},
        ote_scanner={"enabled": False},
        zgmt_scanner={"enabled": False},
        trade_management={
            "partial_tp_enabled": True,
            "partial_tp_fraction": 0.5,
            "breakeven_buffer_pips": 30,
        },
        pairs=["EURUSD"],
        session_timings={"London": {"start": "00:00", "end": "23:59"}},
        killzone_timings={"London": {"start": "00:00", "end": "23:59"}},
        risk_percent=1.0,
        max_trades_day=10,
        max_trades_pair_day=5,
        max_open_trades=2,
        max_open_risk_percent=6.0,
        spread_limits={"EURUSD": 2.0},
        effective_rr_min=2.0,
        aplus_threshold=85.0,
        slippage_max_pips=2.0,
        scan_frequency_seconds=60,
        correlation_groups={},
        demo_mode=True,
        trading_pool_size=10000.0,
        mt5_login=123,
        mt5_password="pass",
        mt5_server="server",
        telegram_token="tok",
        telegram_chat_id="123",
        google_sheet_id="sheet",
        google_creds_path="creds.json"
    )

def create_dummy_ohlc(start_time, num_bars, freq, base_price: float = 1.1000):
    """Creates a dummy DataFrame for testing. base_price allows simulating different instruments."""
    times = [start_time + i * freq for i in range(num_bars)]
    bp = base_price
    spread = bp * 0.0005  # ~0.05% spread for realistic OHLC
    data = {
        "time": times,
        "open":  np.linspace(bp,          bp + bp * 0.01, num_bars),
        "high":  np.linspace(bp + spread, bp + bp * 0.01 + spread, num_bars),
        "low":   np.linspace(bp - spread, bp + bp * 0.01 - spread, num_bars),
        "close": np.linspace(bp,          bp + bp * 0.01, num_bars),
        "tick_volume": [100] * num_bars
    }
    return pd.DataFrame(data)


def test_backtest_connector_initialization(mock_config):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 10, timedelta(minutes=1))
    data = {"M1": m1_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    
    assert connector.symbol == "EURUSD"
    assert connector.get_account_balance() == 10000.0
    assert connector.is_connected() is True

def test_backtest_connector_replay_cursor(mock_config):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 100, timedelta(minutes=1))
    data = {"M1": m1_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    
    connector.set_bar_index(50)
    assert connector.current_time() == m1_df.iloc[50]["time"]
    
    candles = connector.get_candles("EURUSD", "M1", count=5)
    assert len(candles) == 5
    assert candles.iloc[-1]["time"] == m1_df.iloc[50]["time"]

def test_backtest_connector_order_simulation(mock_config):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 10, timedelta(minutes=1))
    data = {"M1": m1_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    
    connector.set_bar_index(5)
    result = connector.place_order("EURUSD", "BUY", 0.1, sl=1.09, tp=1.12)
    
    assert result["success"] is True
    ticket = result["ticket"]
    
    positions = connector.positions_get(ticket)
    assert len(positions) == 1
    assert positions[0].volume == 0.1
    assert positions[0].price_open == m1_df.iloc[5]["close"]

def test_backtest_engine_warmup(mock_config):
    """Verifies the engine respects the warmup period."""
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    # Create 150 bars, warmup is 120
    m1_df = create_dummy_ohlc(start, 150, timedelta(minutes=1))
    data = {"M1": m1_df, "M15": m1_df, "H1": m1_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    engine = BacktestEngine(mock_config, connector, "EURUSD")
    
    # Run the engine
    results = engine.run()
    
    # Should not crash and should return a list
    assert isinstance(results, list)
    # With a straight line, it likely finds 0 signals, but the loop must run from 120 to 149
    assert connector.current_bar_idx == 149

def test_backtest_engine_force_close_at_end(mock_config):
    """Verifies that open trades are closed at the end of the data."""
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 200, timedelta(minutes=1))
    data = {"M1": m1_df, "M15": m1_df, "H1": m1_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    engine = BacktestEngine(mock_config, connector, "EURUSD")
    
    # Manually inject an open trade
    from backtest.engine import SimulatedTrade
    open_trade = SimulatedTrade(
        trade_id="test_id", signal_id="sig_id", pair="EURUSD", direction="BUY",
        entry=1.10, sl=1.09, tp1=1.11, tp2=1.12, tp3=0.0, lot=0.1,
        bar_index=150, bar_time=m1_df.iloc[150]["time"]
    )
    engine._open_trades.append(open_trade)
    
    results = engine.run()
    
    # The manually injected trade should be in results as CLOSED/EXPIRED
    assert len(results) >= 1
    assert any(r["trade_id"] == "test_id" for r in results)
    closed_trade = next(r for r in results if r["trade_id"] == "test_id")
    assert closed_trade["status"] == "CLOSED"
    assert closed_trade["result"] == "EXPIRED"

def test_backtest_engine_zgmt_optimization(mock_config):
    """Verifies that ScannerZGMT backtesting is optimized to only scan inside signal windows."""
    from unittest.mock import MagicMock
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    
    mock_config.zgmt_scanner = {
        "enabled": True,
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:00"
    }
    
    # 05:30 IST is 00:00 UTC
    start_inside = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    
    # 200 bars with 1 min interval = 200 minutes = 3 hours and 20 mins (00:00 UTC to 03:20 UTC)
    bars_df = create_dummy_ohlc(start_inside, 200, timedelta(minutes=1))
    data = {"M1": bars_df, "M15": bars_df, "H1": bars_df}
    connector = BacktestConnector(mock_config, data, "EURUSD")
    
    state = StateEngine(":memory:")
    scanner = ScannerZGMT(mock_config, connector, state)
    scanner.scan = MagicMock(return_value=None)
    
    engine = BacktestEngine(mock_config, connector, "EURUSD", scanner=scanner)
    engine.run()
    
    assert scanner.scan.called
    # Total bars = 200. Warmup = 120 bars (starts at index 120 / 02:00 UTC / 07:30 IST).
    # Window ends at 08:00 IST / 02:30 UTC / index 150.
    # Bars scanned should be indexes 120 to 150 inclusive = 31 calls.
    # Remaining 49 bars (indexes 151 to 199) are outside window and must be skipped.
    assert scanner.scan.call_count == 31


def test_zgmt_daily_finalization_logic(mock_config):
    """Verifies that ScannerZGMT correctly finalizes the daily setup when invalid or cap reached."""
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    from unittest.mock import MagicMock

    mock_config.zgmt_scanner = {
        "enabled": True,
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:00",
        "zgmt_test_threshold_pips": 5,
        "max_daily_trades": 1
    }

    connector = MagicMock()
    # Mocking current_time to return a UTC datetime inside the window
    # 05:30 IST is 00:00 UTC
    connector.current_time.return_value = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)

    state = StateEngine(":memory:")
    scanner = ScannerZGMT(mock_config, connector, state)

    # Initial state should not be finalized
    assert scanner._is_daily_finalized("EURUSD") is False

    # Mark it finalized manually
    scanner._mark_daily_finalized("EURUSD")
    assert scanner._is_daily_finalized("EURUSD") is True

    # Test daily finalized checks when scan is called
    result = scanner.scan("EURUSD", "London", "Asia")
    assert result is None


def test_zgmt_structural_absence_finalization(mock_config):
    """Verifies that structural missing data like 0 GMT open or PD bias finalizes the day."""
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    from unittest.mock import MagicMock
    import pandas as pd

    mock_config.zgmt_scanner = {
        "enabled": True,
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:00",
        "d1_candles_for_range": 20,
        "require_pd_array_check": True
    }

    connector = MagicMock()
    connector.current_time.return_value = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    
    # 1. Test case: Insufficient D1 candles (returns len < n)
    connector.get_candles.return_value = pd.DataFrame()  # Empty D1/H1
    
    state = StateEngine(":memory:")
    scanner = ScannerZGMT(mock_config, connector, state)
    
    assert scanner._is_daily_finalized("EURUSD") is False
    result = scanner.scan("EURUSD", "London", "Asia")
    assert result is None
    # In backtest mode, it should be marked as finalized automatically on PD bias failure
    assert scanner._is_daily_finalized("EURUSD") is True

    # Reset finalization
    scanner._daily_finalized.clear()
    assert scanner._is_daily_finalized("EURUSD") is False

    # 2. Test case: 0 GMT open price is structurally missing (no 00:00 H1 candle)
    # Mock daily bias working, but H1 candles missing 00:00 H1 candle
    # D1 candles (length 21, midpoint range high 1.12, low 1.08, mid 1.10)
    d1_df = pd.DataFrame({
        "high": [1.12] * 22,
        "low": [1.08] * 22
    })
    connector.get_tick.return_value = {"bid": 1.09, "ask": 1.091}
    
    # Empty H1 dataframe (means 0 GMT candle not found)
    h1_df = pd.DataFrame()
    
    def mock_get_candles(symbol, timeframe, count=None):
        if timeframe == "D1":
            return d1_df
        return h1_df
        
    connector.get_candles.side_effect = mock_get_candles
    
    result = scanner.scan("EURUSD", "London", "Asia")
    assert result is None
    # 0 GMT candle structurally not found should trigger daily finalization
    assert scanner._is_daily_finalized("EURUSD") is True


def test_backtest_engine_gold_initialization(mock_config):
    """Verifies that the BacktestEngine correctly initializes Gold properties."""
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 150, timedelta(minutes=1))
    data = {"M1": m1_df, "M15": m1_df, "H1": m1_df}
    connector = BacktestConnector(mock_config, data, "XAUUSD")
    
    # 1. Standard initialization with default 10.0 pip_value parameter (should be overridden to 1.0)
    engine = BacktestEngine(mock_config, connector, "XAUUSD")
    assert engine.pip_size  == 0.01
    assert engine.pip_value == 1.0

    # 2. Gold pip_value is always forced to 1.0 (contract-fixed) regardless of argument
    engine_custom = BacktestEngine(mock_config, connector, "XAUUSD", pip_value=1.5)
    assert engine_custom.pip_size  == 0.01
    assert engine_custom.pip_value == 1.0   # always 1.0 for XAUUSD


def test_backtest_engine_jpy_initialization(mock_config):
    """Verifies that JPY pairs use pip_size=0.01 and dynamic pip_value (~1000/price)."""
    # GBPJPY at ~190 → pip_value ≈ 1000/190 ≈ 5.26
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    m1_df = create_dummy_ohlc(start, 150, timedelta(minutes=1), base_price=190.0)
    data = {"M1": m1_df, "M15": m1_df, "H1": m1_df}

    for jpy_pair in ["GBPJPY", "USDJPY", "EURJPY"]:
        connector = BacktestConnector(mock_config, data, jpy_pair)
        engine = BacktestEngine(mock_config, connector, jpy_pair)

        assert engine.pip_size == 0.01, f"{jpy_pair}: pip_size should be 0.01"
        # pip_value should be ~1000/190 ≈ 5.26, definitely NOT 10.0
        assert engine.pip_value != 10.0, f"{jpy_pair}: pip_value should not be default 10.0"
        assert 3.0 < engine.pip_value < 15.0, (
            f"{jpy_pair}: pip_value {engine.pip_value} out of realistic range (3-15) for price ~190"
        )



def test_backtest_engine_gold_trade_execution(mock_config):
    """Verifies that a Gold trade executes, processes TP1 and TP2 with correct USD profit values."""
    from unittest.mock import MagicMock
    
    mock_config.trading_pool_size = 1000.0
    mock_config.risk_percent = 1.0
    mock_config.effective_rr_min = 1.9
    mock_config.spread_limits = {"XAUUSD": 30.0}
    
    # Setup data
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    # We need at least 125 bars to cover warm-up (120) and trade bars (121, 122, 123)
    times = [start + i * timedelta(minutes=1) for i in range(130)]
    df_data = {
        "time": times,
        "open": [2000.0] * 130,
        "high": [2000.0] * 130,
        "low": [2000.0] * 130,
        "close": [2000.0] * 130,
        "tick_volume": [100] * 130
    }
    m1_df = pd.DataFrame(df_data)
    
    # Customize the OHLC values at index 121 (TP1 hit) and 122 (TP2 hit)
    # Entry at bar 120 close = 2000.00
    # TP1 is at 2000.95. Bar 121 high = 2001.00, low = 1999.50
    m1_df.loc[121, "high"] = 2001.00
    m1_df.loc[121, "low"] = 1999.50
    m1_df.loc[121, "close"] = 2000.50
    
    # TP2 is at 2001.90. Bar 122 high = 2002.00, low = 2000.50
    m1_df.loc[122, "high"] = 2002.00
    m1_df.loc[122, "low"] = 2000.50
    m1_df.loc[122, "close"] = 2001.80
    
    data = {"M1": m1_df, "M15": m1_df, "H1": m1_df}
    connector = BacktestConnector(mock_config, data, "XAUUSD")
    
    # Instantiate scanner and mock scan method to return a ZGMT gold signal at bar index 120
    scanner = MagicMock()
    
    def mock_scan(pair, session, killzone):
        # We only want to trigger the signal once on bar 120
        if connector.current_bar_idx == 120:
            return {
                "signal_id": "sig_gold_1",
                "pair": "XAUUSD",
                "session": "London",
                "direction": "BUY",
                "entry_price": 2000.0,
                "sl_price": 1999.05,
                "tp1_price": 2000.95,
                "tp2_price": 2001.90,
                "sl_pips": 95.0,
                "tp_pips": 190.0,
                "spread_pips": 0.0,
                "score": 90.0,
                "fixed_lot_size": 0.0
            }
        return None
        
    scanner.scan.side_effect = mock_scan
    
    engine = BacktestEngine(mock_config, connector, "XAUUSD", scanner=scanner)
    # Override HistoricalSessionEngine check to always return London
    engine.session_engine.get_active_session = MagicMock(return_value="London")
    engine.session_engine.get_active_killzone = MagicMock(return_value="London")
    
    results = engine.run()
    
    # We should have exactly 1 closed trade (expired trades would be 0 since it closes at TP2)
    assert len(results) == 1
    trade = results[0]
    
    assert trade["signal_id"] == "sig_gold_1"
    assert trade["status"] == "CLOSED"
    assert trade["result"] == "WIN"
    assert trade["exit_reason"] == "TP2_HIT"
    
    # Math validation:
    # Lot size calculation: Risk amount = $10.0. SL pips = 95.0. Pip value = 1.0.
    # Lot = 10.0 / (95.0 * 1.0) = 0.10526... -> rounded to 0.11.
    assert trade["lot"] == 0.11
    
    # Entry price
    assert trade["entry"] == 2000.0
    # TP2 exit price
    assert trade["exit_price"] == 2001.90
    
    # P&L Calculation:
    # TP1 hit at bar 121: 95.0 pips * 1.0 pip_value * 0.06 lots = $5.70 profit.
    # TP2 hit at bar 122: 190.0 pips * 1.0 pip_value * 0.05 lots = $9.50 profit.
    # Total profit = $5.70 + $9.50 = $15.20.
    assert trade["profit_usd"] == 15.20


def test_backtest_timezone_offset_handling():
    """Verify that _fetch_from_mt5 adjusts query dates and returned times correctly by offset_hours."""
    from unittest.mock import patch
    import MetaTrader5 as mock_mt5
    import importlib.util
    import os
    from datetime import datetime, timezone
    
    # Load backtest.py script dynamically to avoid name collision with backtest package
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backtest.py")
    spec = importlib.util.spec_from_file_location("backtest_script", script_path)
    backtest_script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(backtest_script)
    _fetch_from_mt5 = backtest_script._fetch_from_mt5
    
    # Setup mock data for copy_rates_range
    # Epoch time 1711930800 is 2024-04-01 00:20:00 UTC (or broker time 03:20:00)
    mock_rates = [
        {"time": 1711930800 + idx * 60, "open": 1.1000, "high": 1.1010, "low": 1.0990, "close": 1.1001, "tick_volume": 100}
        for idx in range(10)
    ]
    
    mock_mt5.copy_rates_range.return_value = mock_rates
    mock_mt5.symbol_select.return_value = True
    
    date_from = datetime(2024, 4, 1, 0, 0, tzinfo=timezone.utc)
    date_to = datetime(2024, 4, 1, 1, 0, tzinfo=timezone.utc)
    
    # Fetch with offset_hours = 3.0
    df = _fetch_from_mt5("EURUSD", "M1", date_from, date_to, offset_hours=3.0)
    
    # 1. Assert copy_rates_range was called with times adjusted by +3 hours
    # 2024-04-01 00:00:00 + 3h = 03:00:00
    # 2024-04-01 01:00:00 + 3h = 04:00:00
    args, kwargs = mock_mt5.copy_rates_range.call_args
    # args: (symbol, timeframe, dt_from, dt_to)
    assert args[0] == "EURUSD"
    assert args[2] == datetime(2024, 4, 1, 3, 0)
    assert args[3] == datetime(2024, 4, 1, 4, 0)
    
    # 2. Assert returned DataFrame time is shifted back by 3 hours
    # Raw time in rates is 1711930800 -> 2024-04-01 00:20:00 UTC
    # Shifted by -3 hours -> 2024-03-31 21:20:00 UTC
    expected_time = pd.to_datetime(1711930800, unit="s", utc=True) - pd.to_timedelta(3.0, unit="h")
    assert df.iloc[0]["time"] == expected_time



def test_zgmt_level_tested_exclusion(mock_config):
    """Verify that _is_zgmt_level_tested respects the exclusion window in all three critical scenarios."""
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    from unittest.mock import MagicMock
    import pandas as pd

    mock_config.zgmt_scanner = {
        "enabled": True,
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:00",
        "zgmt_test_threshold_pips": 5,
        "zgmt_test_exclude_first_mins": 15
    }

    state = StateEngine(":memory:")

    # ── Case 1: now < exclusion window end (still at 0 GMT open, 00:05 UTC) ─
    # Must return True (block) regardless of candle data — price hasn't displaced yet.
    connector1 = MagicMock()
    connector1.current_time.return_value = datetime(2026, 5, 21, 0, 5, tzinfo=timezone.utc)
    connector1.get_candles.return_value = pd.DataFrame({
        "time": [datetime(2026, 5, 21, 0, 0, tzinfo=timezone.utc)],
        "open": [1.25000], "high": [1.25010], "low": [1.24990], "close": [1.25000]
    })
    scanner1 = ScannerZGMT(mock_config, connector1, state)
    assert scanner1._is_zgmt_level_tested("EURUSD", 1.25000, mock_config.zgmt_scanner) is True, \
        "Inside exclusion window should block (return True)"

    # ── Case 2: now past exclusion window, touch only inside excluded period → untested ─
    connector2 = MagicMock()
    connector2.current_time.return_value = datetime(2026, 5, 21, 1, 0, tzinfo=timezone.utc)
    # Touch at 00:05 (inside excluded window), second candle far away
    connector2.get_candles.return_value = pd.DataFrame({
        "time": [datetime(2026, 5, 21, 0, 5, tzinfo=timezone.utc),
                 datetime(2026, 5, 21, 0, 20, tzinfo=timezone.utc)],
        "open":  [1.25000, 1.26000],
        "high":  [1.25010, 1.26010],
        "low":   [1.24990, 1.26000],
        "close": [1.25000, 1.26000]
    })
    scanner2 = ScannerZGMT(mock_config, connector2, state)
    assert scanner2._is_zgmt_level_tested("EURUSD", 1.25000, mock_config.zgmt_scanner) is False, \
        "Touch only inside excluded window should return False (untested)"

    # ── Case 3: now past exclusion window, touch after excluded period → tested ─
    connector3 = MagicMock()
    connector3.current_time.return_value = datetime(2026, 5, 21, 1, 0, tzinfo=timezone.utc)
    # First candle far away, second candle (00:20) touches the level
    connector3.get_candles.return_value = pd.DataFrame({
        "time": [datetime(2026, 5, 21, 0, 5, tzinfo=timezone.utc),
                 datetime(2026, 5, 21, 0, 20, tzinfo=timezone.utc)],
        "open":  [1.26000, 1.25000],
        "high":  [1.26010, 1.25010],
        "low":   [1.26000, 1.24990],
        "close": [1.26000, 1.25000]
    })
    scanner3 = ScannerZGMT(mock_config, connector3, state)
    assert scanner3._is_zgmt_level_tested("EURUSD", 1.25000, mock_config.zgmt_scanner) is True, \
        "Touch after excluded window should return True (already tested)"


def test_zgmt_skip_rr_check_in_risk_engine(mock_config):
    """Verify that a ZGMT signal with skip_rr_check=True bypasses the effective_rr_min gate."""
    from modules.riskengine import RiskEngine
    from unittest.mock import MagicMock

    mock_config.effective_rr_min = 1.9  # Global threshold that would normally block ZGMT

    connector = MagicMock()
    engine = RiskEngine(mock_config, connector)

    # GBPUSD ZGMT signal: SL=25, TP=50, spread=2 → effective RR=1.78 (below 1.9)
    zgmt_signal = {
        "pair": "GBPUSD",
        "spread_pips": 2.0,
        "sl_pips": 25.0,
        "tp_pips": 50.0,
        "skip_rr_check": True,
    }

    result = engine.run_all_checks(zgmt_signal, open_trades=[])
    assert result["pass"] is True, "ZGMT signal with skip_rr_check=True must pass risk checks"
    # Effective RR is still computed and reported
    assert abs(result["effective_rr"] - 1.78) < 0.01

    # Without skip_rr_check, the same signal should be blocked
    regular_signal = dict(zgmt_signal)
    regular_signal["skip_rr_check"] = False
    result_blocked = engine.run_all_checks(regular_signal, open_trades=[])
    assert result_blocked["pass"] is False
    assert result_blocked["failed_check"] == "check_effective_rr"


def test_zgmt_gold_spread_vs_sl_bypass(mock_config):
    """Verify Gold ZGMT trades are not blocked by the spread_vs_sl check (30/95 = 31.6% > 10%)."""
    from modules.riskengine import RiskEngine
    from unittest.mock import MagicMock

    mock_config.effective_rr_min = 1.9
    mock_config.spread_limits = {"XAUUSD": 30.0}

    connector = MagicMock()
    engine = RiskEngine(mock_config, connector)

    # XAUUSD ZGMT signal: spread=30 pips, SL=95 → 31.6% ratio fails 10% check without bypass
    gold_signal = {
        "pair": "XAUUSD",
        "spread_pips": 30.0,
        "sl_pips": 95.0,
        "tp_pips": 190.0,
        "skip_rr_check": True,
    }
    result = engine.run_all_checks(gold_signal, open_trades=[])
    assert result["pass"] is True, "Gold ZGMT must not be blocked by spread_vs_sl check"

    # Without skip_rr_check, the 31.6% spread ratio would block on check_spread_vs_sl
    # (since check_effective_rr passes: (190-30)/(95+30) = 160/125 = 1.28, below 1.9 → blocked first)
    gold_signal_no_skip = dict(gold_signal)
    gold_signal_no_skip["skip_rr_check"] = False
    result_blocked = engine.run_all_checks(gold_signal_no_skip, open_trades=[])
    assert result_blocked["pass"] is False


def test_zgmt_direct_entry_at_zgmt_price(mock_config):
    """Verify DIRECT entry mode uses zgmt_price (strategy Step 4/10), not the drifted tick."""
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    from unittest.mock import MagicMock, patch
    import pandas as pd

    mock_config.zgmt_scanner = {
        "enabled": True,
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:30",
        "zgmt_window_end_ist": "08:30",
        "d1_candles_for_range": 5,
        "require_pd_array_check": True,
        "require_power_of_three": False,
        "zgmt_entry_mode": "DIRECT",
        "zgmt_filter_pips": 20,
        "zgmt_test_threshold_pips": 5,
        "zgmt_test_exclude_first_mins": 15,
        "zgmt_sl_tp": {"sl_pips_gold": 95, "tp_pips_gold": 190, "sl_pips_fx": 25, "tp_pips_fx": 50},
        "score": 70.0,
        "cooldown_minutes": 0,
        "max_daily_trades": 5,
        "allow_buy": True,
        "allow_sell": True,
        "fixed_lot_size": 0.0,
        "skip_rr_check": True,
    }

    # Simulate scanning at 01:00 UTC (past exclusion window, inside IST window 05:30–08:30)
    mock_time = datetime(2026, 5, 21, 1, 0, tzinfo=timezone.utc)
    zgmt_price_open = 1.35000  # The 0 GMT open price

    connector = MagicMock()
    connector.current_time.return_value = mock_time
    connector.get_current_spread.return_value = 2.0
    connector.get_tick.return_value = {"bid": 1.35200, "ask": 1.35202}  # Drifted 20 pips away

    # D1 candles: price in discount zone (BID 1.35200 < mid 1.36000)
    d1_df = pd.DataFrame({
        "time": [datetime(2026, 5, 20, tzinfo=timezone.utc)] * 6,
        "high": [1.37000] * 6, "low": [1.35000] * 6,
        "open": [1.36000] * 6, "close": [1.36000] * 6
    })
    # H1 candles: one at 00:00 UTC (0 GMT candle) with open = zgmt_price
    h1_df = pd.DataFrame({
        "time": [datetime(2026, 5, 21, 0, 0, tzinfo=timezone.utc)],
        "open": [zgmt_price_open], "high": [1.35100],
        "low": [1.34900], "close": [1.35050]
    })
    # M1 candles: all far from zgmt_price (level untested after exclusion window)
    m1_df = pd.DataFrame({
        "time": [datetime(2026, 5, 21, 0, 20, tzinfo=timezone.utc)],
        "open": [1.35300], "high": [1.35400],
        "low": [1.35250], "close": [1.35350]
    })

    def mock_get_candles(symbol, timeframe, count=None):
        if timeframe == "D1": return d1_df
        if timeframe == "H1": return h1_df
        return m1_df

    connector.get_candles.side_effect = mock_get_candles

    state = StateEngine(":memory:")
    scanner = ScannerZGMT(mock_config, connector, state)
    signal = scanner.scan("GBPUSD", "Asia", "Asia")

    assert signal is not None, "Should produce a signal"
    assert signal["direction"] == "BUY", "Bias should be BULLISH (discount zone)"
    # DIRECT mode must use zgmt_price, NOT the drifted tick ask (1.35202)
    assert signal["entry_price"] == round(zgmt_price_open, 5), \
        f"DIRECT entry must be at zgmt_price ({zgmt_price_open}), got {signal['entry_price']}"


# ─────────────────────────────────────────────────────────────────────────────
# ZGMT Partial-TP Backtest Logic Tests
# Each test directly invokes BacktestEngine._check_exits() via a minimal helper
# so we never need a scanner, config file, or MT5 connection.
# ─────────────────────────────────────────────────────────────────────────────

def _zgmt_engine(mock_config):
    """Return a minimal BacktestEngine wired for XAUUSD (pip_size=0.01, pip_value=$1)."""
    from backtest.engine import BacktestEngine, SimulatedTrade
    from backtest.connector import BacktestConnector
    from unittest.mock import MagicMock
    import numpy as np

    # Minimal 200-bar M1 dataset so the engine initialises without complaining
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    times = [start + timedelta(minutes=i) for i in range(200)]
    df = pd.DataFrame({
        "time": times,
        "open":  [3200.0] * 200,
        "high":  [3205.0] * 200,
        "low":   [3195.0] * 200,
        "close": [3200.0] * 200,
        "tick_volume": [100] * 200,
    })

    connector = BacktestConnector(mock_config, {"M1": df}, "XAUUSD")
    engine = BacktestEngine(mock_config, connector, "XAUUSD", scanner=MagicMock(), pip_value=1.0)
    return engine, SimulatedTrade


def _make_zgmt_trade(SimulatedTrade, direction="BUY", lot=0.02):
    """Build a fresh ZGMT SimulatedTrade for XAUUSD."""
    pip_size = 0.01
    entry = 3200.00
    sl    = 3150.00   # 50 pips below  (BUY)
    tp1   = 3250.00   # +50 pips  = 1R
    tp2   = 3300.00   # +100 pips = 2R
    if direction == "SELL":
        sl, tp1, tp2 = 3250.00, 3150.00, 3100.00

    return SimulatedTrade(
        trade_id="T1", signal_id="S1", pair="XAUUSD",
        direction=direction, entry=entry,
        sl=sl, tp1=tp1, tp2=tp2, tp3=0.0,
        lot=lot, bar_index=0,
        bar_time=datetime(2026, 5, 1, tzinfo=timezone.utc),
        pip_size=pip_size,
        use_partial_tp=True,
        partial_tp_fraction=0.5,
        be_buffer_pips=30,
    )


def _bar(high, low):
    """Make a fake M1 bar Series."""
    return pd.Series({"high": float(high), "low": float(low)})


def _fake_time():
    return datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc)


# ── Test 1: Full WIN — TP1 bar then TP2 bar ─────────────────────────────────
def test_zgmt_bt_full_win(mock_config):
    """TP1 hit → partial closed → TP2 hit → WIN with combined P&L."""
    engine, ST = _zgmt_engine(mock_config)
    trade = _make_zgmt_trade(ST, "BUY", lot=0.02)

    # Bar 1: price hits TP1 but NOT TP2
    closed = engine._check_exits(trade, _bar(high=3255, low=3198), 1, _fake_time())
    assert not closed,             "Should still be open after TP1"
    assert trade.tp1_hit,          "tp1_hit must be set"
    assert trade.be_moved,         "be_moved must be set"
    assert trade.remaining_lot == pytest.approx(0.01)  # 50% closed
    assert trade.partial_profit > 0                     # locked profit at TP1
    assert trade.current_sl == pytest.approx(3200.30)  # entry + 30*0.01

    # Bar 2: price hits TP2
    closed = engine._check_exits(trade, _bar(high=3310, low=3295), 2, _fake_time())
    assert closed,                 "Should be closed at TP2"
    assert trade.result == "WIN"
    assert trade.exit_reason == "TP2_HIT"
    assert trade.profit_usd > 0


# ── Test 2: LOSS — SL hit before TP1 ────────────────────────────────────────
def test_zgmt_bt_loss_before_tp1(mock_config):
    """SL hit before TP1 → full lot LOSS."""
    engine, ST = _zgmt_engine(mock_config)
    trade = _make_zgmt_trade(ST, "BUY", lot=0.02)

    closed = engine._check_exits(trade, _bar(high=3210, low=3140), 1, _fake_time())
    assert closed,                 "Should be closed at SL"
    assert trade.result == "LOSS"
    assert trade.exit_reason == "SL_HIT"
    assert trade.profit_usd < 0   # net loss
    assert not trade.tp1_hit


# ── Test 3: BREAKEVEN — SL hit at BE+buffer after TP1 ───────────────────────
def test_zgmt_bt_breakeven_after_tp1(mock_config):
    """TP1 hit → SL moved to BE+30pips → SL hit → BREAKEVEN with positive total."""
    engine, ST = _zgmt_engine(mock_config)
    trade = _make_zgmt_trade(ST, "BUY", lot=0.02)

    # Bar 1: hit TP1
    engine._check_exits(trade, _bar(high=3255, low=3198), 1, _fake_time())
    assert trade.tp1_hit
    # New SL must be above entry (be_buffer makes it a small profit even at SL)
    assert trade.current_sl == pytest.approx(3200.30)

    # Bar 2: price dips to exactly BE+buffer SL
    closed = engine._check_exits(trade, _bar(high=3205, low=3199), 2, _fake_time())
    assert closed,                     "Should close at BE SL"
    assert trade.result == "BREAKEVEN"
    assert trade.profit_usd > 0        # TP1 profit > tiny SL loss at +30pip


# ── Test 4: Same-bar TP1 + TP2 (big candle) ─────────────────────────────────
def test_zgmt_bt_same_bar_tp1_tp2(mock_config):
    """A single large candle hits both TP1 and TP2 — must close as WIN immediately."""
    engine, ST = _zgmt_engine(mock_config)
    trade = _make_zgmt_trade(ST, "BUY", lot=0.02)

    # One bar whose high blows past both TP1 (3250) and TP2 (3300)
    closed = engine._check_exits(trade, _bar(high=3310, low=3198), 1, _fake_time())
    assert closed,                 "Must close when same bar hits both TP1+TP2"
    assert trade.result == "WIN"
    assert trade.exit_reason == "TP2_HIT"
    assert trade.tp1_hit
    assert trade.profit_usd > 0
    # TP1 partial + TP2 remainder should both be positive
    assert trade.partial_profit > 0


# ── Test 5: SELL trade — TP1 hit → TP2 hit ─────────────────────────────────
def test_zgmt_bt_sell_full_win(mock_config):
    """SELL: TP1 = lower price, TP2 = even lower. Verify direction is handled correctly."""
    engine, ST = _zgmt_engine(mock_config)
    trade = _make_zgmt_trade(ST, "SELL", lot=0.02)
    # SELL: entry=3200, sl=3250, tp1=3150, tp2=3100

    # Bar 1: low drops to TP1
    closed = engine._check_exits(trade, _bar(high=3202, low=3145), 1, _fake_time())
    assert not closed
    assert trade.tp1_hit
    # SL should have moved DOWN from 3250 to entry - 30*pip = 3200 - 0.30 = 3199.70
    assert trade.current_sl == pytest.approx(3199.70)

    # Bar 2: low drops to TP2
    closed = engine._check_exits(trade, _bar(high=3199, low=3090), 2, _fake_time())
    assert closed
    assert trade.result == "WIN"
    assert trade.profit_usd > 0





# ═══════════════════════════════════════════════════════════════════════════════
# ZGMT Strategy Spec Alignment Tests (ICT 0-GMT Master Strategy)
# Tests for: _is_metal(), pip_size(XAGUSD), filter pips, ADR SL/TP, SPLIT mode
# ═══════════════════════════════════════════════════════════════════════════════

def _make_scanner(mock_config, connector_mock):
    """Helper: build a ScannerZGMT with a minimal mock connector."""
    from modules.scannerzgmt import ScannerZGMT
    from core.stateengine import StateEngine
    state = StateEngine(":memory:")
    return ScannerZGMT(mock_config, connector_mock, state)


# ── Test 1: _is_metal() covers XAU and XAG, not FX ─────────────────────────
def test_is_metal_pairs(mock_config):
    """_is_metal() must return True for XAUUSD and XAGUSD; False for FX/JPY pairs."""
    from unittest.mock import MagicMock
    scanner = _make_scanner(mock_config, MagicMock())

    assert scanner._is_metal("XAUUSD") is True,  "XAUUSD should be metal"
    assert scanner._is_metal("XAGUSD") is True,  "XAGUSD should be metal"
    assert scanner._is_metal("xauusd") is True,  "Case-insensitive"
    assert scanner._is_metal("xagusd") is True,  "Case-insensitive"
    assert scanner._is_metal("EURUSD") is False, "EURUSD is not a metal"
    assert scanner._is_metal("GBPJPY") is False, "GBPJPY is not a metal"
    assert scanner._is_metal("USDJPY") is False, "USDJPY is not a metal"
    assert scanner._is_metal("XAGUAH") is True,  "Any XAG pair is metal"


# ── Test 2: _pip_size() returns 0.01 for XAGUSD ─────────────────────────────
def test_pip_size_silver(mock_config):
    """_pip_size('XAGUSD') must return 0.01 (same as XAUUSD, not 0.0001)."""
    from unittest.mock import MagicMock
    scanner = _make_scanner(mock_config, MagicMock())

    assert scanner._pip_size("XAGUSD") == 0.01, "Silver pip size must be 0.01"
    assert scanner._pip_size("XAUUSD") == 0.01, "Gold pip size must be 0.01"
    assert scanner._pip_size("EURUSD") == 0.0001
    assert scanner._pip_size("GBPJPY") == 0.01  # JPY


# ── Test 3: Filter pips are metal-vs-FX differentiated ──────────────────────
def test_filter_pips_metal_vs_fx(mock_config):
    """
    _compute_entry_sl_tp() must use zgmt_filter_pips_metal (~95) for metals
    and zgmt_filter_pips_fx (~25) for FX, not the legacy single zgmt_filter_pips.
    """
    from unittest.mock import MagicMock
    import pandas as pd

    # D1 candles for ADR: 7 bars, ADR(5) will be computed from completed ones
    def make_d1(price_range):
        return pd.DataFrame({
            "high":  [100 + price_range] * 8,
            "low":   [100.0] * 8,
            "open":  [100.0] * 8,
            "close": [100 + price_range] * 8,
        })

    connector = MagicMock()
    connector.get_current_spread.return_value = 0.0

    scanner = _make_scanner(mock_config, connector)
    zgmt_price = 1.10000

    # Shared config with both keys
    zgmt_cfg = {
        "zgmt_entry_mode": "FILTER",
        "zgmt_filter_pips_fx": 25,
        "zgmt_filter_pips_metal": 95,
        "zgmt_sl_tp": {"sl_pips_fx": 25, "tp_pips_fx": 50,
                       "sl_pips_gold": 95, "tp_pips_gold": 190},
        "zgmt_adr_days": 5,
    }

    # FX pair: D1 candles giving ADR ≈ 60 pips (each daily range = 0.006)
    connector.get_candles.return_value = make_d1(0.006)
    result_fx = scanner._compute_entry_sl_tp("EURUSD", "BULLISH", zgmt_price, {}, zgmt_cfg)
    assert result_fx is not None
    fx_filter = result_fx["filter_pips"]
    assert fx_filter == 25, f"FX filter pips should be 25, got {fx_filter}"
    # FILTER BUY: entry_price = zgmt_price - 25 pips = 1.10000 - 0.0025 = 1.09750
    assert abs(result_fx["entry_price"] - (zgmt_price - 25 * 0.0001)) < 1e-5

    # Metal pair: D1 candles giving ADR ≈ 200 pips (each daily range = 2.0)
    connector.get_candles.return_value = make_d1(2.0)
    result_gold = scanner._compute_entry_sl_tp("XAUUSD", "BULLISH", 2000.0, {}, zgmt_cfg)
    assert result_gold is not None
    metal_filter = result_gold["filter_pips"]
    assert metal_filter == 95, f"Metal filter pips should be 95, got {metal_filter}"
    # FILTER BUY: entry = 2000 - 95 pips = 2000 - 0.95 = 1999.05
    assert abs(result_gold["entry_price"] - (2000.0 - 95 * 0.01)) < 1e-4

    # Silver must behave identically to Gold for filter pips
    result_silver = scanner._compute_entry_sl_tp("XAGUSD", "BULLISH", 30.0, {}, zgmt_cfg)
    assert result_silver is not None
    assert result_silver["filter_pips"] == 95, "XAGUSD must use metal filter pips"


# ── Test 4: ADR-based SL/TP for FX pair ─────────────────────────────────────
def test_adr_sl_tp_dynamic_fx(mock_config):
    """
    With D1 candles giving ADR(5) = 60 pips for EURUSD,
    sl_pips must be ≈ 30 (ADR/2/pip_size) and tp_pips ≈ 60, NOT the fixed 25/50.
    """
    from unittest.mock import MagicMock
    import pandas as pd

    connector = MagicMock()
    # 7 D1 bars: completed range = 0.006 each → ADR(5) = 0.006 → sl_dist = 0.003 → 30 pips
    connector.get_candles.return_value = pd.DataFrame({
        "high":  [1.1060] * 7,
        "low":   [1.1000] * 7,
        "open":  [1.1000] * 7,
        "close": [1.1060] * 7,
    })
    connector.get_current_spread.return_value = 0.0

    scanner = _make_scanner(mock_config, connector)
    zgmt_cfg = {
        "zgmt_entry_mode": "DIRECT",
        "zgmt_filter_pips_fx": 25,
        "zgmt_filter_pips_metal": 95,
        "zgmt_adr_days": 5,
        "zgmt_sl_tp": {"sl_pips_fx": 25, "tp_pips_fx": 50},
    }
    result = scanner._compute_entry_sl_tp("EURUSD", "BULLISH", 1.1000, {}, zgmt_cfg)
    assert result is not None

    # ADR(5) = 0.006 → sl_dist = 0.003 → sl_pips = 0.003 / 0.0001 = 30
    expected_sl_pips = 30.0
    expected_tp_pips = 60.0
    assert abs(result["sl_pips"] - expected_sl_pips) < 1.0, \
        f"FX sl_pips should be ≈{expected_sl_pips}, got {result['sl_pips']}"
    assert abs(result["tp_pips"] - expected_tp_pips) < 1.0, \
        f"FX tp_pips should be ≈{expected_tp_pips}, got {result['tp_pips']}"

    # Ensure TP = 2 × SL in price terms
    sl_dist = result["entry_price"] - result["sl_price"]
    tp2_dist = result["tp2_price"] - result["entry_price"]
    assert abs(tp2_dist - sl_dist * 2) < 1e-5, "TP2 must be exactly 2× SL distance"


# ── Test 5: ADR-based SL/TP for XAUUSD ──────────────────────────────────────
def test_adr_sl_tp_dynamic_gold(mock_config):
    """
    With D1 candles giving ADR(5) = 200 pips for XAUUSD,
    sl_pips must be ≈ 100 (ADR/2/0.01) and tp_pips ≈ 200, NOT the fixed 95/190.
    """
    from unittest.mock import MagicMock
    import pandas as pd

    connector = MagicMock()
    # 7 D1 bars: daily range = 2.0 → ADR(5) = 2.0 → sl_dist = 1.0 → 100 pips (0.01 pip)
    connector.get_candles.return_value = pd.DataFrame({
        "high":  [2002.0] * 7,
        "low":   [2000.0] * 7,
        "open":  [2000.0] * 7,
        "close": [2002.0] * 7,
    })
    connector.get_current_spread.return_value = 0.0

    scanner = _make_scanner(mock_config, connector)
    zgmt_cfg = {
        "zgmt_entry_mode": "DIRECT",
        "zgmt_filter_pips_fx": 25,
        "zgmt_filter_pips_metal": 95,
        "zgmt_adr_days": 5,
        "zgmt_sl_tp": {"sl_pips_gold": 95, "tp_pips_gold": 190},
    }
    result = scanner._compute_entry_sl_tp("XAUUSD", "BULLISH", 2000.0, {}, zgmt_cfg)
    assert result is not None

    # ADR(5) = 2.0 → sl_dist = 1.0 → sl_pips = 1.0 / 0.01 = 100
    expected_sl_pips = 100.0
    expected_tp_pips = 200.0
    assert abs(result["sl_pips"] - expected_sl_pips) < 2.0, \
        f"Gold sl_pips should be ≈{expected_sl_pips}, got {result['sl_pips']}"
    assert abs(result["tp_pips"] - expected_tp_pips) < 2.0, \
        f"Gold tp_pips should be ≈{expected_tp_pips}, got {result['tp_pips']}"

    # 1:2 RR check
    sl_dist = result["entry_price"] - result["sl_price"]
    tp2_dist = result["tp2_price"] - result["entry_price"]
    assert abs(tp2_dist - sl_dist * 2) < 1e-3, "TP2 must be exactly 2× SL distance"

    # Also verify XAGUSD behaves the same way
    result_ag = scanner._compute_entry_sl_tp("XAGUSD", "BULLISH", 30.0, {}, zgmt_cfg)
    assert result_ag is not None
    assert abs(result_ag["sl_pips"] - 100.0) < 2.0, "XAGUSD sl_pips must match XAUUSD"


# ── Test 6: ADR fallback to fixed pips when D1 candles are insufficient ──────
def test_adr_sl_fallback_on_missing_data(mock_config):
    """
    When D1 candles are insufficient for ADR computation, the scanner must:
    1. Log a WARNING (not crash).
    2. Fall back to the fixed sl_pips_fx / sl_pips_gold from config.
    """
    import logging
    from unittest.mock import MagicMock
    import pandas as pd

    connector = MagicMock()
    # Only 2 D1 bars: insufficient for ADR(5)
    connector.get_candles.return_value = pd.DataFrame({
        "high":  [1.1060, 1.1050],
        "low":   [1.1000, 1.0990],
        "open":  [1.1000, 1.0990],
        "close": [1.1060, 1.1050],
    })
    connector.get_current_spread.return_value = 0.0

    scanner = _make_scanner(mock_config, connector)
    zgmt_cfg = {
        "zgmt_entry_mode": "DIRECT",
        "zgmt_filter_pips_fx": 25,
        "zgmt_filter_pips_metal": 95,
        "zgmt_adr_days": 5,
        "zgmt_sl_tp": {"sl_pips_fx": 25, "tp_pips_fx": 50,
                       "sl_pips_gold": 95, "tp_pips_gold": 190},
    }

    # Ensure no exception is raised (ADR failure must be handled gracefully)
    result = scanner._compute_entry_sl_tp("EURUSD", "BULLISH", 1.1000, {}, zgmt_cfg)

    assert result is not None, "Should return a result even on ADR failure"
    # Must fall back to fixed 25 pips
    assert result["sl_pips"] == 25.0, \
        f"Fallback sl_pips should be 25 (fixed), got {result['sl_pips']}"
    assert result["tp_pips"] == 50.0, \
        f"Fallback tp_pips should be 50 (fixed), got {result['tp_pips']}"


# ── Test 7: SPLIT mode emits two signals ───────────
def test_split_mode_two_signals(mock_config):
    """
    SPLIT mode must:
    1. Emit two signals (Leg A: Direct, Leg B: Filter)
    2. Both must have position_fraction=0.5
    """
    from unittest.mock import MagicMock
    import pandas as pd
    from datetime import datetime, timezone

    connector = MagicMock()
    # 7 D1 bars with range 0.006 → ADR OK
    connector.get_candles.return_value = pd.DataFrame({
        "high":  [1.1060] * 7,
        "low":   [1.1000] * 7,
        "open":  [1.1000] * 7,
        "close": [1.1060] * 7,
    })
    connector.get_current_spread.return_value = 0.0
    # Simulate valid tick
    connector.get_tick.return_value = {"bid": 1.10050, "ask": 1.10050}
    # Simulate time inside window
    connector.current_time.return_value = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc) # 6:30 IST

    mock_config.zgmt_scanner = {
        "zgmt_entry_mode": "SPLIT",
        "zgmt_filter_pips_fx": 25,
        "zgmt_filter_pips_metal": 95,
        "zgmt_adr_days": 5,
        "zgmt_sl_tp": {"sl_pips_fx": 25, "tp_pips_fx": 50},
        "zgmt_window_start_ist": "05:30",
        "zgmt_window_end_ist": "08:00",
        "allow_buy": True,
        "allow_sell": True,
        "require_pd_array_check": False,
        "require_power_of_three": False,
    }

    scanner = _make_scanner(mock_config, connector)
    zgmt_price = 1.10000
    # Override the _get_zgmt_price mock
    scanner._get_zgmt_price = MagicMock(return_value=(zgmt_price, False))
    # Override _is_zgmt_level_tested
    scanner._is_zgmt_level_tested = MagicMock(return_value=False)

    # Call scan
    result = scanner.scan("EURUSD", "LONDON", "OPEN")
    
    # Should be a list of two signals
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 2

    leg_a, leg_b = result
    
    assert leg_a["position_fraction"] == 0.5
    assert leg_b["position_fraction"] == 0.5

    # Entry A must be direct
    assert abs(leg_a["entry_price"] - zgmt_price) < 1e-5

    # Entry B must be at filter zone (below 0 GMT for BUY since tick bid > 0)
    expected_entry_b = zgmt_price - 25 * 0.0001
    assert abs(leg_b["entry_price"] - expected_entry_b) < 1e-5
