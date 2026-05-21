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

def create_dummy_ohlc(start_time, num_bars, freq):
    """Creates a dummy DataFrame for testing."""
    times = [start_time + i * freq for i in range(num_bars)]
    data = {
        "time": times,
        "open": np.linspace(1.1000, 1.1100, num_bars),
        "high": np.linspace(1.1005, 1.1105, num_bars),
        "low": np.linspace(1.0995, 1.1095, num_bars),
        "close": np.linspace(1.1001, 1.1101, num_bars),
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
        entry=1.10, sl=1.09, tp1=1.11, tp2=1.12, lot=0.1,
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
    
    # 1. Standard initialization with default 10.0 pip_value parameter (which should be overridden to 1.0)
    engine = BacktestEngine(mock_config, connector, "XAUUSD")
    assert engine.pip_size == 0.01
    assert engine.pip_value == 1.0
    
    # 2. Custom pip_value should not be overridden if it was passed specifically (e.g. non-default)
    engine_custom = BacktestEngine(mock_config, connector, "XAUUSD", pip_value=1.5)
    assert engine_custom.pip_size == 0.01
    assert engine_custom.pip_value == 1.5


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





