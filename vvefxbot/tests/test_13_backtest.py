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
