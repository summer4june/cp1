import pytest
import os
import sys
from unittest.mock import MagicMock

# Mock MetaTrader5 module for non-Windows environments
mock_mt5_module = MagicMock()
sys.modules["MetaTrader5"] = mock_mt5_module

# Configure common return values to avoid MagicMock leakage into DB
mock_pos = MagicMock()
mock_pos.price_open = 1.0
mock_mt5_module.positions_get.return_value = [mock_pos]

from core.configengine import Config

@pytest.fixture
def temp_db():
    db_path = "test_fxbot.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)

@pytest.fixture
def mock_mt5():
    return MagicMock()

@pytest.fixture
def mock_telegram():
    return MagicMock()

@pytest.fixture
def mock_google():
    return MagicMock()

@pytest.fixture
def config_mock():
    config = MagicMock(spec=Config)
    config.pairs = ["EURUSD", "GBPUSD"]
    config.session_timings = {
        "Asia": {"start": "05:30", "end": "12:30"},
        "London": {"start": "12:30", "end": "18:30"},
        "NewYork": {"start": "18:30", "end": "02:30"}
    }
    config.killzone_timings = {
        "Asia": {"start": "06:30", "end": "09:30"},
        "London": {"start": "13:30", "end": "16:30"},
        "NewYork": {"start": "18:30", "end": "21:30"},
        "LondonClose": {"start": "21:30", "end": "23:30"}
    }
    config.correlation_groups = {
        "A": ["EURUSD", "GBPUSD"],
        "B": ["USDJPY", "EURJPY", "GBPJPY"],
        "C": ["USDCAD"]
    }
    config.spread_limits = {"EURUSD": 2.0}
    config.risk_percent = 1.0
    config.trading_pool_size = 1000.0
    config.effective_rr_min = 2.0
    config.max_open_trades = 5
    config.max_trades_day = 10
    config.max_trades_pair_day = 3
    config.google_creds_path = "dummy.json"
    config.google_sheet_id = "dummy_id"
    config.mt5_login = "123"
    config.mt5_password = "pass"
    config.mt5_server = "server"
    config.telegram_token = "tok"
    config.telegram_chat_id = "chat"
    return config
