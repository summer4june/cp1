"""Integration tests covering multiple components and Google Sheets reporter."""
import pytest
from unittest.mock import patch, MagicMock
from modules.reportgoogle import GoogleSheetReporter
from core.stateengine import StateEngine
from modules.executionengine import ExecutionEngine

def test_report_google_connect(config_mock):
    with patch("modules.reportgoogle.ServiceAccountCredentials") as creds_mock,          patch("modules.reportgoogle.gspread") as gspread_mock,          patch("os.path.exists", return_value=True):
        
        reporter = GoogleSheetReporter(config_mock)
        assert reporter.connect() is True

def test_report_google_missing_creds(config_mock):
    reporter = GoogleSheetReporter(config_mock)
    # File doesn't exist by default
    assert reporter.connect() is False

def test_integration_full_trade_mocked(config_mock, temp_db):
    state_engine = StateEngine(db_path=temp_db)
    mt5_mock = MagicMock()
    mt5_mock.get_current_spread.return_value = 1.0
    mt5_mock.place_order.return_value = {"success": True, "ticket": 12345}
    
    risk_engine = MagicMock()
    risk_engine.run_all_checks.return_value = {"pass": True, "lot_size": 0.1}
    risk_engine.check_slippage.return_value = True
    
    engine = ExecutionEngine(config_mock, mt5_mock, risk_engine, state_engine, MagicMock())
    
    signal = {
        "signal_id": "sig_123", "pair": "EURUSD", "session": "London",
        "direction": "BUY", "entry_price": 1.0, "sl_price": 0.9,
        "tp1_price": 1.1, "tp2_price": 1.2, "sl_pips": 10, "tp_pips": 20,
        "spread_pips": 1, "effective_rr": 2.0, "score": 90, "detected_time": "2026-01-01"
    }
    state_engine.insert_signal(signal)
    
    result = engine.execute_signal("sig_123")
    assert result["success"] is True
    assert result["ticket"] == 12345
    
    open_trades = state_engine.get_open_trades()
    assert len(open_trades) == 1
    assert open_trades[0]["ticket_id"] == 12345
