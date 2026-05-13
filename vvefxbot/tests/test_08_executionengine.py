"""Tests for ExecutionEngine 13-step flow."""
import pytest
from unittest.mock import MagicMock
from modules.executionengine import ExecutionEngine

def test_execution_signal_not_found(config_mock):
    state_mock = MagicMock()
    state_mock.get_signal.return_value = None
    
    engine = ExecutionEngine(config_mock, MagicMock(), MagicMock(), state_mock, MagicMock())
    result = engine.execute_signal("missing_id")
    assert result["success"] is False
    assert "Signal not found" in result["error"]

def test_execution_spread_too_high(config_mock):
    state_mock = MagicMock()
    state_mock.get_signal.return_value = {"pair": "EURUSD", "direction": "BUY", "score": 90}
    
    mt5_mock = MagicMock()
    mt5_mock.get_current_spread.return_value = 5.0
    
    engine = ExecutionEngine(config_mock, mt5_mock, MagicMock(), state_mock, MagicMock())
    result = engine.execute_signal("sig_1")
    assert result["success"] is False
    assert "spread too high" in result["error"]

def test_execution_daily_guard_blocks(config_mock):
    state_mock = MagicMock()
    state_mock.get_signal.return_value = {
        "pair": "EURUSD", "direction": "BUY", "score": 90,
        "spread_pips": 1.0, "sl_pips": 10, "tp_pips": 20
    }
    state_mock.is_bot_disabled_today.return_value = True
    
    mt5_mock = MagicMock()
    mt5_mock.get_current_spread.return_value = 1.0
    
    risk_mock = MagicMock()
    risk_mock.run_all_checks.return_value = {"pass": True, "lot_size": 0.1}
    
    engine = ExecutionEngine(config_mock, mt5_mock, risk_mock, state_mock, MagicMock())
    result = engine.execute_signal("sig_1")
    assert result["success"] is False
    assert "disabled today" in result["error"]
