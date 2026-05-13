"""Tests for CorrelationFilter group isolation logic."""
import pytest
from modules.correlationfilter import CorrelationFilter

def test_get_group(config_mock):
    engine = CorrelationFilter(config_mock)
    assert engine.get_group("EURUSD") == "A"
    assert engine.get_group("USDJPY") == "B"
    assert engine.get_group("UNKNOWN") is None

def test_can_trade_jpy_max(config_mock):
    engine = CorrelationFilter(config_mock)
    open_trades = [{"pair": "USDJPY", "direction": "BUY"}]
    # Try to open another JPY trade
    allowed, reason = engine.can_trade("EURJPY", open_trades, "BUY")
    assert allowed is False
    assert reason == "JPY_MAX_REACHED"

def test_can_trade_eurusd_gbpusd_rule3(config_mock):
    engine = CorrelationFilter(config_mock)
    open_trades = [{"pair": "EURUSD", "direction": "BUY"}]
    
    # Same direction -> Blocked
    allowed1, reason1 = engine.can_trade("GBPUSD", open_trades, "BUY")
    assert allowed1 is False
    assert reason1 == "CORR_SAME_DIRECTION"
    
    # Different direction -> Allowed
    allowed2, reason2 = engine.can_trade("GBPUSD", open_trades, "SELL")
    assert allowed2 is True
    assert reason2 == "OK"
