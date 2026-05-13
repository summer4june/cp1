"""Tests for RiskEngine lot sizing and guards."""
import pytest
from modules.riskengine import RiskEngine

def test_calculate_lot_size(config_mock):
    from unittest.mock import MagicMock
    mt5_mock = MagicMock()
    # Force current price for JPY calculation
    mock_sym = MagicMock()
    mock_sym.bid = 150.0
    mt5_mock.symbol_info_tick.return_value = mock_sym
    
    engine = RiskEngine(config_mock, mt5_mock)
    
    # EURUSD formula: RiskAmount = 1000 * 0.01 = 10.
    # sl_pips = 10. PipValue = $10. 
    # Lot = 10 / (10 * 10) = 0.1
    lot = engine.calculate_lot_size(10.0, "EURUSD")
    assert lot == 0.1
    
    # JPY formula: PipValue = 10 / 150 * 100 = 6.66...
    # Lot = 10 / (10 * 6.66) = 0.15
    lot_jpy = engine.calculate_lot_size(10.0, "USDJPY")
    assert lot_jpy == 0.15

def test_check_spread(config_mock):
    engine = RiskEngine(config_mock, None)
    assert engine.check_spread("EURUSD", 1.5) is True
    assert engine.check_spread("EURUSD", 2.5) is False

def test_check_effective_rr(config_mock):
    engine = RiskEngine(config_mock, None)
    # rr = (20 - 1) / (10 + 1) = 19 / 11 = 1.72 (fails min 2.0)
    passed, rr = engine.check_effective_rr(tp_pips=20, sl_pips=10, spread_pips=1)
    assert passed is False
    
    # rr = (30 - 1) / (10 + 1) = 29 / 11 = 2.63 (passes)
    passed, rr2 = engine.check_effective_rr(tp_pips=30, sl_pips=10, spread_pips=1)
    assert passed is True
