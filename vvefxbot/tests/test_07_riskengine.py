"""Tests for RiskEngine lot sizing and guards."""
import pytest
from unittest.mock import MagicMock
from modules.riskengine import RiskEngine

def test_calculate_lot_size(config_mock):
    mt5_mock = MagicMock()
    mock_sym = MagicMock()
    mock_sym.bid = 150.0
    import pandas as pd
    df = pd.DataFrame([{'close': 150.0}])
    mt5_mock.get_candles.return_value = df
    mt5_mock.symbol_info_tick.return_value = mock_sym
    
    engine = RiskEngine(config_mock, mt5_mock)
    
    # EURUSD
    lot = engine.calculate_lot_size(10.0, "EURUSD")
    assert lot == 0.1
    
    # JPY: PipValue = 10 / 150 * 100 = 6.6666
    # Lot = 10 / (10 * 6.6666) = 10 / 66.666 = 0.150001
    lot_jpy = engine.calculate_lot_size(10.0, "USDJPY")
    assert lot_jpy == 0.15 # rounded to 0.01

def test_check_spread(config_mock):
    engine = RiskEngine(config_mock, None)
    assert engine.check_spread("EURUSD", 1.5) is True
    assert engine.check_spread("EURUSD", 2.5) is False

def test_check_effective_rr(config_mock):
    engine = RiskEngine(config_mock, None)
    passed, rr = engine.check_effective_rr(tp_pips=20, sl_pips=10, spread_pips=1)
    assert passed is False
    
    passed, rr2 = engine.check_effective_rr(tp_pips=30, sl_pips=10, spread_pips=1)
    assert passed is True
