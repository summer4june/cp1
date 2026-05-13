"""Tests for MT5Connector using mocked MT5 calls."""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from core.mt5connector import MT5Connector

def test_connect_success(config_mock):
    with patch("core.mt5connector.mt5", create=True) as mock_mt5:
        mock_mt5.initialize.return_value = True
        mock_mt5.login.return_value = True
        connector = MT5Connector(config_mock)
        assert connector.connect() is True

def test_connect_failure(config_mock):
    with patch("core.mt5connector.mt5", create=True) as mock_mt5:
        mock_mt5.initialize.return_value = False
        connector = MT5Connector(config_mock)
        assert connector.connect() is False

def test_get_current_spread(config_mock):
    with patch("core.mt5connector.mt5", create=True) as mock_mt5:
        mock_sym_info = MagicMock()
        mock_sym_info.spread = 15
        mock_mt5.symbol_info.return_value = mock_sym_info
        
        connector = MT5Connector(config_mock)
        
        # JPY pairs: spread_points * point / 0.01
        mock_sym_info.point = 0.001 # 15 * 0.001 / 0.01 = 1.5
        assert connector.get_current_spread("USDJPY") == 1.5
        
        # Standard pairs: spread_points * point / 0.0001
        mock_sym_info.point = 0.00001 # 15 * 0.00001 / 0.0001 = 1.5
        assert connector.get_current_spread("EURUSD") == 1.5

def test_get_candles_empty(config_mock):
    with patch("core.mt5connector.mt5", create=True) as mock_mt5:
        mock_mt5.TIMEFRAME_M15 = 15
        mock_mt5.TIMEFRAME_M1 = 1
        mock_mt5.copy_rates_from_pos.return_value = None
        connector = MT5Connector(config_mock)
        df = connector.get_candles("EURUSD", "M15")
        assert df.empty is True
