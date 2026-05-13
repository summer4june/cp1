"""Tests for the ScannerMMXM strategy logic."""
import pytest
import pandas as pd
import numpy as np
from modules.scannermmxm import ScannerMMXM

def mock_candles(trend="up"):
    # Generate some dummy data
    data = []
    for i in range(20):
        data.append({
            "time": "2026-01-01 10:00:00",
            "open": 1.0 + i*0.001 if trend == "up" else 1.0 - i*0.001,
            "high": 1.001 + i*0.001 if trend == "up" else 1.001 - i*0.001,
            "low": 0.999 + i*0.001 if trend == "up" else 0.999 - i*0.001,
            "close": 1.0005 + i*0.001 if trend == "up" else 1.0005 - i*0.001,
        })
    return pd.DataFrame(data)

def test_scanner_bias_check(config_mock, mock_mt5):
    from unittest.mock import MagicMock
    state_mock = MagicMock()
    connector = MagicMock()
    
    scanner = ScannerMMXM(config_mock, connector, state_mock)
    
    df = mock_candles("up")
    
    # 50% level = (max_high + min_low)/2
    # In 'up' trend, price ends high, so current price > 50% -> PREMIUM -> SELL bias
    result = scanner._check_bias_m15(df)
    assert result["bias"] == "SELL"
    assert result["level"] > 0
    
    df_down = mock_candles("down")
    result_down = scanner._check_bias_m15(df_down)
    assert result_down["bias"] == "BUY"

def test_liquidity_sweep():
    # To test fully we would create specific data frames, here we ensure it doesn't crash on empty
    state_mock = MagicMock()
    connector = MagicMock()
    scanner = ScannerMMXM(MagicMock(), connector, state_mock)
    assert scanner._detect_liquidity_sweep(pd.DataFrame(), "BUY") == (False, 0.0)
