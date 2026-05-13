"""Tests for the ScannerMMXM strategy logic."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from modules.scannermmxm import ScannerMMXM

def mock_candles(trend="up"):
    data = []
    for i in range(20):
        data.append({
            "time": "2026-01-01 10:00:00",
            "open": 1.0 + i*0.001 if trend == "up" else 1.0 - i*0.001,
            "high": 1.001 + i*0.001 if trend == "up" else 1.001 - i*0.001,
            "low": 0.999 + i*0.001 if trend == "up" else 0.999 - i*0.001,
            "close": 1.0005 + i*0.001 if trend == "up" else 1.0005 - i*0.001,
            "tick_volume": 100
        })
    return pd.DataFrame(data)

def test_scanner_bias_check(config_mock):
    state_mock = MagicMock()
    connector = MagicMock()
    
    scanner = ScannerMMXM(config_mock, connector, state_mock)
    
    df = mock_candles("up")
    result = scanner._get_bias(df)
    assert result == "SELL"
    
    df_down = mock_candles("down")
    result_down = scanner._get_bias(df_down)
    assert result_down == "BUY"

def test_liquidity_sweep(config_mock):
    state_mock = MagicMock()
    connector = MagicMock()
    scanner = ScannerMMXM(config_mock, connector, state_mock)
    res = scanner.detect_liquidity_sweep(pd.DataFrame(), "BUY")
    assert res["detected"] is False
