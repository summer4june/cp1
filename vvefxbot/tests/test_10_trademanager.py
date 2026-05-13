"""Tests for TradeManager background tracking."""
import pytest
from unittest.mock import MagicMock
from modules.trademanager import TradeManager

def test_price_reached():
    engine = TradeManager(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    # BUY: price >= target
    assert engine._price_reached(1.05, 1.04, "BUY") is True
    assert engine._price_reached(1.03, 1.04, "BUY") is False
    
    # SELL: price <= target
    assert engine._price_reached(1.03, 1.04, "SELL") is True
    assert engine._price_reached(1.05, 1.04, "SELL") is False

def test_handle_loss_guards(config_mock):
    state_mock = MagicMock()
    state_mock.get_daily_state.return_value = {"total_loss_usd": 250.0} # Config has 1000, 20% is 200
    
    engine = TradeManager(config_mock, MagicMock(), state_mock, MagicMock())
    engine._handle_loss_guards("2026-01-01", 10.0, "LOSS", "EURUSD")
    
    state_mock.disable_bot_today.assert_called_once_with("2026-01-01")
