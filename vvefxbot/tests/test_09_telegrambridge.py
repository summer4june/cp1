"""Tests for TelegramBridge signal delivery and callbacks."""
import pytest
from unittest.mock import patch, MagicMock
from modules.telegrambridge import TelegramBridge

def test_send_signal(config_mock):
    with patch("telebot.TeleBot") as bot_mock:
        bridge = TelegramBridge(config_mock, MagicMock(), MagicMock())
        signal = {
            "signal_id": "sig_1", "pair": "EURUSD", "session": "London",
            "direction": "BUY", "entry_price": 1.0, "sl_price": 0.9,
            "tp1_price": 1.1, "tp2_price": 1.2, "sl_pips": 10, "tp_pips": 20,
            "spread_pips": 1, "effective_rr": 2.0, "score": 90
        }
        res = bridge.send_signal(signal, 0.1)
        assert res is True
        assert "sig_1" in bridge._pending

def test_handle_yes_callback_expired(config_mock):
    with patch("telebot.TeleBot") as bot_mock:
        bridge = TelegramBridge(config_mock, MagicMock(), MagicMock())
        from datetime import datetime, timezone, timedelta
        bridge._pending["sig_1"] = {
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=20)
        }
        bridge.handle_yes_callback("sig_1")
        assert "sig_1" not in bridge._pending # Should be removed
