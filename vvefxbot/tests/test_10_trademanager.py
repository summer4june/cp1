"""Tests for TradeManager background tracking."""
import math
import pytest
from unittest.mock import MagicMock, patch
from modules.trademanager import TradeManager


def _make_manager(config_mock=None):
    """Helper: build a TradeManager with a config that has trade_management set."""
    if config_mock is None:
        config_mock = MagicMock()
    config_mock.trade_management = {
        "partial_tp_enabled": True,
        "partial_tp_fraction": 0.5,
        "breakeven_buffer_pips": 30,
    }
    config_mock.trading_pool_size = 1000.0
    return TradeManager(config_mock, MagicMock(), MagicMock(), MagicMock())


# ─────────────────────────────────────────────────────────────────────
# Basic helpers
# ─────────────────────────────────────────────────────────────────────

def test_price_reached():
    tm = _make_manager()
    # BUY: price >= target
    assert tm._price_reached(1.05, 1.04, "BUY") is True
    assert tm._price_reached(1.03, 1.04, "BUY") is False

    # SELL: price <= target
    assert tm._price_reached(1.03, 1.04, "SELL") is True
    assert tm._price_reached(1.05, 1.04, "SELL") is False


def test_pip_size():
    tm = _make_manager()
    assert tm._pip_size("USDJPY") == 0.01
    assert tm._pip_size("XAUUSD") == 0.01
    assert tm._pip_size("EURUSD") == 0.0001
    assert tm._pip_size("GBPUSD") == 0.0001


def test_pips_to_price():
    tm = _make_manager()
    # 30 pips on FX = 30 * 0.0001 = 0.003
    assert abs(tm._pips_to_price("EURUSD", 30) - 0.003) < 1e-9
    # 30 pips on XAUUSD = 30 * 0.01 = 0.30
    assert abs(tm._pips_to_price("XAUUSD", 30) - 0.30) < 1e-9


# ─────────────────────────────────────────────────────────────────────
# Volume step rounding
# ─────────────────────────────────────────────────────────────────────

def test_round_to_volume_step_normal():
    """0.01 lot with 50% fraction = 0.005, floored to step=0.01 gives 0.00 (< step)."""
    tm = _make_manager()
    tm.mt5.get_volume_step = MagicMock(return_value=0.01)

    # 0.02 lot → 50% = 0.01 → step=0.01 → stays 0.01
    assert tm._round_to_volume_step("EURUSD", 0.02 * 0.5) == pytest.approx(0.01)

    # 0.04 lot → 50% = 0.02 → step=0.01 → stays 0.02
    assert tm._round_to_volume_step("EURUSD", 0.04 * 0.5) == pytest.approx(0.02)

    # 0.03 lot → 50% = 0.015 → step=0.01 → floor to 0.01
    assert tm._round_to_volume_step("EURUSD", 0.03 * 0.5) == pytest.approx(0.01)


def test_round_to_volume_step_floor_below_step():
    """0.01 lot with 50% = 0.005; floor to step=0.01 gives 0.0 (warning case)."""
    tm = _make_manager()
    tm.mt5.get_volume_step = MagicMock(return_value=0.01)
    result = tm._round_to_volume_step("EURUSD", 0.005)
    # math.floor(0.005 / 0.01) * 0.01 = 0 * 0.01 = 0.0
    assert result == 0.0


def test_round_to_volume_step_small_step():
    """If broker supports 0.001 step, 0.005 rounds to 0.005."""
    tm = _make_manager()
    tm.mt5.get_volume_step = MagicMock(return_value=0.001)
    assert tm._round_to_volume_step("EURUSD", 0.005) == pytest.approx(0.005)


# ─────────────────────────────────────────────────────────────────────
# BE buffer calculation
# ─────────────────────────────────────────────────────────────────────

def test_be_buffer_buy():
    """BUY: new SL = entry + 30 pips."""
    tm = _make_manager()
    entry = 1.10000
    buffer = tm._pips_to_price("EURUSD", 30)   # 0.0030
    expected_sl = round(entry + buffer, 5)
    assert expected_sl == pytest.approx(1.1030, abs=1e-5)


def test_be_buffer_sell():
    """SELL: new SL = entry - 30 pips."""
    tm = _make_manager()
    entry = 1.10000
    buffer = tm._pips_to_price("EURUSD", 30)
    expected_sl = round(entry - buffer, 5)
    assert expected_sl == pytest.approx(1.0970, abs=1e-5)


def test_be_buffer_gold():
    """XAUUSD BUY: new SL = entry + 30 * 0.01 pips = +0.30."""
    tm = _make_manager()
    entry = 3000.0
    buffer = tm._pips_to_price("XAUUSD", 30)   # 0.30
    expected_sl = round(entry + buffer, 5)
    assert expected_sl == pytest.approx(3000.30)


# ─────────────────────────────────────────────────────────────────────
# Daily guards
# ─────────────────────────────────────────────────────────────────────

def test_handle_loss_guards(config_mock):
    """Daily loss threshold disables bot today."""
    state_mock = MagicMock()
    state_mock.get_daily_state.return_value = {"total_loss_usd": 250.0, "consecutive_losses": 1}

    tm = TradeManager(config_mock, MagicMock(), state_mock, MagicMock())
    tm._handle_loss_guards("2026-01-01", 10.0, "LOSS", "EURUSD")

    state_mock.disable_bot_today.assert_called_once_with("2026-01-01")
