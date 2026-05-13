"""Tests for SQLite StateEngine persistence."""
import pytest
from core.stateengine import StateEngine

def test_db_init_and_signal(temp_db):
    engine = StateEngine(db_path=temp_db)
    signal = {
        "signal_id": "sig_1", "pair": "EURUSD", "session": "London",
        "timeframe_bias": "M15", "timeframe_entry": "M1", "direction": "BUY",
        "bias_summary": "Test", "entry_price": 1.0, "sl_price": 0.9,
        "tp1_price": 1.1, "tp2_price": 1.2, "sl_pips": 10, "tp_pips": 20,
        "spread_pips": 1, "effective_rr": 2.0, "score": 90, "detected_time": "2026-01-01"
    }
    engine.insert_signal(signal)
    assert engine.signal_exists("sig_1") is True
    assert engine.signal_exists("sig_2") is False

def test_trade_insertion_and_get_open(temp_db):
    engine = StateEngine(db_path=temp_db)
    trade = {
        "trade_id": "trd_1", "signal_id": "sig_1", "ticket_id": 123,
        "pair": "EURUSD", "direction": "BUY", "executed_price": 1.0,
        "sl": 0.9, "tp1": 1.1, "tp2": 1.2, "lot_total": 0.1,
        "risk_amount": 10.0, "execution_time": "2026-01-01",
        "status": "OPEN", "result": None, "profit_usd": 0.0
    }
    engine.insert_trade(trade)
    open_trades = engine.get_open_trades()
    assert len(open_trades) == 1
    assert open_trades[0]["trade_id"] == "trd_1"

def test_daily_state_guards(temp_db):
    engine = StateEngine(db_path=temp_db)
    today = "2026-01-01"
    assert engine.is_bot_disabled_today(today) is False
    engine.disable_bot_today(today)
    assert engine.is_bot_disabled_today(today) is True

def test_pair_cooldown(temp_db):
    engine = StateEngine(db_path=temp_db)
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    future = now + timedelta(minutes=15)
    engine.set_pair_cooldown("EURUSD", future)
    assert engine.is_pair_on_cooldown("EURUSD") is True
    
    past = now - timedelta(minutes=1)
    engine.set_pair_cooldown("GBPUSD", past)
    assert engine.is_pair_on_cooldown("GBPUSD") is False
