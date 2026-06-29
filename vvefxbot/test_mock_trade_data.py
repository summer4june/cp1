import pytest
from unittest.mock import patch, MagicMock

def test_debug():
    from core.stateengine import StateEngine
    from modules.executionengine import ExecutionEngine
    
    mt5_mock = MagicMock()
    mt5_mock.get_current_spread.return_value = 1.0
    mt5_mock.order_calc_profit.return_value = 10.0
    mt5_mock.order_calc_margin.return_value = 5.0
    mt5_mock.place_order.return_value = {"success": True, "ticket": 12345}
    
    mock_pos = MagicMock()
    mock_pos.price_open = 1.0000
    
    config_mock = MagicMock()
    config_mock.pairs = ["EURUSD"]
    config_mock.max_trades_pair_day = 5
    config_mock.max_trades_day = 10
    config_mock.trading_pool_size = 1000.0
    config_mock.risk_percent = 1.0
    
    state_engine = MagicMock()
    state_engine.is_bot_disabled_today.return_value = False
    state_engine.get_pair_trades_today.return_value = 0
    state_engine.get_daily_state.return_value = {}
    
    risk_engine = MagicMock()
    risk_engine.run_all_checks.return_value = {"pass": True, "lot_size": 0.1, "risk_amount_usd": 10.0}
    
    engine = ExecutionEngine(config_mock, mt5_mock, risk_engine, state_engine, MagicMock())
    
    signal = {
        "signal_id": "sig_123", "pair": "EURUSD", "session": "London",
        "direction": "BUY", "entry_price": 1.0, "sl_price": 0.9,
        "tp1_price": 1.1, "tp2_price": 1.2, "sl_pips": 10, "tp_pips": 20,
        "spread_pips": 1, "effective_rr": 2.0, "score": 90, "detected_time": "2026-01-01"
    }
    
    # We want to see what is passed to state_engine.insert_trade
    def dummy_insert(trade):
        for i, (k, v) in enumerate(trade.items()):
            print(f"{i+1}. {k}: {type(v)} = {v}")
    
    state_engine.insert_trade.side_effect = dummy_insert
    engine.execute_signal_by_dict(signal, is_pending=False)

if __name__ == "__main__":
    test_debug()
