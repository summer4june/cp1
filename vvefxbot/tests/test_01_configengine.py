"""Tests for ConfigEngine loading and validation."""
import pytest
import os
import json
from core.configengine import ConfigEngine

def test_config_loads_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("MT5_LOGIN", "123")
    monkeypatch.setenv("MT5_PASSWORD", "pass")
    monkeypatch.setenv("MT5_SERVER", "server")
    monkeypatch.setenv("TELEGRAM_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("GOOGLE_SHEET_ID", "dummy_id")
    monkeypatch.setenv("GOOGLE_CREDS_PATH", "dummy.json")
    
    config_data = {
        "pairs": ["EURUSD"], "risk_percent": 1.0, "trading_pool_size": 1000.0,
        "session_timings": {}, "killzone_timings": {}, "session_pairs": {},
        "correlation_groups": {}, "spread_limits": {}, "score_weights": {},
        "score_threshold_aplus": 85, "effective_rr_min": 2.0, "max_open_trades": 5,
        "max_trades_day": 10, "max_trades_pair_day": 3, "scan_frequency_seconds": 10,
        "demo_mode": True, "max_open_risk_percent": 2.0, "slippage_max_pips": 2.0
    }
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))
    
    # Temporarily change dir to tmp_path so it reads config.json
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        engine = ConfigEngine()
        config = engine.get_config()
        assert config.pairs == ["EURUSD"]
        assert str(config.mt5_login) == "123"
        assert config.demo_mode is True
    finally:
        os.chdir(orig_dir)

def test_config_missing_env_raises(monkeypatch):
    # Let it read the real config.json and .env
    # But mock os.getenv to return None specifically for MT5_LOGIN
    import os
    original_getenv = os.getenv
    
    def mock_getenv(key, default=None):
        if key == "MT5_LOGIN":
            return None
        return original_getenv(key, default)
        
    monkeypatch.setattr(os, "getenv", mock_getenv)
    
    with pytest.raises(ValueError, match="missing or invalid"):
        ConfigEngine()

def test_config_missing_json_field_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("MT5_LOGIN", "123")
    monkeypatch.setenv("MT5_PASSWORD", "pass")
    monkeypatch.setenv("MT5_SERVER", "server")
    monkeypatch.setenv("TELEGRAM_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.setenv("GOOGLE_SHEET_ID", "dummy_id")
    monkeypatch.setenv("GOOGLE_CREDS_PATH", "dummy.json")
    
    config_data = {"pairs": ["EURUSD"]} # Missing other keys
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))
    
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        with pytest.raises(ValueError):
            ConfigEngine()
    finally:
        os.chdir(orig_dir)
