"""Tests for Logger masking functionality."""
import pytest
import logging
import os
from core.logger import get_logger

def test_logger_masks_sensitive_data(monkeypatch, tmp_path):
    monkeypatch.setenv("MT5_LOGIN", "secret_login")
    monkeypatch.setenv("TELEGRAM_TOKEN", "secret_token")
    
    # We must reset the logger handlers to ensure it writes to temp
    log_file = tmp_path / "logs" / "bot.log"
    import os
    orig_dir = os.getcwd()
    os.chdir(tmp_path)
    try:
        logger = get_logger("TestLogger")
        # Ensure we write to our temporary file
        logger.info("Connecting with secret_login and secret_token")
        
        for handler in logger.handlers:
            handler.flush()
            
        content = log_file.read_text()
        assert "secret_login" not in content
        assert "secret_token" not in content
        assert "********" in content
    finally:
        os.chdir(orig_dir)
        # Clean up handlers for next tests
        logger.handlers.clear()
