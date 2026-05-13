"""Tests for Logger masking functionality."""
import pytest
import logging
import os
from core.logger import get_logger

def test_logger_masks_sensitive_data(monkeypatch, tmp_path):
    monkeypatch.setenv("MT5_LOGIN", "secret_login")
    monkeypatch.setenv("TELEGRAM_TOKEN", "secret_token")
    
    # We must reset the logger handlers to ensure it writes to temp
    log_file = tmp_path / "test.log"
    logger = logging.getLogger("TestLogger")
    logger.setLevel(logging.DEBUG)
    
    # Remove existing handlers
    logger.handlers = []
    
    handler = logging.FileHandler(log_file)
    from core.logger import SensitiveFilter
    handler.addFilter(SensitiveFilter())
    logger.addHandler(handler)
    
    logger.info("Connecting with secret_login and secret_token")
    handler.flush()
    
    content = log_file.read_text()
    assert "secret_login" not in content
    assert "secret_token" not in content
    assert "****" in content
