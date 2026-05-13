import pytest
import os
import sqlite3
from unittest.mock import MagicMock

@pytest.fixture
def temp_db():
    db_path = "test_fxbot.db"
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)

@pytest.fixture
def mock_mt5():
    return MagicMock()

@pytest.fixture
def mock_telegram():
    return MagicMock()

@pytest.fixture
def mock_google():
    return MagicMock()

@pytest.fixture
def config_mock():
    config = MagicMock()
    config.pairs = ["EURUSD"]
    return config
