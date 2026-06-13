import unittest
from unittest.mock import MagicMock, patch
import os
import json
from datetime import datetime, timezone
import sys

# Mock MetaTrader5 before anything else imports it
sys.modules['MetaTrader5'] = MagicMock()

# Ensure we can import from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.vaultengine import VaultEngine

class TestVaultEngine(unittest.TestCase):
    def setUp(self):
        self.config_path = "tests/test_vault_config.json"
        self.db_path = "tests/test_vault.sqlite"
        
        # Setup initial config
        initial_config = {
            "start_balance": 100.0,
            "trading_balance": 100.0,
            "vault_balance": 0.0,
            "divide_factor": 15,
            "vault_percentage": 0.50,
            "min_trading_balance": 70.0,
            "max_daily_drawdown_pct": 0.20
        }
        with open(self.config_path, "w") as f:
            json.dump(initial_config, f)
            
        self.engine = VaultEngine(config_path=self.config_path, db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except PermissionError:
                pass

    def test_get_current_risk_amount(self):
        # 100 / 15 = 6.666...
        risk = self.engine.get_current_risk_amount()
        self.assertAlmostEqual(risk, 6.6666666, places=4)

    def test_process_end_of_day_profit(self):
        mock_state = MagicMock()
        mock_state.get_daily_state.return_value = {"daily_profit_usd": 10.0}
        
        self.engine.process_end_of_day(mock_state)
        
        config = self.engine.get_vault_config()
        self.assertEqual(config["vault_balance"], 5.0)  # 50% to vault
        self.assertEqual(config["trading_balance"], 105.0)  # 50% to trading balance

    def test_process_end_of_day_loss(self):
        mock_state = MagicMock()
        mock_state.get_daily_state.return_value = {"daily_profit_usd": -10.0}
        
        self.engine.process_end_of_day(mock_state)
        
        config = self.engine.get_vault_config()
        self.assertEqual(config["vault_balance"], 0.0) 
        self.assertEqual(config["trading_balance"], 90.0) 

    def test_process_end_of_day_deficit_vault_covers(self):
        # Start with some vault balance
        config = self.engine.get_vault_config()
        config["vault_balance"] = 20.0
        self.engine.save_vault_config(config)

        mock_state = MagicMock()
        mock_state.get_daily_state.return_value = {"daily_profit_usd": -40.0}
        
        self.engine.process_end_of_day(mock_state)
        
        config = self.engine.get_vault_config()
        # TB drops to 60. Min is 70. Deficit = 10. Vault covers 10.
        self.assertEqual(config["vault_balance"], 10.0) 
        self.assertEqual(config["trading_balance"], 70.0)

    def test_process_end_of_day_deficit_vault_depleted(self):
        # Start with some vault balance
        config = self.engine.get_vault_config()
        config["vault_balance"] = 5.0
        self.engine.save_vault_config(config)

        mock_state = MagicMock()
        mock_state.get_daily_state.return_value = {"daily_profit_usd": -40.0}
        
        self.engine.process_end_of_day(mock_state)
        
        config = self.engine.get_vault_config()
        # TB drops to 60. Min is 70. Deficit = 10. Vault only has 5.
        # Vault drops to 0. TB = 60 + 5 = 65.
        self.assertEqual(config["vault_balance"], 0.0) 
        self.assertEqual(config["trading_balance"], 65.0)

    def test_check_drawdown_panic(self):
        mock_state = MagicMock()
        # Realized PnL is -5.0
        mock_state.get_daily_state.return_value = {"daily_profit_usd": -5.0}
        mock_mt5 = MagicMock()
        
        # Unrealized PnL is -16.0
        # Total = -21.0. TB = 100. 20% limit = 20.0.
        # -21 <= -20, should trigger panic.
        panic = self.engine.check_drawdown(mock_state, mock_mt5, -16.0)
        
        self.assertTrue(panic)
        mock_mt5.close_all_positions.assert_called_once()
        mock_state.disable_bot_today.assert_called_once()

    def test_check_drawdown_safe(self):
        mock_state = MagicMock()
        # Realized PnL is 5.0
        mock_state.get_daily_state.return_value = {"daily_profit_usd": 5.0}
        mock_mt5 = MagicMock()
        
        # Unrealized PnL is -16.0
        # Total = -11.0. TB = 100. 20% limit = 20.0.
        # -11 > -20, should NOT trigger panic.
        panic = self.engine.check_drawdown(mock_state, mock_mt5, -16.0)
        
        self.assertFalse(panic)
        mock_mt5.close_all_positions.assert_not_called()
        mock_state.disable_bot_today.assert_not_called()

if __name__ == '__main__':
    unittest.main()
