import sys
from unittest.mock import MagicMock
sys.modules["MetaTrader5"] = MagicMock()
sys.path.append("/Users/Vikas/Documents/cp1/vvefxbot")

from core.configengine import ConfigEngine
from modules.riskengine import RiskEngine

config = ConfigEngine().config
print(f"Max Risk Percent: {config.max_open_risk_percent}")

engine = RiskEngine(config, None)

open_trades = [
    {"pair": "EURUSD", "direction": "SELL", "lot_total": 0.01, "risk_amount": config.trading_pool_size * config.risk_percent / 100}
]

res = engine.check_portfolio_exposure(open_trades)
print("Pass 1 trade:", res)

# Try adding more trades
open_trades = [
    {"pair": "EURUSD", "direction": "SELL", "lot_total": 0.01, "risk_amount": config.trading_pool_size * config.risk_percent / 100}
] * 6
res2 = engine.check_portfolio_exposure(open_trades)
print("Pass 6 trades:", res2)

open_trades = [
    {"pair": "EURUSD", "direction": "SELL", "lot_total": 0.01, "risk_amount": config.trading_pool_size * config.risk_percent / 100}
] * 7
res3 = engine.check_portfolio_exposure(open_trades)
print("Pass 7 trades:", res3)
