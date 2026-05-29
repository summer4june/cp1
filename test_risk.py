import json

config_str = """
{
  "trading_pool_size": 1000.0,
  "risk_percent": 1.0,
  "max_open_risk_percent": 6.0
}
"""
config = json.loads(config_str)

open_trades = [
    {"pair": "EURUSD", "direction": "SELL", "lot_total": 0.01, "risk_amount": config["trading_pool_size"] * config["risk_percent"] / 100}
]

total_risk_usd = 0.0
for trade in open_trades:
    total_risk_usd += trade.get("risk_amount", 0.0)

max_risk_usd = config["trading_pool_size"] * (config["max_open_risk_percent"] / 100.0)

print(f"Total risk: {total_risk_usd}, Max risk: {max_risk_usd}")
print(f"Pass: {total_risk_usd <= max_risk_usd}")
