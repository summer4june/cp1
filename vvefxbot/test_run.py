import asyncio
from core.configengine import ConfigEngine
from backtest.engine import BacktestEngine
from backtest import load_from_csv, fetch_all_timeframes, BacktestConnector
from modules.scannerzgmt import ScannerZGMT
from core.stateengine import StateEngine

async def run():
    config = ConfigEngine()
    config.demo_mode = True
    config.strategy_mode = "ZGMT"
    if isinstance(config.zgmt_scanner, dict):
        config.zgmt_scanner["enabled"] = True
    
    # Just mock enough to test if _get_broker_utc_offset_hours runs
    class MockConnector:
        def get_historical_data(self, *args, **kwargs):
            return None
            
    # Mock MT5
    class MockMT5:
        def get_tick(self, pair):
            from datetime import datetime, timezone
            return {"bid": 1.0, "ask": 1.0, "time": datetime(2026, 5, 20, tzinfo=timezone.utc).timestamp()}
        def current_time(self):
            from datetime import datetime, timezone
            return datetime(2026, 5, 20, tzinfo=timezone.utc)
            
    scanner = ScannerZGMT(config, None, StateEngine(":memory:"))
    scanner.mt5 = MockMT5()
    
    offset = scanner._get_broker_utc_offset_hours("EURUSD")
    print(f"OFFSET: {offset}")

asyncio.run(run())
