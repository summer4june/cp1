import asyncio
from core.mt5connector import MT5Connector
from core.configengine import Config

async def run():
    config = Config("config.json")
    mt5 = MT5Connector(config)
    
    # We can't run MetaTrader5 on Linux... 
    pass
if __name__ == "__main__":
    asyncio.run(run())
