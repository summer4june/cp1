# Dummy MetaTrader5 module for testing on non-Windows platforms
def initialize(): return True
def login(*args, **kwargs): return True
def shutdown(): pass
def symbol_info(*args): return None
def symbol_info_tick(*args): return None
def copy_rates_from_pos(*args, **kwargs): return None
def order_send(*args, **kwargs): return None
def positions_get(*args, **kwargs): return None
def account_info(): return None

TIMEFRAME_M1 = 1
TIMEFRAME_M15 = 15
TIMEFRAME_H1 = 60
