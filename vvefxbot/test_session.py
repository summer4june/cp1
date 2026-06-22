from core.config import Config
from core.sessionengine import SessionEngine

config = Config.load()
engine = SessionEngine(config)
session = engine.get_active_session()
kz = engine.get_active_killzone()
print("Session:", repr(session), type(session))
print("KZ:", repr(kz), type(kz))
