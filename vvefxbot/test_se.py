import os
import sys
sys.path.append(os.getcwd())
from core.config import Config
from modules.sessionengine import SessionEngine

cfg = Config.load()
se = SessionEngine(cfg)
s = se.get_active_session()
k = se.get_active_killzone()
print(repr(s), type(s))
print(repr(k), type(k))
