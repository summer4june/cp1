"""Tests for SessionEngine and time conversions using freezegun."""
import pytest
from freezegun import freeze_time
from modules.sessionengine import SessionEngine

def test_get_current_ist_time(config_mock):
    engine = SessionEngine(config_mock)
    # Freeze to 12:00 UTC -> 17:30 IST
    with freeze_time("2026-05-13 12:00:00", tz_offset=0):
        ist_time = engine.get_current_ist_time()
        assert ist_time.hour == 17
        assert ist_time.minute == 30

def test_get_active_session(config_mock):
    engine = SessionEngine(config_mock)
    # 07:00 IST -> Asia Session
    with freeze_time("2026-05-13 01:30:00", tz_offset=0): # 07:00 IST
        assert engine.get_active_session() == "Asia"
        
    # 15:00 IST -> London Session
    with freeze_time("2026-05-13 09:30:00", tz_offset=0): # 15:00 IST
        assert engine.get_active_session() == "London"

def test_get_active_killzone(config_mock):
    engine = SessionEngine(config_mock)
    # 08:00 IST -> Asia Killzone
    with freeze_time("2026-05-13 02:30:00", tz_offset=0): # 08:00 IST
        assert engine.get_active_killzone() == "Asia"
        assert engine.is_killzone_active() is True
        
    # 11:00 IST -> Not in any killzone
    with freeze_time("2026-05-13 05:30:00", tz_offset=0): # 11:00 IST
        assert engine.get_active_killzone() is None
        assert engine.is_killzone_active() is False

def test_avoid_window_monday(config_mock):
    engine = SessionEngine(config_mock)
    # Monday 13:00 IST
    with freeze_time("2026-05-11 07:30:00", tz_offset=0):
        assert engine.is_avoid_window() is True
