import pytz
from datetime import datetime, time
from typing import Optional, List
from core.logger import get_logger
from core.configengine import Config

logger = get_logger("SessionEngine")

class SessionEngine:
    """Engine to manage trading sessions, killzones, and avoid windows based on IST."""
    
    def __init__(self, config: Config):
        """
        Initializes the SessionEngine with configuration.
        
        Args:
            config (Config): Validated configuration dataclass.
        """
        self.config = config
        self.tz_ist = pytz.timezone("Asia/Kolkata")

    def get_current_ist_time(self) -> datetime:
        """
        Return the current time in IST (Asia/Kolkata).
        
        Returns:
            datetime: Current IST datetime.
        """
        return datetime.now(self.tz_ist)

    def _is_time_in_range(self, current: time, start_str: str, end_str: str) -> bool:
        """
        Check if a given time is within a range, handling midnight crossover.
        
        Args:
            current (time): Current time to check.
            start_str (str): Start time as "HH:MM".
            end_str (str): End time as "HH:MM".
            
        Returns:
            bool: True if in range.
        """
        start = datetime.strptime(start_str, "%H:%M").time()
        end = datetime.strptime(end_str, "%H:%M").time()
        
        if start <= end:
            return start <= current < end
        else:  # Midnight crossover
            return current >= start or current < end

    def get_active_session(self) -> Optional[str]:
        """
        Return the name of the currently active trading session.
        
        Returns:
            str | None: "Asia", "London", "NewYork", or None.
        """
        now_ist = self.get_current_ist_time().time()
        for session, timings in self.config.session_timings.items():
            if self._is_time_in_range(now_ist, timings["start"], timings["end"]):
                return session
        return None

    def get_active_killzone(self) -> Optional[str]:
        """
        Return the name of the currently active killzone.
        
        Returns:
            str | None: "Asia", "London", "NewYork", "LondonClose", or None.
        """
        now_ist = self.get_current_ist_time().time()
        for kz, timings in self.config.killzone_timings.items():
            if self._is_time_in_range(now_ist, timings["start"], timings["end"]):
                return kz
        return None

    def is_killzone_active(self) -> bool:
        """
        Check if any killzone is currently active.
        
        Returns:
            bool: True if active.
        """
        return self.get_active_killzone() is not None

    def get_allowed_pairs(self, session: Optional[str]) -> List[str]:
        """
        Get list of allowed trading pairs for a given session.
        
        Args:
            session (str | None): Current session name.
            
        Returns:
            List[str]: List of allowed symbols.
        """
        if session == "Asia":
            allowed = ["USDJPY", "EURJPY"]
            if "GBPJPY" in self.config.pairs:
                allowed.append("GBPJPY")
            return [p for p in allowed if p in self.config.pairs]
        elif session in ["London", "NewYork"]:
            return self.config.pairs
        return []

    def is_pair_allowed(self, pair: str, session: Optional[str]) -> bool:
        """
        Check if a specific pair is allowed in the current session.
        
        Args:
            pair (str): Trading symbol.
            session (str | None): Current session name.
            
        Returns:
            bool: True if allowed.
        """
        return pair in self.get_allowed_pairs(session)

    def is_avoid_window(self) -> bool:
        """
        Check if the current time falls within an 'avoid window'.
        
        Avoid windows:
        - Monday: London killzone first hour (11:30–12:30 IST)
        - Friday: NY session after 19:00 IST
        
        Returns:
            bool: True if in an avoid window.
        """
        now_ist = self.get_current_ist_time()
        day = now_ist.weekday()  # 0 is Monday, 4 is Friday
        now_time = now_ist.time()

        # Monday 11:30 - 12:30 IST
        if day == 0:
            start_avoid = time(11, 30)
            end_avoid = time(12, 30)
            if start_avoid <= now_time < end_avoid:
                return True

        # Friday after 19:00 IST
        if day == 4:
            if now_time >= time(19, 0):
                return True

        return False

    def log_session_status(self):
        """Logs the current session and killzone status."""
        now_ist = self.get_current_ist_time()
        session = self.get_active_session()
        kz = self.get_active_killzone()
        avoid = self.is_avoid_window()
        
        logger.info(
            f"SESSION | IST: {now_ist.strftime('%H:%M:%S')} | "
            f"Session: {session if session else 'None'} | "
            f"KZ: {kz if kz else 'None'} | "
            f"Avoid: {avoid}"
        )
