import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Any
from dotenv import load_dotenv

@dataclass
class Config:
    """Dataclass to hold all configuration settings for the trading bot."""
    # From config.json
    strategy_mode: str
    enabled_scanners: Dict[str, bool]
    ote_scanner: Dict[str, Any]
    pairs: List[str]
    session_timings: Dict[str, Dict[str, str]]
    killzone_timings: Dict[str, Dict[str, str]]
    risk_percent: float
    max_trades_day: int
    max_trades_pair_day: int
    max_open_trades: int
    max_open_risk_percent: float
    spread_limits: Dict[str, float]
    effective_rr_min: float
    aplus_threshold: float
    slippage_max_pips: float
    scan_frequency_seconds: int
    correlation_groups: Dict[str, List[str]]
    demo_mode: bool
    trading_pool_size: float
    
    # From .env
    mt5_login: int
    mt5_password: str
    mt5_server: str
    telegram_token: str
    telegram_chat_id: str
    google_sheet_id: str
    google_creds_path: str

class ConfigEngine:
    """Engine to load, validate, and provide access to the bot configuration."""
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initializes the ConfigEngine.
        
        Args:
            config_path (str): Path to the config.json file.
        """
        self.config_path = config_path
        self.config: Config = self._load_and_validate()

    def _load_and_validate(self) -> Config:
        """
        Loads configuration from config.json and .env and validates all fields.
        
        Returns:
            Config: The validated Config dataclass.
            
        Raises:
            ValueError: If any required field is missing or invalid.
        """
        # Load .env
        load_dotenv()
        
        # Load config.json
        if not os.path.exists(self.config_path):
            raise ValueError(f"CONFIG ERROR: {self.config_path} missing")
            
        with open(self.config_path, "r") as f:
            try:
                json_data = json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"CONFIG ERROR: {self.config_path} is not a valid JSON")

        # Set defaults for new backward-compatible keys
        if "strategy_mode" not in json_data:
            json_data["strategy_mode"] = "MMXM"
        if "enabled_scanners" not in json_data:
            json_data["enabled_scanners"] = {"mmxm": True, "ote": False}
        if "ote_scanner" not in json_data:
            json_data["ote_scanner"] = {"enabled": False}

        # Required config.json keys
        json_keys = [
            "strategy_mode", "enabled_scanners", "ote_scanner",
            "pairs", "session_timings", "killzone_timings", "risk_percent", 
            "max_trades_day", "max_trades_pair_day", "max_open_trades", 
            "max_open_risk_percent", "spread_limits", "effective_rr_min", 
            "aplus_threshold", "slippage_max_pips", "scan_frequency_seconds", 
            "correlation_groups", "demo_mode", "trading_pool_size"
        ]
        
        # Required .env keys
        env_keys = [
            "MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER", "TELEGRAM_TOKEN", 
            "TELEGRAM_CHAT_ID", "GOOGLE_SHEET_ID", "GOOGLE_CREDS_PATH"
        ]

        # Validate json keys
        for key in json_keys:
            if key not in json_data:
                raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            
            # Simple type validation for some fields
            if key in ["risk_percent", "max_open_risk_percent", "effective_rr_min", "aplus_threshold", "slippage_max_pips", "trading_pool_size"]:
                if not isinstance(json_data[key], (int, float)):
                    raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            elif key in ["max_trades_day", "max_trades_pair_day", "max_open_trades", "scan_frequency_seconds"]:
                if not isinstance(json_data[key], int):
                    raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            elif key == "demo_mode":
                if not isinstance(json_data[key], bool):
                    raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            elif key == "pairs":
                if not isinstance(json_data[key], list):
                    raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            elif key in ["session_timings", "killzone_timings", "spread_limits", "correlation_groups", "enabled_scanners", "ote_scanner"]:
                if not isinstance(json_data[key], dict):
                    raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            elif key == "strategy_mode":
                if json_data[key] not in ["MMXM", "OTE", "MULTI"]:
                    raise ValueError(f"CONFIG ERROR: strategy_mode must be MMXM, OTE, or MULTI")

        # Validate OTE specific keys if enabled or if present
        ote = json_data.get("ote_scanner", {})
        if ote and ote.get("enabled", False):
            if ote.get("fib_min", 0) < 0 or ote.get("fib_max", 0) > 1 or ote.get("fib_min", 0) >= ote.get("fib_max", 0):
                raise ValueError("CONFIG ERROR: OTE fib_min must be < fib_max, >= 0, and <= 1")
            if ote.get("ema_period", 0) <= 0:
                raise ValueError("CONFIG ERROR: OTE ema_period must be > 0")
            if ote.get("range_start_index", -1) < 0:
                raise ValueError("CONFIG ERROR: OTE range_start_index must be >= 0")
            if ote.get("range_end_index", 0) <= ote.get("range_start_index", 0):
                raise ValueError("CONFIG ERROR: OTE range_end_index must be > range_start_index")
            if ote.get("min_h1_candles", 0) <= 0:
                raise ValueError("CONFIG ERROR: OTE min_h1_candles must be > 0")
            if ote.get("fetch_h1_candles", 0) < ote.get("min_h1_candles", 0):
                raise ValueError("CONFIG ERROR: OTE fetch_h1_candles must be >= min_h1_candles")
            if ote.get("sl_points", 0) <= 0:
                raise ValueError("CONFIG ERROR: OTE sl_points must be > 0")
            if ote.get("tp_points", 0) <= 0:
                raise ValueError("CONFIG ERROR: OTE tp_points must be > 0")
            if ote.get("cooldown_minutes", -1) < 0:
                raise ValueError("CONFIG ERROR: OTE cooldown_minutes must be >= 0")
            if ote.get("max_daily_trades", 0) <= 0:
                raise ValueError("CONFIG ERROR: OTE max_daily_trades must be > 0")

        # Validate .env keys
        env_values = {}
        for key in env_keys:
            val = os.getenv(key)
            if val is None or val == "":
                raise ValueError(f"CONFIG ERROR: {key} missing or invalid")
            env_values[key.lower()] = val

        # Special handling for MT5_LOGIN which should be an integer
        try:
            env_values["mt5_login"] = int(env_values["mt5_login"])
        except ValueError:
            raise ValueError("CONFIG ERROR: MT5_LOGIN missing or invalid")

        # Construct and return Config dataclass
        return Config(
            strategy_mode=json_data["strategy_mode"],
            enabled_scanners=json_data["enabled_scanners"],
            ote_scanner=json_data["ote_scanner"],
            pairs=json_data["pairs"],
            session_timings=json_data["session_timings"],
            killzone_timings=json_data["killzone_timings"],
            risk_percent=float(json_data["risk_percent"]),
            max_trades_day=int(json_data["max_trades_day"]),
            max_trades_pair_day=int(json_data["max_trades_pair_day"]),
            max_open_trades=int(json_data["max_open_trades"]),
            max_open_risk_percent=float(json_data["max_open_risk_percent"]),
            spread_limits=json_data["spread_limits"],
            effective_rr_min=float(json_data["effective_rr_min"]),
            aplus_threshold=float(json_data["aplus_threshold"]),
            slippage_max_pips=float(json_data["slippage_max_pips"]),
            scan_frequency_seconds=int(json_data["scan_frequency_seconds"]),
            correlation_groups=json_data["correlation_groups"],
            demo_mode=bool(json_data["demo_mode"]),
            trading_pool_size=float(json_data["trading_pool_size"]),
            mt5_login=env_values["mt5_login"],
            mt5_password=env_values["mt5_password"],
            mt5_server=env_values["mt5_server"],
            telegram_token=env_values["telegram_token"],
            telegram_chat_id=env_values["telegram_chat_id"],
            google_sheet_id=env_values["google_sheet_id"],
            google_creds_path=env_values["google_creds_path"]
        )

    def get_config(self) -> Config:
        """
        Returns the validated Config dataclass.
        
        Returns:
            Config: The configuration settings.
        """
        return self.config
