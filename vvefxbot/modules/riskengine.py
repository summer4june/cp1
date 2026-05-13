import math
from typing import List, Dict, Any, Tuple
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector

logger = get_logger("RiskEngine")

class RiskEngine:
    """Engine to calculate risk, lot size, and validate trade setups."""

    def __init__(self, config: Config, mt5connector: MT5Connector):
        """
        Initializes the RiskEngine.

        Args:
            config (Config): Validated configuration dataclass.
            mt5connector (MT5Connector): Live MT5 connection.
        """
        self.config = config
        self.mt5 = mt5connector

    def _get_pip_size(self, pair: str) -> float:
        """Helper to get pip size."""
        return 0.01 if "JPY" in pair.upper() else 0.0001

    def _get_pip_value(self, pair: str) -> float:
        """
        Calculate the pip value for a standard lot (100,000 units).
        """
        if pair.upper() in ["EURUSD", "GBPUSD"]:
            return 10.0
        elif "JPY" in pair.upper():
            # Get current price of the pair
            candles = self.mt5.get_candles(pair, "M1", count=1)
            if not candles.empty:
                current_price = candles.iloc[-1]["close"]
                return (10.0 / current_price) * 100.0
            return 10.0  # Fallback if price fetch fails
        elif "USDCAD" in pair.upper():
            candles = self.mt5.get_candles("USDCAD", "M1", count=1)
            if not candles.empty:
                current_price = candles.iloc[-1]["close"]
                return 10.0 / current_price
            return 10.0  # Fallback
        else:
            # Default fallback for other pairs not explicitly stated
            # If USD is quote currency, it's 10. Otherwise approximate.
            if pair.upper().endswith("USD"):
                return 10.0
            else:
                candles = self.mt5.get_candles(pair, "M1", count=1)
                if not candles.empty:
                    current_price = candles.iloc[-1]["close"]
                    # Rough approximation
                    return 10.0 / current_price
                return 10.0

    def calculate_lot_size(self, sl_pips: float, pair: str) -> float:
        """
        Calculate the appropriate lot size based on risk parameters.

        Args:
            sl_pips (float): Stop loss distance in pips.
            pair (str): Trading pair symbol.

        Returns:
            float: Lot size, constrained between 0.01 and 1.0.
        """
        if sl_pips <= 0:
            return 0.01

        risk_amount = self.config.trading_pool_size * (self.config.risk_percent / 100.0)
        pip_value = self._get_pip_value(pair)
        
        lot_size = risk_amount / (sl_pips * pip_value)
        
        # Round to nearest 0.01
        lot_size = round(lot_size, 2)
        
        # Min/Max constraints
        lot_size = max(0.01, lot_size)
        lot_size = min(1.0, lot_size)
        
        return lot_size

    def check_spread(self, pair: str, current_spread: float) -> bool:
        """
        Check if the current spread is within the allowed limit for the pair.
        """
        limit = self.config.spread_limits.get(pair, 999.0)
        return current_spread <= limit

    def check_effective_rr(self, tp_pips: float, sl_pips: float, spread_pips: float) -> Tuple[bool, float]:
        """
        Check if the effective reward-to-risk ratio meets the minimum requirement.
        """
        denominator = sl_pips + spread_pips
        if denominator <= 0:
            return False, 0.0
            
        effective_rr = (tp_pips - spread_pips) / denominator
        return effective_rr >= self.config.effective_rr_min, effective_rr

    def check_spread_vs_sl(self, spread_pips: float, sl_pips: float) -> bool:
        """
        Check if spread exceeds 10% of the SL distance.
        """
        return spread_pips <= (sl_pips * 0.10)

    def check_slippage(self, expected_price: float, executed_price: float, pair: str) -> bool:
        """
        Check if the execution slippage is within the allowed limit.
        """
        pip_size = self._get_pip_size(pair)
        slippage_pips = abs(executed_price - expected_price) / pip_size
        return slippage_pips <= self.config.slippage_max_pips

    def check_portfolio_exposure(self, open_trades: List[Dict[str, Any]]) -> bool:
        """
        Check if total open risk exceeds the maximum allowed portfolio risk.
        """
        total_risk_usd = 0.0
        for trade in open_trades:
            # We assume open_trades dicts may contain 'pair', 'sl_pips', 'lot_total'
            # If not provided natively by DB schema, we might use 'risk_amount' or reconstruct
            
            lot = trade.get("lot_total", 0.0)
            sl_pips = trade.get("sl_pips")
            pair = trade.get("pair")
            
            # If sl_pips and pair are missing, try to reconstruct them from prices
            if sl_pips is None and "sl" in trade and "executed_price" in trade and "pair" in trade:
                pip_size = self._get_pip_size(trade["pair"])
                sl_pips = abs(trade["executed_price"] - trade["sl"]) / pip_size
                
            if sl_pips is not None and pair is not None:
                pip_value = self._get_pip_value(pair)
                total_risk_usd += lot * sl_pips * pip_value
            else:
                # Fallback to risk_amount if pair/sl_pips are unavailable
                total_risk_usd += trade.get("risk_amount", 0.0)

        max_risk_usd = self.config.trading_pool_size * (self.config.max_open_risk_percent / 100.0)
        return total_risk_usd <= max_risk_usd

    def check_max_open_trades(self, open_trades: List[Dict[str, Any]]) -> bool:
        """
        Check if the number of open trades is below the maximum allowed.
        """
        return len(open_trades) < self.config.max_open_trades

    def run_all_checks(self, signal: Dict[str, Any], open_trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run all risk and exposure checks sequentially.
        Fail-fast on the first failed check.
        """
        pair = signal["pair"]
        spread_pips = signal["spread_pips"]
        sl_pips = signal["sl_pips"]
        tp_pips = signal["tp_pips"]

        # 1. Spread Check
        if not self.check_spread(pair, spread_pips):
            return {"pass": False, "failed_check": "check_spread", "lot_size": 0.0, "effective_rr": 0.0}

        # 2. Effective RR Check
        passed_rr, effective_rr = self.check_effective_rr(tp_pips, sl_pips, spread_pips)
        if not passed_rr:
            return {"pass": False, "failed_check": "check_effective_rr", "lot_size": 0.0, "effective_rr": effective_rr}

        # 3. Spread vs SL Check
        if not self.check_spread_vs_sl(spread_pips, sl_pips):
            return {"pass": False, "failed_check": "check_spread_vs_sl", "lot_size": 0.0, "effective_rr": effective_rr}

        # 4. Portfolio Exposure Check
        if not self.check_portfolio_exposure(open_trades):
            return {"pass": False, "failed_check": "check_portfolio_exposure", "lot_size": 0.0, "effective_rr": effective_rr}

        # 5. Max Open Trades Check
        if not self.check_max_open_trades(open_trades):
            return {"pass": False, "failed_check": "check_max_open_trades", "lot_size": 0.0, "effective_rr": effective_rr}

        # All checks passed, calculate lot size
        lot_size = self.calculate_lot_size(sl_pips, pair)

        return {
            "pass": True,
            "failed_check": None,
            "lot_size": lot_size,
            "effective_rr": effective_rr
        }
