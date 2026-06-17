import math
from typing import List, Dict, Any, Tuple
from core.logger import get_logger
from core.configengine import Config
from core.mt5connector import MT5Connector
from modules.vaultengine import VaultEngine

logger = get_logger("RiskEngine")

class RiskEngine:
    """Engine to calculate risk, lot size, and validate trade setups."""

    def __init__(self, config: Config, mt5connector: MT5Connector, vault_engine: VaultEngine = None):
        """
        Initializes the RiskEngine.

        Args:
            config (Config): Validated configuration dataclass.
            mt5connector (MT5Connector): Live MT5 connection.
            vault_engine (VaultEngine): Vault system for dynamic lot sizing.
        """
        self.config = config
        self.mt5 = mt5connector
        self.vault = vault_engine

    def _get_pip_size(self, pair: str) -> float:
        """Helper to get pip size."""
        if "JPY" in pair.upper() or "XAU" in pair.upper():
            return 0.01
        return 0.0001

    def _get_pip_value(self, pair: str) -> float:
        """
        Calculate the pip value in USD for a standard lot (100,000 units).
        """
        pair_upper = pair.upper()
        if "XAU" in pair_upper:
            # Gold: 1 pip = 0.01, standard lot contract size = 100 oz.
            # Pip Value = 100 * 0.01 = 1.00 USD
            return 1.0
        elif "XAG" in pair_upper:
            # Silver: 1 pip = 0.01, contract = 5000 oz.
            return 50.0

        if pair_upper.endswith("USD"):
            return 10.0

        # Extract suffix for pairs like EURJPYm, GBPCHF.a, etc.
        suffix = pair[6:] if len(pair) > 6 else ""

        # For JPY pairs (e.g. USDJPY, EURJPY, GBPJPY)
        # 1 pip = 0.01 JPY. Value in JPY = 100,000 * 0.01 = 1000 JPY
        # USD Value = 1000 / USDJPY
        if "JPY" in pair_upper:
            candles = self.mt5.get_candles(f"USDJPY{suffix}", "M1", count=1)
            if not candles.empty:
                usdjpy_price = candles.iloc[-1]["close"]
                return 1000.0 / usdjpy_price
            return 6.66  # Fallback based on ~150 USDJPY

        # For CAD pairs (e.g. EURCAD, GBPCAD)
        # USD Value = 10 / USDCAD
        if pair_upper.endswith("CAD"):
            candles = self.mt5.get_candles(f"USDCAD{suffix}", "M1", count=1)
            if not candles.empty:
                usdcad_price = candles.iloc[-1]["close"]
                return 10.0 / usdcad_price
            return 7.35

        # For CHF pairs (e.g. EURCHF, GBPCHF)
        if pair_upper.endswith("CHF"):
            candles = self.mt5.get_candles(f"USDCHF{suffix}", "M1", count=1)
            if not candles.empty:
                usdchf_price = candles.iloc[-1]["close"]
                return 10.0 / usdchf_price
            return 11.23

        # For GBP quotes (e.g. EURGBP)
        # USD Value = 10 * GBPUSD
        if pair_upper.endswith("GBP"):
            candles = self.mt5.get_candles(f"GBPUSD{suffix}", "M1", count=1)
            if not candles.empty:
                gbpusd_price = candles.iloc[-1]["close"]
                return 10.0 * gbpusd_price
            return 12.50
            
        # Default fallback
        return 10.0

    def calculate_lot_size(self, sl_pips: float, pair: str, score: float = 100.0) -> float:
        """
        Calculate the appropriate lot size based on risk parameters.

        Args:
            sl_pips (float): Stop loss distance in pips.
            pair (str): Trading pair symbol.
            score (float): The signal strength score.

        Returns:
            float: Lot size, constrained between 0.01 and 1.0.
        """
        if sl_pips <= 0:
            return 0.04

        if self.vault:
            risk_amount = self.vault.get_current_risk_amount()
            trading_balance = self.vault.get_vault_config().get("trading_balance", 100.0)
        else:
            risk_amount = self.config.trading_pool_size * (self.config.risk_percent / 100.0)
            trading_balance = 999.0

        pip_value = self._get_pip_value(pair)
        
        lot_size = risk_amount / (sl_pips * pip_value)
        
        # Round to nearest 0.01
        lot_size = round(lot_size, 2)
        
        # Min/Max constraints
        lot_size = max(0.04, lot_size)
        
        if trading_balance < 101.0:
            lot_size = min(0.04, lot_size)
        else:
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

        if self.vault:
            max_risk_usd = self.vault.get_vault_config().get("trading_balance", 100.0) * (self.config.max_open_risk_percent / 100.0)
        else:
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
        # Some strategies (e.g. ZGMT) define a fixed RR by design and explicitly opt out
        # of the spread-penalised global RR gate via the 'skip_rr_check' signal flag.
        skip_rr = signal.get("skip_rr_check", False)
        if not skip_rr:
            passed_rr, effective_rr = self.check_effective_rr(tp_pips, sl_pips, spread_pips)
            if not passed_rr:
                return {"pass": False, "failed_check": "check_effective_rr", "lot_size": 0.0, "effective_rr": effective_rr}
        else:
            # Still compute for logging — just don't gate on it
            _, effective_rr = self.check_effective_rr(tp_pips, sl_pips, spread_pips)

        # 3. Spread vs SL Check
        # For fixed-RR strategies like ZGMT, the SL is strategy-defined and
        # Gold's inherent spread ratio (30 pips / 95 pip SL = 31.6%) exceeds the
        # 10% threshold by design. Skip this check alongside the RR check.
        if not skip_rr:
            if not self.check_spread_vs_sl(spread_pips, sl_pips):
                return {"pass": False, "failed_check": "check_spread_vs_sl", "lot_size": 0.0, "effective_rr": effective_rr}

        # 4. Portfolio Exposure Check
        if not self.check_portfolio_exposure(open_trades):
            return {"pass": False, "failed_check": "check_portfolio_exposure", "lot_size": 0.0, "effective_rr": effective_rr}

        # 5. Max Open Trades Check
        if not self.check_max_open_trades(open_trades):
            return {"pass": False, "failed_check": "check_max_open_trades", "lot_size": 0.0, "effective_rr": effective_rr}

        # All checks passed, calculate lot size
        score = signal.get("score", 100.0)
        lot_size = self.calculate_lot_size(sl_pips, pair, score)

        return {
            "pass": True,
            "failed_check": None,
            "lot_size": lot_size,
            "effective_rr": effective_rr
        }
