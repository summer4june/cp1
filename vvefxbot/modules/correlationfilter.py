from typing import List, Dict, Tuple, Optional
from core.configengine import Config

class CorrelationFilter:
    """Filter to prevent highly correlated trades from opening simultaneously."""
    
    def __init__(self, config: Config):
        """
        Initializes the CorrelationFilter.
        
        Args:
            config (Config): Validated configuration dataclass.
        """
        self.config = config

    def get_group(self, pair: str) -> Optional[str]:
        """
        Get the correlation group for a given pair.
        
        Args:
            pair (str): Trading symbol.
            
        Returns:
            str | None: Group key ("A", "B", "C", ...) or None.
        """
        for group, pairs in self.config.correlation_groups.items():
            if pair in pairs:
                return group
        return None

    def get_open_group_pairs(self, group: str, open_trades: List[Dict]) -> List[str]:
        """
        Return a list of pairs from the given group that are currently open.
        
        Args:
            group (str): Group key ("A", "B", "C").
            open_trades (List[Dict]): List of currently open trade dictionaries.
            
        Returns:
            List[str]: List of open pairs belonging to the group.
        """
        open_group_pairs = []
        for trade in open_trades:
            trade_pair = trade.get("pair")
            if trade_pair and self.get_group(trade_pair) == group:
                open_group_pairs.append(trade_pair)
        return open_group_pairs

    def can_trade(self, pair: str, open_trades: List[Dict], direction: str) -> Tuple[bool, str]:
        """
        Check if a new trade on pair is allowed given current open trades and correlation rules.
        
        Args:
            pair (str): Trading symbol for the new signal.
            open_trades (List[Dict]): List of currently open trade dictionaries.
            direction (str): Direction of the new signal ("BUY" or "SELL").
            
        Returns:
            Tuple[bool, str]: (True, "OK") if allowed, (False, REASON) if blocked.
        """
        new_group = self.get_group(pair)
        if not new_group:
            return True, "OK"
            
        for trade in open_trades:
            open_pair = trade.get("pair")
            open_direction = trade.get("direction")
            open_group = self.get_group(open_pair)
            
            if open_group == new_group:
                # Rule 2: Max 1 JPY trade at a time (Group B has max 1)
                if new_group == "B":
                    return False, "JPY_MAX_REACHED"
                
                # Rule 3: EURUSD and GBPUSD (Group A) specific logic
                elif new_group == "A":
                    is_cross_pair = (pair == "EURUSD" and open_pair == "GBPUSD") or \
                                    (pair == "GBPUSD" and open_pair == "EURUSD")
                    
                    if is_cross_pair:
                        if direction == open_direction:
                            return False, "CORR_SAME_DIRECTION"
                        else:
                            # Allow if different directions
                            continue
                    else:
                        # Rule 1: Never 2 trades from same group simultaneously 
                        # (applies if they are the exact same pair in Group A, 
                        # though max_open_trades usually handles this, we block it here too).
                        return False, "GROUP_CONFLICT"
                        
                # Rule 1: General block for any other group (e.g., Group C)
                else:
                    return False, "GROUP_CONFLICT"
                    
        return True, "OK"
