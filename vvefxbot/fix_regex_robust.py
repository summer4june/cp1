import re

with open("modules/scannerzgmt.py", "r") as f:
    text = f.read()

def replace_iterrows(match):
    return """        times = candles["time"]
        if times.dt.tz is None:
            times = times.dt.tz_localize(timezone.utc)
            
        mask = (times >= test_start_broker_time)
        valid_candles = candles[mask]
        
        if not valid_candles.empty:
            lows = valid_candles["low"].values.astype(float)
            highs = valid_candles["high"].values.astype(float)
            
            tested = (lows <= zgmt_price) & (zgmt_price <= highs)
            if tested.any():
                idx = tested.argmax()
                candle_high = highs[idx]
                candle_low = lows[idx]
                logger.debug(f"[{pair}] ZGMT Step 2B: Level ALREADY TESTED (price touched). Candle H={candle_high:.5f} L={candle_low:.5f} vs ZGMT={zgmt_price:.5f}")
                return True

        logger.debug(f"[{pair}] ZGMT Step 2B: Level NOT YET TESTED. Proceeding.")
        return False"""

# The iterrows is inside _is_zgmt_level_tested
pattern = re.compile(r'for _, row in candles\.iterrows\(\):.*?logger\.debug\(f"\[\{pair\}\] ZGMT Step 2B: Level NOT YET TESTED\. Proceeding\."\)\n\s+return False', re.DOTALL)
text, num = pattern.subn(replace_iterrows, text)
print(f"Replaced {num} times")

with open("modules/scannerzgmt.py", "w") as f:
    f.write(text)
