import re

with open("modules/scannerzgmt.py", "r") as f:
    text = f.read()

target = """        for _, row in candles.iterrows():
            candle_time = row["time"]
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Only evaluate candles that opened after the exclusion window
            if candle_time < test_start_broker_time:
                continue

            candle_low = float(row["low"])
            candle_high = float(row["high"])

            # "Tested" = price actually touched or crossed the 0GMT level.
            if candle_low <= zgmt_price <= candle_high:
                logger.debug(
                    f"[{pair}] ZGMT Step 2B: Level ALREADY TESTED (price touched). "
                    f"Candle H={candle_high:.5f} L={candle_low:.5f} vs ZGMT={zgmt_price:.5f}"
                )
                return True

        logger.debug(f"[{pair}] ZGMT Step 2B: Level NOT YET TESTED. Proceeding.")
        return False"""

replacement = """        times = candles["time"]
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

if target in text:
    print("Exact match found!")
    text = text.replace(target, replacement)
else:
    print("No exact match. Trying regex.")
    pattern = re.compile(r'        for _, row in candles\.iterrows\(\):\n            candle_time = row\["time"\]\n.*?return False', re.DOTALL)
    if pattern.search(text):
        print("Regex match found!")
        text = pattern.sub(replacement, text, count=1)
    else:
        print("No regex match.")

with open("modules/scannerzgmt.py", "w") as f:
    f.write(text)
