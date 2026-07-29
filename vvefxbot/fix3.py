import sys

try:
    with open("modules/scannerzgmt.py", "r") as f:
        lines = f.readlines()
    
    idx = -1
    for i, line in enumerate(lines):
        if "def _is_zgmt_level_tested" in line:
            idx = i
            break
            
    if idx == -1:
        print("Not found")
        sys.exit(1)
        
    start_idx = -1
    for i in range(idx, len(lines)):
        if "for _, row in candles.iterrows():" in lines[i]:
            start_idx = i
            break
            
    if start_idx == -1:
        print("Iterrows not found")
        sys.exit(1)
        
    end_idx = -1
    for i in range(start_idx, len(lines)):
        if "return False" in lines[i]:
            end_idx = i
            break
            
    print(f"Replacing lines {start_idx} to {end_idx}")
    
    new_block = """        times = candles["time"]
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
        return False\n"""
        
    lines[start_idx:end_idx+1] = [new_block]
    
    with open("modules/scannerzgmt.py", "w") as f:
        f.writelines(lines)
    print("DONE!")
except Exception as e:
    print(f"Error: {e}")
