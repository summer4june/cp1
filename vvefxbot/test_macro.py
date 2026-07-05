import datetime
from datetime import time as dt_time

MACRO_WINDOWS = [
    (17, 50, 18, 20, "Macro 1", "Manipulation"),
    (18, 20, 18, 40, "Macro 2", "Continuation"),
    (18, 40, 19, 20, "Macro 3", "Manipulation"),
    (19, 20, 19, 40, "Macro 4", "Continuation"),
    (19, 40, 20, 20, "Silver Bullet 5", "Manipulation"),
    (20, 20, 20, 40, "Silver Bullet 6", "Continuation"),
    (22, 50, 23, 20, "Reversal 7", "Manipulation"),
    (23, 20, 23, 40, "Reversal 8", "Continuation"),
    (23, 40,  0, 20, "Reversal 9", "Manipulation"),
    ( 0, 20,  0, 40, "Reversal 10", "Continuation")
]

def get_active(curr_t):
    for (sh, sm, eh, em, name, wtype) in MACRO_WINDOWS:
        start_t = dt_time(sh, sm)
        end_t = dt_time(eh, em)
        if start_t > end_t:
            if curr_t >= start_t or curr_t < end_t:
                return name
        else:
            if start_t <= curr_t < end_t:
                return name
    return None

import pandas as pd
start_utc = pd.Timestamp("2026-06-24 00:00:00", tz="UTC")
matches = 0
for i in range(10000):
    now_utc = start_utc + pd.Timedelta(minutes=i)
    now_ist = now_utc + datetime.timedelta(hours=5, minutes=30)
    if get_active(now_ist.time()):
        matches += 1

print(f"Total matches in 10000 minutes: {matches}")
