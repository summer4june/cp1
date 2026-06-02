import pandas as pd
import json

def parse_log(logfile):
    with open(logfile, 'r') as f:
        lines = f.readlines()
        
    for line in lines:
        if "Trade PLACED" in line and "USDJPY" in line:
            print(line.strip())
        if "Limit Trade TRIGGERED" in line and "USDJPY" in line:
            print(line.strip())

parse_log("../bot (2).log")
