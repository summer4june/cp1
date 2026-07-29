import re

with open('backtest_may_june_fix.log', 'r') as f:
    for line in f:
        if '2026-06-04' in line or '2026-06-05' in line:
            if 'GBPJPYm' in line and 'ZGMT' in line and 'SELL' in line:
                print(line.strip())
            elif 'GBPJPYm' in line and 'tapping' in line:
                print(line.strip())
            elif 'GBPJPYm' in line and 'exception' in line:
                print(line.strip())
