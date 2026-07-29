import pandas as pd
df = pd.read_csv('backtest/data/GBPJPYm_15m.csv')
df['time'] = pd.to_datetime(df['time'])
mask = (df['time'] >= '2026-06-04') & (df['time'] <= '2026-06-06')
print(df[mask]['high'].max())
print(df[mask][df[mask]['high'] > 215.1])
