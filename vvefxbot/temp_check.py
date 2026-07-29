import pandas as pd
df = pd.read_csv('backtest/data/GBPJPYm_1_2026.csv', names=['date', 'time', 'open', 'high', 'low', 'close', 'tickvol', 'vol', 'spread'], sep='\t')
df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
df = df[(df['datetime'] >= '2026-06-04') & (df['datetime'] < '2026-06-06')]
max_high = df['high'].max()
max_row = df[df['high'] == max_high]
print(f"Max high on June 4/5: {max_high}")
print(max_row)
