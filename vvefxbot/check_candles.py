import pandas as pd
import datetime

# Load the GBPJPYm 1h data
df = pd.read_csv('data/raw/GBPJPYm_1h_20240101_20260710.csv')
df['time'] = pd.to_datetime(df['time'])

# Filter for June 4th and 5th
mask = (df['time'] >= '2026-06-04') & (df['time'] < '2026-06-06')
df_filtered = df.loc[mask]

for idx, row in df_filtered.iterrows():
    print(f"{row['time']}: O={row['open']} H={row['high']} L={row['low']} C={row['close']}")
