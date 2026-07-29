import pandas as pd
import datetime

df = pd.read_parquet('data/GBPJPYm_M5_2026-01-01_2026-07-20.parquet')
df['time'] = pd.to_datetime(df['time'])
df = df.set_index('time')

mask = (df.index >= '2026-06-04') & (df.index < '2026-06-06')
subset = df[mask]
print("Max High:", subset['high'].max())
print("Time of Max High:", subset['high'].idxmax())

