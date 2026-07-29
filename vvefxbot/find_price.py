import pandas as pd

df = pd.read_csv('backtest/data/GBPJPYm_M5_2026-05-01_2026-06-30.csv', parse_dates=['datetime'])
df_filtered = df[(df['datetime'] >= '2026-06-04') & (df['datetime'] <= '2026-06-06')]

for index, row in df_filtered.iterrows():
    if row['high'] >= 215.1:
        print(f"Price {row['high']} at {row['datetime']}")
