import pandas as pd

# Let's say H1 open is 157.987
zgmt_price = 157.987
# M1 candle at 00:00 has exactly the same open
m1_open = 157.987
m1_high = 157.987
m1_low = 157.950

entry = round(zgmt_price, 5)
print("entry <= high:", entry <= m1_high)
print("entry >= low:", entry >= m1_low)

