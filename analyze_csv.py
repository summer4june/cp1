import pandas as pd
df = pd.read_csv("merge-csv.com__6a29411d63986.csv", skiprows=3)
print(df[df['entry_leg'] == 'B']['session'].value_counts())
