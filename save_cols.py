import pandas as pd
df = pd.read_csv('exports/waste_listings_with_pricing.csv', nrows=1)
with open('cols.txt', 'w') as f:
    f.write(','.join(df.columns.tolist()))
