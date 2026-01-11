import pandas as pd
df = pd.read_csv('exports/waste_listings_with_pricing.csv', nrows=1)
print(df.columns.tolist())
