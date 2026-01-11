import pandas as pd
import json

print('VERIFYING ENRICHMENT QUALITY')
try:
    df = pd.read_csv('exports/waste_listings_granular.csv')
    sample = df.sample(5)
    
    print(f'Total Records: {len(df):,}')
    
    for idx, row in sample.iterrows():
        print('\n--- RECORD ---')
        print(f"Company: {row.get('source_company', 'N/A')}")
        print(f"Region: {row.get('region', 'N/A')} ({row.get('lat', 'N/A')}, {row.get('lon', 'N/A')})")
        print(f"Material: {row.get('material', 'N/A')}")
        print(f"Chemical Profile: {row.get('chemical_profile', 'N/A')[:100]}...") # Truncate for display
        print(f"Price: {row.get('price_per_ton_usd', 'N/A')} USD")

except Exception as e:
    print(f'Error: {e}')
