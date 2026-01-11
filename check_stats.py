import pandas as pd
print('DATA QUALITY METRICS')
try:
    df = pd.read_csv('exports/waste_listings_with_pricing.csv')
    total = len(df)
    unknown_mat = len(df[df['material'] == 'unknown'])
    missing_price = len(df[df['estimated_value_usd'].isna()])
    
    print(f'Total Rows: {total:,}')
    print(f'Missing Regions: 100% (Column "region" not found in export)')
    print(f'Unknown Materials: {unknown_mat:,} ({unknown_mat/total:.1%})')
    print(f'Missing/Null Prices: {missing_price:,} ({missing_price/total:.1%})')
except Exception as e:
    print(f'Error: {e}')
