import pandas as pd
import json
from pathlib import Path

print('='*70)
print('DATA QUALITY INSPECTION')
print('='*70)

# 1. WASTE LISTINGS
print('\n1. WASTE LISTINGS (First 5 rows)')
csv_path = 'data/processed/waste_listings_with_pricing.csv'
if Path(csv_path).exists():
    try:
        df = pd.read_csv(csv_path, nrows=5)
        print(df.to_string(index=False))
        print('\nColumns used:', df.columns.tolist())
        
        # Check for nulls/unknowns in a larger sample
        df_large = pd.read_csv(csv_path, nrows=1000)
        unknown_loc = len(df_large[df_large['region'] == 'unknown'])
        unknown_mat = len(df_large[df_large['material_category'] == 'unknown'])
        print(f'\nQuality Check (Sample 1000):')
        print(f'  Unknown Regions: {unknown_loc/1000:.1%}')
        print(f'  Unknown Materials: {unknown_mat/1000:.1%}')
    except Exception as e:
        print(f'Error reading CSV: {e}')
else:
    print('Entries CSV not found')

# 2. PRICING MODEL
print('\n2. PRICING MODEL DEPTH')
json_path = 'exports/industry_pricing.json'
if Path(json_path).exists():
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        print(f"Parent Categories: {len(data.get('parent_categories', {}))}")
        print(f"Sub-industries: {len(data.get('sub_industries', {}))}")
        print(f"Materials: {len(data.get('materials', {}))}")
        
        # Check specific material content
        mats = data.get('materials', {})
        if mats:
            # Print first material details
            name = list(mats.keys())[0]
            print(f'\nSample Material ({name}):')
            print(json.dumps(mats[name], indent=2))
            
    except Exception as e:
        print(f'Error reading JSON: {e}')
else:
    print('Pricing JSON not found')
