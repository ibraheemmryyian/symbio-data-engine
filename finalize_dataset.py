"""
DATASET FINALIZER
=================
Consolidates the most enriched temporary file into a "Gold Master" release
for downstream agents (SymbioFlows).
Source: exports/waste_listings_granular_industry.csv
Target: exports/symbio_data_engine_v1.csv
"""
import pandas as pd
import shutil
import os

print('FINALIZING DATASET...')

obs_path = 'exports/waste_listings_granular_industry.csv'
final_path = 'exports/symbio_data_engine_v1.csv'

if os.path.exists(obs_path):
    # 1. Load to Verify
    df = pd.read_csv(obs_path)
    print(f'Source Loaded: {len(df):,} records')
    
    # 2. Check Key Columns
    req_cols = ['source_company', 'region', 'lat', 'lon', 'industry', 'chemical_profile', 'price_per_ton_usd']
    missing = [c for c in req_cols if c not in df.columns]
    
    if missing:
        print(f'CRITICAL: Missing columns {missing}. Aborting.')
    else:
        # Reorder for cleanliness
        # Put key metadata first, then data
        cols = req_cols + [c for c in df.columns if c not in req_cols]
        df = df[cols]
        
        # 3. Save Master
        df.to_csv(final_path, index=False)
        print(f'SUCCESS: Created {final_path}')
        print('Status: READY FOR HANDOFF')
else:
    print(f'Error: Source file {obs_path} not found. Did the industry Enrichment run finish?')
