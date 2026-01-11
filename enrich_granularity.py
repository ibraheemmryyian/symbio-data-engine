"""
GRANULARITY ENRICHMENT
======================
Enhances waste listings with chemical composition data by mapping vague descriptions
to our MSDS Knowledge Base.
"""
import pandas as pd
import json
import re
from msds_knowledge_base import MSDS_MAP

def run_granularity_upgrade():
    print('='*70)
    print('GRANULARITY ENRICHMENT PIPELINE')
    print('='*70)
    
    # 1. Load Data (Geospatial if available, else standard)
    input_file = 'exports/waste_listings_geospatial.csv'
    try:
        df = pd.read_csv(input_file)
        print(f'Loaded {len(df):,} records from {input_file}')
    except:
        print(f'Geospatial file not found. Falling back to fuzzy enriched.')
        try:
             df = pd.read_csv('exports/waste_listings_with_pricing.csv') # Fallback
             print(f'Loaded fallback: {len(df):,} records')
        except:
            print('CRITICAL: No input data found.')
            return

    # 2. Enrich Function
    def get_chemical_profile(description):
        desc_lower = str(description).lower()
        
        # Check against MSDS Map keys
        for key in MSDS_MAP:
            if key in desc_lower:
                return json.dumps(MSDS_MAP[key])
                
        return None

    # 3. Apply Mapping
    print('Mapping chemical compositions...')
    # Use 'material' or 'wasteClassification' field
    if 'material' in df.columns:
        target_col = 'material'
    elif 'wasteClassification' in df.columns:
        target_col = 'wasteClassification' # E-PRTR default
    else:
        print('Warning: No suitable description column found. Using first string col.')
        target_col = [c for c in df.columns if df[c].dtype == 'object'][0]
        
    print(f'Using column: {target_col}')
    
    df['chemical_profile'] = df[target_col].apply(get_chemical_profile)
    
    # 4. Stats
    enriched_count = df['chemical_profile'].notna().sum()
    print(f'Enriched: {enriched_count:,} records ({enriched_count/len(df):.1%})')
    
    # 5. Export
    out_path = 'exports/waste_listings_granular.csv'
    df.to_csv(out_path, index=False)
    print(f'\nSaved granular data to: {out_path}')
    
    # Sample
    sample = df[df['chemical_profile'].notna()].head(1)
    if not sample.empty:
        print('\nSample Enrichment:')
        print(f"Material: {sample.iloc[0][target_col]}")
        print(f"Profile: {sample.iloc[0]['chemical_profile']}")

if __name__ == '__main__':
    run_granularity_upgrade()
