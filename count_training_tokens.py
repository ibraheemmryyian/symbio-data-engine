"""
TOKEN COUNTER
=============
Estimates total training tokens in the Symbio Data Engine exports.
Uses standard approximation: 1 token ~= 4 characters (English text).
"""
import pandas as pd
import os

def run_token_count():
    print('='*70)
    print('TRAINING DATA VOLUME (TOKEN ESTIMATE)')
    print('='*70)
    
    files = [
        'exports/waste_listings_granular.csv',
        # Add other potential training files if they exist and are distinct
        # 'exports/waste_listings_geospatial.csv' is a subset/precursor, so don't double count
    ]
    
    total_tokens = 0
    total_rows = 0
    
    for f_path in files:
        if not os.path.exists(f_path):
            print(f'Skipping {f_path} (Not found)')
            continue
            
        try:
            # Read file
            df = pd.read_csv(f_path)
            rows = len(df)
            
            # Estimate tokens: Convert entire DF to string representation (JSON-like) per row is best for training
            # But specific columns matter most. Let's strictly count the content.
            # Training Prompt Format usually: "Material: {m}, Region: {r} -> Price: {p}"
            # We'll estimate raw text text chars.
            
            text_blob = df.to_string() # Rough but captures all data
            char_count = len(text_blob)
            
            # Refined method: Sum meaningful columns
            # Text = Source + Material + Region + Profile
            cols = ['source_company', 'material', 'region', 'chemical_profile', 'price_per_ton_usd']
            if 'chemical_profile' not in df.columns: cols.remove('chemical_profile')
            
            df_str = df[cols].astype(str).agg(' '.join, axis=1)
            char_count = df_str.str.len().sum()
            
            tokens = int(char_count / 4)
            total_tokens += tokens
            total_rows += rows
            
            print(f'File: {f_path}')
            print(f'  Rows: {rows:,}')
            print(f'  Chars: {char_count:,}')
            print(f'  Est. Tokens: {tokens:,}')
            print('-'*30)
            
        except Exception as e:
            print(f'Error reading {f_path}: {e}')

    print(f'\nTOTAL TRAINING VOLUME: {total_tokens:,} TOKENS')
    print(f'TOTAL RECORDS: {total_rows:,}')
    print('='*70)

if __name__ == '__main__':
    run_token_count()
