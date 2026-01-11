"""
ACCURACY TRUTH SAMPLER
======================
Extracts specific examples of mappings to allow for human verification of "Ground Truth" accuracy.
"""
import pandas as pd
import json

def run_truth_check():
    print('='*70)
    print('ACCURACY TRUTH CHECK: HUMAN INSPECTION REQUIRED')
    print('='*70)
    
    try:
        df = pd.read_csv('exports/waste_listings_granular.csv')
        
        # 1. GEOSPATIAL ACCURACY
        print('\n[1] GEOSPATIAL MAPPING SAMPLES (Source Company -> Enriched Region)')
        print('Check: Does the company actually operate primarily in this region?')
        print('-'*70)
        
        # Get diverse regions
        regions = df['region'].unique()
        for r in regions:
            if r == 'unknown': continue
            # Get 2 examples per region
            samples = df[df['region'] == r].sample(min(2, len(df[df['region'] == r])))
            for _, row in samples.iterrows():
                print(f"  {row['source_company']}  ==>  {r.upper()}")

        # 2. CHEMICAL ACCURACY
        print('\n[2] GRANULARITY MAPPING SAMPLES (Material -> Chemical Profile)')
        print('Check: Is the chemical composition scientifically valid for the waste stream?')
        print('-'*70)
        
        # Get diverse materials
        df_chem = df[df['chemical_profile'].notna()]
        materials = df_chem['material'].unique()[:5] # First 5 unique types
        
        # Also try to find specific complex ones
        complex_terms = ['sludge', 'ash', 'catalyst', 'mud']
        for term in complex_terms:
            matches = df_chem[df_chem['material'].astype(str).str.contains(term, case=False, na=False)]
            if not matches.empty:
                row = matches.iloc[0]
                prof = json.loads(row['chemical_profile'])
                components = ", ".join([c['component'] for c in prof['composition']])
                print(f"  '{row['material']}'")
                print(f"      ==> Contains: {components}")
                print(f"      ==> Hazards: {prof.get('hazards', [])}")
                print('')

    except Exception as e:
        print(f"Verification Error: {e}")

if __name__ == '__main__':
    run_truth_check()
