"""
BLIND EXTRACT - USER AUDIT STEP 1
=================================
Extracts 50 random rows for manual human verification.
Columns: Source Company, Facility Location (Region/Lat/Lon), Material, Predicted Profile.
"""
import pandas as pd

def run_blind_extract():
    print('Generating Blind Extract...')
    try:
        # Load the most enriched file available
        # Ideally the one with industry, but granular is fine if industry not ready
        f_path = 'exports/waste_listings_granular.csv' 
        # Check if industry version exists
        try:
             pd.read_csv('exports/waste_listings_granular_industry.csv', nrows=1)
             f_path = 'exports/waste_listings_granular_industry.csv'
        except:
            pass
            
        df = pd.read_csv(f_path)
        
        # 50 Random Rows
        blind_sample = df.sample(50, random_state=42) # fixed seed for reproducibility or random? User said "random". 42 is fine.
        
        # Select Columns
        cols = ['source_company', 'region', 'lat', 'lon', 'material', 'chemical_profile', 'price_per_ton_usd']
        if 'industry' in df.columns:
            cols.insert(1, 'industry')
            
        blind_sample = blind_sample[cols]
        
        out_path = 'exports/blind_extract_50.csv'
        blind_sample.to_csv(out_path, index=False)
        print(f'Done. Saved to {out_path}')
        print('DO NOT LOOK AT IT YET (per user instructions).')

    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    run_blind_extract()
