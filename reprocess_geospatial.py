"""
GEOSPATIAL ENRICHMENT PIPELINE (SMART SCHEMA)
=============================================
Enriches waste data with location (Lat/Lon/Country) by dynamically mapping E-PRTR schema.
"""
import zipfile
import pandas as pd
import io
from pathlib import Path

def get_best_col(cols, keywords):
    """Find best column match based on keywords."""
    cols_lower = [c.lower() for c in cols]
    for k in keywords:
        for i, c in enumerate(cols_lower):
            if k in c:
                return cols[i]
    return None

def run_enrichment():
    print('='*70)
    print('GEOSPATIAL ENRICHMENT PIPELINE (SMART)')
    print('='*70)
    
    zip_path = r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip'
    
    try:
        with zipfile.ZipFile(zip_path) as z:
            names = z.namelist()
            
            # Fuzzy find files
            fac_path = next((n for n in names if 'facilities' in n.lower() and n.lower().endswith('.csv')), None)
            waste_path = next((n for n in names if 'wastetransfers' in n.lower() and n.lower().endswith('.csv')), None)
            
            if not fac_path or not waste_path:
                print('ERROR: Missing files.')
                return

            print(f'Facility File: {fac_path}')
            print(f'Waste File: {waste_path}')
            
            # 1. Analyze Facility Schema
            with z.open(fac_path) as f:
                fac_cols = pd.read_csv(f, nrows=1).columns.tolist()
            print(f'Facility Columns: {fac_cols}')
            
            # Map Facility Columns
            cid_col = get_best_col(fac_cols, ['facilityreportid', 'facilityid', 'nationalpd'])
            name_col = get_best_col(fac_cols, ['facilityname', 'parentcompanyname'])
            lat_col = get_best_col(fac_cols, ['latitude', 'lat'])
            lon_col = get_best_col(fac_cols, ['longitude', 'long'])
            coord_col = get_best_col(fac_cols, ['coordinates'])
            country_col = get_best_col(fac_cols, ['countrycode', 'countryname'])
            city_col = get_best_col(fac_cols, ['city'])
            
            print(f'Mapped: ID={cid_col}, Lat={lat_col}, Lon={lon_col}, Country={country_col}')
            
            if not cid_col:
                print('CRITICAL: No Facility ID column found.')
                return

            # Load Facilities
            use_cols = [c for c in [cid_col, name_col, lat_col, lon_col, coord_col, country_col, city_col] if c]
            with z.open(fac_path) as f:
                df_fac = pd.read_csv(f, usecols=use_cols, encoding='utf-8', on_bad_lines='skip')
                
            # Rename for consistency
            rename_map = {cid_col: 'FacilityID'}
            if country_col: rename_map[country_col] = 'CountryCode'
            if city_col: rename_map[city_col] = 'City'
            if lat_col: rename_map[lat_col] = 'Lat'
            if lon_col: rename_map[lon_col] = 'Long'
            
            df_fac.rename(columns=rename_map, inplace=True)
            
            # 2. Analyze Waste Schema
            with z.open(waste_path) as f:
                waste_cols = pd.read_csv(f, nrows=1).columns.tolist()
            print(f'Waste Columns: {waste_cols}')
            
            # Map Waste Columns
            wid_col = get_best_col(waste_cols, ['facilityreportid', 'facilityid']) # Join key
            qty_col = get_best_col(waste_cols, ['quantitytotal', 'quantity', 'amount'])
            unit_col = get_best_col(waste_cols, ['unitcode', 'unit'])
            class_col = get_best_col(waste_cols, ['wasteclassificationcode', 'classification'])
            treat_col = get_best_col(waste_cols, ['wastetreatmentcode', 'treatment'])
            
            print(f'Mapped: ID={wid_col}, Qty={qty_col}, Class={class_col}')
            
            if not wid_col or not qty_col:
                print('CRITICAL: Missing key waste columns.')
                return

            # Load Waste Transfers
            print('\nLoading chunks...')
            use_waste = [c for c in [wid_col, qty_col, unit_col, class_col, treat_col] if c]
            chunks = []
            with z.open(waste_path) as f:
                for chunk in pd.read_csv(f, chunksize=100000, usecols=use_waste, encoding='utf-8', on_bad_lines='skip'):
                    chunks.append(chunk)
            
            df_waste = pd.concat(chunks)
            df_waste.rename(columns={wid_col: 'FacilityID', qty_col: 'QuantityTotal'}, inplace=True)
            
            # 3. Merge
            print('Merging...')
            df_enriched = pd.merge(df_waste, df_fac, on='FacilityID', how='inner')
            print(f'Merged Records: {len(df_enriched):,}')
            
            # 4. Save
            df_enriched.to_csv('exports/geospatial_waste.csv', index=False)
            print('Saved to exports/geospatial_waste.csv')

    except Exception as e:
        print(f'ERROR: {e}')
        raise

if __name__ == '__main__':
    run_enrichment()
