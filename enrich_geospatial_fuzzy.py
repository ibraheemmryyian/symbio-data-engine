"""
FUZZY GEOSPATIAL ENRICHMENT
===========================
Maps waste listings to regions using fuzzy matching against our known company database.
"""
import pandas as pd
from difflib import get_close_matches
import company_list_expanded as cl
import json

def run_fuzzy_enrichment():
    print('='*70)
    print('FUZZY GEOSPATIAL ENRICHMENT')
    print('='*70)
    
    # 1. Load Data
    print('Loading waste listings...')
    df = pd.read_csv('exports/waste_listings_with_pricing.csv')
    unique_companies = df['source_company'].unique()
    print(f'Unique Source Companies: {len(unique_companies):,}')
    
    # 2. Build Reference Map (Parse source file text for regions)
    print('Parsing company database for regions...')
    ref_map = {}
    current_region = 'unknown'
    
    with open('company_list_expanded.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    for line in lines:
        line = line.strip()
        # Detect Region Headers
        if line.startswith('#') and 'MENA' in line: current_region = 'mena'
        if line.startswith('#') and 'ASIA' in line: current_region = 'asia_pacific'
        if line.startswith('#') and 'EUROPE' in line: current_region = 'europe'
        if line.startswith('#') and 'NORTH AMERICA' in line: current_region = 'north_america'
        if line.startswith('#') and 'LATIN AMERICA' in line: current_region = 'latin_america'
        if line.startswith('#') and 'AFRICA' in line: current_region = 'africa'
        
        # Extract Company Name
        if line.startswith('("'):
            parts = line.split('"')
            if len(parts) > 1:
                name = parts[1]
                ref_map[name.lower()] = current_region

    print(f'Reference Database: {len(ref_map)} companies mapped to regions')
    
    # 3. Fuzzy Match
    print('\nMatching...')
    
    matches = {}
    hits = 0
    
    # Simplified coordinate centroids for regions
    region_coords = {
        'north_america': {'lat': 40.0, 'lon': -100.0},
        'europe': {'lat': 50.0, 'lon': 10.0},
        'mena': {'lat': 25.0, 'lon': 45.0},
        'asia_pacific': {'lat': 30.0, 'lon': 110.0},
        'latin_america': {'lat': -15.0, 'lon': -60.0},
        'africa': {'lat': 0.0, 'lon': 20.0}
    }
    
    for company in unique_companies:
        c_lower = str(company).lower()
        
        # Direct match
        if c_lower in ref_map:
            matches[company] = ref_map[c_lower]
            hits += 1
            continue
            
        # Fuzzy match (slow for 800k, but we only have unique companies)
        # Using simple substring for speed first
        found = False
        for ref_c, region in ref_map.items():
            if ref_c in c_lower or c_lower in ref_c:
                matches[company] = region
                hits += 1
                found = True
                break
        
        if not found:
            matches[company] = 'unknown'

    print(f'Match Rate: {hits}/{len(unique_companies)} ({hits/len(unique_companies):.1%})')
    
    # 4. Enrich DataFrame
    df['region'] = df['source_company'].map(matches)
    df['lat'] = df['region'].map(lambda r: region_coords.get(r, {}).get('lat', None))
    df['lon'] = df['region'].map(lambda r: region_coords.get(r, {}).get('lon', None))
    
    # 5. Export
    out_path = 'exports/waste_listings_geospatial.csv'
    df.to_csv(out_path, index=False)
    print(f'\nSaved enriched data to: {out_path}')
    print(df[['source_company', 'region', 'lat']].head().to_string())

if __name__ == '__main__':
    run_fuzzy_enrichment()
