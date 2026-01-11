"""
INDUSTRY ENRICHMENT (FUZZY)
===========================
Parses company_list_expanded.py to extract Industry Sectors from comments
and maps them to the waste listings.
Required for logical consistency checks (e.g., "Food factory shouldn't produce mining tailings").
"""
import pandas as pd

def run_industry_enrichment():
    print('='*70)
    print('INDUSTRY SECTOR ENRICHMENT')
    print('='*70)
    
    # 1. Parse Company -> Industry Map
    # The file structure has headers like:
    # # =========================================
    # # MENA - 80+ Companies
    # # =========================================
    # # UAE - Core (this is sub-region, mixed with sector implied by earlier structure?)
    # ...
    # Actually, looking at the file viewed earlier:
    # # Food/Ag
    # ("ADM", ...)
    # 
    # I need to catch lines starting with "# " that look like sectors.
    
    ref_map = {}
    current_sector = 'Unknown'
    
    # We'll define a list of known sector keywords to latch onto
    SECTOR_KEYWORDS = [
        'Oil & Gas', 'Chemicals', 'Metals & Mining', 'Auto/Tech', 
        'Paper & Packaging', 'Construction', 'Consumer/Retail', 
        'Waste Management', 'Food/Ag', 'Healthcare & Pharma',
        'Energy', 'Telecom', 'Finance', 'Aviation'
    ]
    
    with open('company_list_expanded.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    for line in lines:
        line = line.strip()
        
        # Detect Sector Header
        if line.startswith('#'):
            # Check if this comment is a known sector header
            clean_line = line.replace('#', '').strip()
            # Simple substring check
            for kw in SECTOR_KEYWORDS:
                if kw.lower() in clean_line.lower():
                    current_sector = kw
                    break
        
        # Extract Company Name
        if line.startswith('("'):
            parts = line.split('"')
            if len(parts) > 1:
                name = parts[1]
                ref_map[name.lower()] = current_sector

    print(f'Mapped {len(ref_map)} companies to Industries.')
    
    # 2. Apply to Dataset
    df = pd.read_csv('exports/waste_listings_granular.csv')
    
    def get_industry(company):
        c_lower = str(company).lower()
        # Exact match
        if c_lower in ref_map:
            return ref_map[c_lower]
        # Fuzzy match
        for k, v in ref_map.items():
            if k in c_lower or c_lower in k:
                return v
        return 'Unknown'

    print('Applying industry mapping...')
    df['industry'] = df['source_company'].apply(get_industry)
    
    known_count = len(df[df['industry'] != 'Unknown'])
    print(f'Industry Match Rate: {known_count:,} / {len(df):,} ({known_count/len(df):.1%})')
    
    out_path = 'exports/waste_listings_granular_industry.csv'
    df.to_csv(out_path, index=False)
    print(f'Saved to: {out_path}')
    print(df[['source_company', 'industry']].head(10).to_string())

if __name__ == '__main__':
    run_industry_enrichment()
