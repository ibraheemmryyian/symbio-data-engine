"""
BACKFILL SCRIPT: Material Categories & Country Codes
=====================================================
TOP PRIORITY: Run this to fill in the 849,552 null fields.

This script:
1. Infers `material_category` from the `material` string using keyword matching
2. Extracts `source_country` from `source_location` using country/city mappings

Run with: python backfill_categories.py
Estimated time: 10-15 minutes for 850k records
"""

import psycopg2
import re
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

# Material Category Mappings (keyword -> category)
MATERIAL_CATEGORIES = {
    'metals': ['zinc', 'lead', 'copper', 'nickel', 'aluminum', 'aluminium', 'iron', 'steel', 
               'chromium', 'cadmium', 'mercury', 'arsenic', 'metal', 'alloy'],
    'chemicals': ['acid', 'chlor', 'sulfur', 'sulphur', 'nitro', 'ammonia', 'hydroxide', 
                  'oxide', 'compound', 'solvent', 'benzene', 'toluene', 'xylene', 'phenol'],
    'organics': ['organic', 'carbon', 'methane', 'ethane', 'food', 'bio', 'sludge'],
    'plastics': ['plastic', 'polymer', 'polyethylene', 'polypropylene', 'pvc', 'styrene'],
    'hazardous': ['hazardous', 'toxic', 'radioactive', 'pcb', 'dioxin', 'furan', 'asbestos'],
    'water_emissions': ['water', 'aqueous', 'wastewater', 'effluent'],
    'air_emissions': ['air', 'emission', 'particulate', 'dust', 'pm10', 'pm2.5', 'nox', 'sox'],
    'energy': ['oil', 'fuel', 'petroleum', 'coal', 'gas']
}

# Country Detection (location keyword -> ISO 3166-1 alpha-3)
COUNTRY_MAPPINGS = {
    # Major EU countries (E-PRTR sources)
    'germany': 'DEU', 'deutschland': 'DEU', 'german': 'DEU',
    'france': 'FRA', 'french': 'FRA',
    'spain': 'ESP', 'spanish': 'ESP', 'españa': 'ESP',
    'italy': 'ITA', 'italian': 'ITA', 'italia': 'ITA',
    'poland': 'POL', 'polish': 'POL', 'polska': 'POL',
    'netherlands': 'NLD', 'dutch': 'NLD', 'holland': 'NLD',
    'belgium': 'BEL', 'belgian': 'BEL',
    'sweden': 'SWE', 'swedish': 'SWE',
    'austria': 'AUT', 'austrian': 'AUT', 'österreich': 'AUT',
    'czech': 'CZE', 'czechia': 'CZE',
    'romania': 'ROU', 'romanian': 'ROU',
    'portugal': 'PRT', 'portuguese': 'PRT',
    'greece': 'GRC', 'greek': 'GRC',
    'hungary': 'HUN', 'hungarian': 'HUN',
    'finland': 'FIN', 'finnish': 'FIN',
    'denmark': 'DNK', 'danish': 'DNK',
    'ireland': 'IRL', 'irish': 'IRL',
    'slovakia': 'SVK', 'slovak': 'SVK',
    'bulgaria': 'BGR', 'bulgarian': 'BGR',
    'croatia': 'HRV', 'croatian': 'HRV',
    'slovenia': 'SVN', 'slovenian': 'SVN',
    'lithuania': 'LTU', 'lithuanian': 'LTU',
    'latvia': 'LVA', 'latvian': 'LVA',
    'estonia': 'EST', 'estonian': 'EST',
    'cyprus': 'CYP',
    'luxembourg': 'LUX',
    'malta': 'MLT',
    'united kingdom': 'GBR', 'uk': 'GBR', 'britain': 'GBR', 'england': 'GBR', 'scotland': 'GBR', 'wales': 'GBR',
    # US
    'usa': 'USA', 'united states': 'USA', 'america': 'USA',
    # MENA
    'saudi': 'SAU', 'arabia': 'SAU', 'riyadh': 'SAU', 'jeddah': 'SAU', 'jubail': 'SAU',
    'uae': 'ARE', 'emirates': 'ARE', 'dubai': 'ARE', 'abu dhabi': 'ARE',
    'qatar': 'QAT', 'doha': 'QAT',
    'kuwait': 'KWT',
    'oman': 'OMN',
    'bahrain': 'BHR',
    'egypt': 'EGY', 'cairo': 'EGY',
    'jordan': 'JOR', 'amman': 'JOR',
}

def categorize_material(material_name):
    """Infer category from material name using keyword matching."""
    if not material_name:
        return 'unknown'
    
    material_lower = material_name.lower()
    
    for category, keywords in MATERIAL_CATEGORIES.items():
        for keyword in keywords:
            if keyword in material_lower:
                return category
    
    return 'industrial_waste'  # Default category

def extract_country(location):
    """Extract country code from location string."""
    if not location:
        return None
    
    location_lower = location.lower()
    
    for keyword, country_code in COUNTRY_MAPPINGS.items():
        if keyword in location_lower:
            return country_code
    
    return 'EUR'  # Default to Europe since most data is E-PRTR

def backfill():
    print("🔧 BACKFILL SCRIPT: Starting...")
    print("   This will populate material_category and source_country fields.\n")
    
    conn = psycopg2.connect(
        dbname=POSTGRES_DB, 
        user=POSTGRES_USER, 
        password=POSTGRES_PASSWORD, 
        host=POSTGRES_HOST, 
        port=POSTGRES_PORT
    )
    cur = conn.cursor()
    
    # Get total count
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material_category IS NULL")
    total_nulls = cur.fetchone()[0]
    print(f"   Records to update: {total_nulls:,}\n")
    
    # Process in batches
    BATCH_SIZE = 10000
    processed = 0
    
    while True:
        # Fetch batch of records needing update
        cur.execute("""
            SELECT id, material, source_location 
            FROM waste_listings 
            WHERE material_category IS NULL
            LIMIT %s
        """, (BATCH_SIZE,))
        
        rows = cur.fetchall()
        if not rows:
            break
        
        # Update each record
        for row_id, material, location in rows:
            category = categorize_material(material)
            country = extract_country(location)
            
            cur.execute("""
                UPDATE waste_listings 
                SET material_category = %s, source_country = %s 
                WHERE id = %s
            """, (category, country, row_id))
        
        conn.commit()
        processed += len(rows)
        print(f"   ✅ Processed: {processed:,} / {total_nulls:,} ({(processed/total_nulls)*100:.1f}%)")
    
    print(f"\n🎉 BACKFILL COMPLETE! Updated {processed:,} records.")
    conn.close()

if __name__ == "__main__":
    backfill()
