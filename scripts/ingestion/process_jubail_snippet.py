
import json
import psycopg2
import uuid
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

# The User-Provided Data
DATA = [{"year": "2017", "variable": "Burning", "value": 26237.0},{"year": "2010", "variable": "Landfill", "value": 61400.0},{"year": "2013", "variable": "Landfill", "value": 81300.0},{"year": "2013", "variable": "Total", "value": 192700.0},{"year": "2014", "variable": "Total", "value": 200000.0},{"year": "2018", "variable": "Landfill", "value": 124676.0},{"year": "2010", "variable": "Recycling", "value": 45000.0},{"year": "2017", "variable": "Landfill", "value": 106781.0},{"year": "2010", "variable": "Burning", "value": 37800.0},{"year": "2018", "variable": "Burning", "value": 37860.0},{"year": "2012", "variable": "Landfill", "value": 77500.0},{"year": "2011", "variable": "Burning", "value": 38000.0},{"year": "2015", "variable": "Burning", "value": 42000.0},{"year": "2016", "variable": "Burning", "value": 17120.0},{"year": "2015", "variable": "Landfill", "value": 89582.0},{"year": "2016", "variable": "Landfill", "value": 102618.0},{"year": "2014", "variable": "Recycling", "value": 75000.0},{"year": "2015", "variable": "Recycling", "value": 108440.0},{"year": "2012", "variable": "Total", "value": 184500.0},{"year": "2016", "variable": "Total", "value": 232910.0},{"year": "2017", "variable": "Total", "value": 270897.0},{"year": "2012", "variable": "Burning", "value": 47000.0},{"year": "2013", "variable": "Burning", "value": 41400.0},{"year": "2011", "variable": "Landfill", "value": 62200.0},{"year": "2014", "variable": "Landfill", "value": 80000.0},{"year": "2012", "variable": "Recycling", "value": 60000.0},{"year": "2013", "variable": "Recycling", "value": 70000.0},{"year": "2018", "variable": "Recycling", "value": 195907.0},{"year": "2010", "variable": "Total", "value": 144200.0},{"year": "2011", "variable": "Total", "value": 147200.0},{"year": "2015", "variable": "Total", "value": 240022.0},{"year": "2014", "variable": "Burning", "value": 45000.0},{"year": "2011", "variable": "Recycling", "value": 47000.0},{"year": "2016", "variable": "Recycling", "value": 113172.0},{"year": "2017", "variable": "Recycling", "value": 137879.0},{"year": "2018", "variable": "Total", "value": 358443.0}]

def process():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("\nProcessing Jubail JSON Snippet...")
    
    # Create Document ID
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "jubail_disposal_metadata"))
    
    # 1. Register Document
    try:
        cur.execute("""
            INSERT INTO documents (id, source, source_url, document_type, status)
            VALUES (%s, 'mena_manual', 'jubail_json_snippet', 'json', 'completed')
            ON CONFLICT (id) DO NOTHING
        """, (doc_id,))
    except Exception as e:
        pass
        
    inserted = 0
    skipped = 0
    
    for row in DATA:
        year = int(row['year'])
        # 'variable' is the treatment method (Burning, Landfill, Recycling)
        method = row['variable']
        quantity = float(row['value'])
        
        # We skip 'Total' because it duplicates the sum
        if method.lower() == 'total':
            skipped += 1
            continue
            
        # Insert
        # Since we don't have a specific material, we say "Industrial Waste (Aggregate)"
        # We infer the treatment method
        
        try:
            cur.execute("""
                INSERT INTO waste_listings 
                (document_id, material, quantity_tons, source_company, source_location, source_country, year, material_category, treatment_method)
                VALUES (%s, 'Industrial Waste (Aggregate)', %s, 'Jubail Industrial City', 'Jubail, Saudi Arabia', 'SAU', %s, 'MENA Industrial', %s)
                ON CONFLICT (document_id, material, source_company, year, quantity_tons) DO NOTHING
            """, (doc_id, quantity, year, method))
            inserted += 1
        except Exception as e:
            print(f"Error: {e}")
            
    print(f"✅ Ingested {inserted} records for Jubail (Saudi Arabia). Skipped {skipped} totals.")

if __name__ == "__main__":
    process()
