"""
CLEAN DATA EXPORT
=================
Exports only verified, clean records for production upload.
Filters: No nulls, no zero quantities, no duplicates.
"""
import csv
import psycopg2
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def export_clean():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    # Export ONLY clean records
    cur.execute("""
        SELECT DISTINCT ON (material, source_company, year, quantity_tons)
            w.id,
            w.material,
            w.quantity_tons,
            w.treatment_method,
            w.source_company,
            w.source_location,
            w.year,
            w.source_quote,
            d.source as data_source
        FROM waste_listings w
        LEFT JOIN documents d ON w.document_id = d.id
        WHERE 
            w.material IS NOT NULL 
            AND w.material != ''
            AND w.quantity_tons IS NOT NULL 
            AND w.quantity_tons > 0
            AND w.year >= 1970 
            AND w.year <= 2025
        ORDER BY material, source_company, year, quantity_tons, w.id
    """)
    
    rows = cur.fetchall()
    
    # Export to CSV
    output_path = Path("exports/CLEAN_waste_listings_for_upload.csv")
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "material", "quantity_tons", "treatment_method",
            "source_company", "source_location", "year", "source_quote", "data_source"
        ])
        writer.writerows(rows)
    
    print(f"CLEAN EXPORT COMPLETE")
    print(f"=====================")
    print(f"Clean records: {len(rows):,}")
    print(f"Output: {output_path.absolute()}")
    
    # Stats
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total_original = cur.fetchone()[0]
    
    print(f"\nFiltering removed: {total_original - len(rows):,} records")
    print(f"  - Null/empty materials")
    print(f"  - Zero/null quantities")  
    print(f"  - Invalid years")
    print(f"  - Duplicates")

if __name__ == "__main__":
    export_clean()
