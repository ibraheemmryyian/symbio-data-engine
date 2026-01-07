"""
Export waste listings to CSV for verification.
"""
import csv
import psycopg2
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def export_csv():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()
    
    # Get all waste listings with document source info
    cur.execute("""
        SELECT 
            w.id,
            w.material,
            w.quantity_tons,
            w.treatment_method,
            w.source_company,
            w.source_location,
            w.year,
            w.source_quote,
            w.extraction_confidence,
            d.source as document_source,
            d.source_url
        FROM waste_listings w
        LEFT JOIN documents d ON w.document_id = d.id
        ORDER BY w.id
    """)
    
    rows = cur.fetchall()
    
    # Export to CSV
    output_path = Path("exports/waste_listings_full_export.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "material", "quantity_tons", "treatment_method",
            "source_company", "source_location", "year", "source_quote",
            "extraction_confidence", "document_source", "source_url"
        ])
        writer.writerows(rows)
    
    print(f"Exported {len(rows):,} records to: {output_path.absolute()}")
    
    # Print summary stats
    print(f"\nSample Records (first 5):")
    for row in rows[:5]:
        print(f"  {row[1][:30]}... | {row[2]} tons | {row[4][:20]}... | {row[5]} | {row[6]}")
    
    # Print source breakdown
    cur.execute("""
        SELECT d.source, COUNT(*) 
        FROM waste_listings w
        LEFT JOIN documents d ON w.document_id = d.id
        GROUP BY d.source
        ORDER BY COUNT(*) DESC
    """)
    print(f"\nRecords by Source:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

if __name__ == "__main__":
    export_csv()
