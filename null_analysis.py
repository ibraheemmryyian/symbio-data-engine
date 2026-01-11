
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def check_nulls():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    # Key fields for Waste Valorization Report
    fields = [
        'material', 'material_category', 'quantity_tons', 
        'source_company', 'source_location', 'source_country', 'year'
    ]
    
    print("📊 NULL FIELD ANALYSIS\n")
    
    # Total records
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total = cur.fetchone()[0]
    print(f"Total Records: {total:,}\n")
    
    # Check each field
    print("Per-Field Null Counts:")
    for field in fields:
        cur.execute(f"SELECT COUNT(*) FROM waste_listings WHERE {field} IS NULL OR {field}::text = ''")
        null_count = cur.fetchone()[0]
        pct = (null_count / total) * 100
        status = "✅" if pct < 5 else "⚠️" if pct < 50 else "❌"
        print(f"  {status} {field}: {null_count:,} nulls ({pct:.1f}%)")
    
    # Records with at least one null in critical fields
    print("\n--- CRITICAL COMPLETENESS ---")
    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE material IS NULL 
           OR quantity_tons IS NULL 
           OR source_company IS NULL
    """)
    critical_nulls = cur.fetchone()[0]
    print(f"Records missing CRITICAL data (material/qty/company): {critical_nulls:,}")
    
    # Records with at least one null in ANY field
    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE material IS NULL 
           OR material_category IS NULL
           OR quantity_tons IS NULL 
           OR source_company IS NULL
           OR source_location IS NULL
           OR source_country IS NULL
           OR year IS NULL
    """)
    any_nulls = cur.fetchone()[0]
    print(f"Records with ANY null field: {any_nulls:,} ({(any_nulls/total)*100:.1f}%)")
    
    conn.close()

if __name__ == "__main__":
    check_nulls()
