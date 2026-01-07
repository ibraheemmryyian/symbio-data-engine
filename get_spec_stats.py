
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def get_stats():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    # 1. Total Records
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total = cur.fetchone()[0]
    
    # 2. Full Entries (Rich Data)
    # Defined as having Company, Material, Quantity, and Location
    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE source_company IS NOT NULL 
          AND material IS NOT NULL 
          AND quantity_tons > 0 
          AND source_location IS NOT NULL
    """)
    full = cur.fetchone()[0]
    
    # 3. Country Breakdown
    # Extract country from location string or assumption
    cur.execute("""
        SELECT 
            COALESCE(source_country, split_part(source_location, ', ', 2)) as country, 
            COUNT(*) 
        FROM waste_listings 
        GROUP BY country 
        ORDER BY COUNT(*) DESC 
        LIMIT 20
    """)
    countries = cur.fetchall()
    
    print(f"TOTAL_RECORDS: {total:,}")
    print(f"FULL_ENTRIES: {full:,}")
    print("\nTOP COUNTRIES:")
    for c, count in countries:
        c_name = c if c and c.strip() else "Unknown (likely US/EU)"
        print(f"  - {c_name}: {count:,}")

if __name__ == "__main__":
    get_stats()
