
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def scrub():
    print("🧹 FINAL SCRUB INITIATED...\n")
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()

    # 1. Exact Duplicate Check (should be 0 due to schema)
    sql_dupes = """
        SELECT COUNT(*) FROM (
            SELECT document_id, material, source_company, year, quantity_tons, COUNT(*)
            FROM waste_listings
            GROUP BY document_id, material, source_company, year, quantity_tons
            HAVING COUNT(*) > 1
        ) sub
    """
    cur.execute(sql_dupes)
    dupes = cur.fetchone()[0]
    
    # 2. Uncategorized Material Check
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material_category = 'Unknown'")
    unknowns = cur.fetchone()[0]
    
    # 3. Null Quantity Check
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE quantity_tons IS NULL")
    nulls = cur.fetchone()[0]

    print(f"RESULTS:")
    print(f" - Exact Duplicates: {dupes}")
    print(f" - Uncategorized Materials: {unknowns}")
    print(f" - Null Quantities: {nulls}")

    if dupes == 0 and nulls == 0:
        print("\n✅ DATA IS CLEAN.")
    else:
        print("\n⚠️ DIRTY DATA DETECTED.")

    conn.close()

if __name__ == "__main__":
    scrub()
