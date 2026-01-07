import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def audit_eu():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    # Get count of eprtr records
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE document_id IN (SELECT id FROM documents WHERE source='eprtr')")
    count = cur.fetchone()[0]
    print(f"Total EU Records: {count}")

    if count == 0:
        print("⚠️ No EU records found yet. Processor might still be running or failed.")
        return

    # Check for blanks
    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE document_id IN (SELECT id FROM documents WHERE source='eprtr')
        AND (material IS NULL OR material = '')
    """)
    null_mat = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE document_id IN (SELECT id FROM documents WHERE source='eprtr')
        AND (quantity_tons IS NULL)
    """)
    null_qty = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM waste_listings 
        WHERE document_id IN (SELECT id FROM documents WHERE source='eprtr')
        AND (source_location IS NULL OR source_location = '')
    """)
    null_loc = cur.fetchone()[0]

    print(f"Null Materials: {null_mat}")
    print(f"Null Quantities: {null_qty}")
    print(f"Null Locations: {null_loc}")

    # Check for valid examples
    print("\nSample Data:")
    cur.execute("""
        SELECT material, quantity_tons, treatment_method, source_location 
        FROM waste_listings 
        WHERE document_id IN (SELECT id FROM documents WHERE source='eprtr')
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f" - {row}")

if __name__ == "__main__":
    audit_eu()
