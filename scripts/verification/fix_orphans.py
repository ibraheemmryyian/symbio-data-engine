
import psycopg2
import uuid
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def fix_orphans():
    print("🚑 ORPHAN RECOVERY PROTOCOL INITIATED...\n")
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()

    # 1. Check Orphan Count
    cur.execute("SELECT COUNT(*) FROM waste_listings w LEFT JOIN documents d ON w.document_id = d.id WHERE d.id IS NULL")
    count = cur.fetchone()[0]
    print(f"Found {count} orphans.")
    
    if count == 0:
        print("No orphans to fix.")
        return

    # 2. Create Recovery Document
    recovery_id = str(uuid.uuid4())
    print(f"Creating Recovery Document: {recovery_id}")
    cur.execute("""
        INSERT INTO documents (id, source, source_url, document_type, status, metadata)
        VALUES (%s, 'system_recovery', 'internal:orphan_recovery', 'recovery', 'completed', '{"reason": "stress_test_fix"}')
    """, (recovery_id,))

    # 3. Adopt Orphans
    print("Adopting orphans...")
    # Logic: UPDATE w SET document_id = recovery_id WHERE document_id NOT IN (SELECT id FROM documents)
    # But checking IN (SELECT) on 860k is slow.
    # Better: UPDATE waste_listings w SET document_id = %s WHERE NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = w.document_id)
    cur.execute("""
        UPDATE waste_listings w
        SET document_id = %s
        WHERE NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = w.document_id)
    """, (recovery_id,))
    
    affected = cur.rowcount
    
    conn.commit()
    print(f"✅ RECOVERED {affected} records.")
    conn.close()

if __name__ == "__main__":
    fix_orphans()
