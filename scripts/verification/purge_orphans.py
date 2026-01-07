
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def purge_orphans():
    print("🗑️ ORPHAN PURGE PROTOCOL INITIATED...\n")
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()

    # 1. Count before
    cur.execute("SELECT COUNT(*) FROM waste_listings w LEFT JOIN documents d ON w.document_id = d.id WHERE d.id IS NULL")
    initial_count = cur.fetchone()[0]
    print(f"Found {initial_count} broken records (orphans).")

    # 2. DELETE
    print("EXECUTE ORDER 66: Deleting broken records...")
    cur.execute("DELETE FROM waste_listings w WHERE NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = w.document_id)")
    deleted = cur.rowcount
    conn.commit()

    print(f"✅ PURGED {deleted} records. Integrity Restored.")
    conn.close()

if __name__ == "__main__":
    purge_orphans()
