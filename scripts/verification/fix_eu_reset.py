"""
FIX EU LOCATION DATA
====================
Resets 'eprtr' documents to 'pending' so the processor
can re-ingest them and apply the new 'source_location' mapping.
"""
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def fix():
    try:
        conn = psycopg2.connect(
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cur = conn.cursor()
        
        print("🔄 Resetting E-PRTR documents status...")
        cur.execute("UPDATE documents SET status = 'pending' WHERE source = 'eprtr'")
        count = cur.rowcount
        conn.commit()
        
        print(f"✅ Reset {count} documents to 'pending'.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fix()
