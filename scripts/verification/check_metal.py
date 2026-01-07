
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def check():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    # Metal Query
    sql = "SELECT COUNT(*) FROM waste_listings WHERE material ILIKE '%metal%' OR material ILIKE '%zinc%' OR material ILIKE '%copper%' OR material ILIKE '%aluminum%' OR material ILIKE '%lead%'"
    cur.execute(sql)
    count = cur.fetchone()[0]
    print(f"METALLURGY_COUNT: {count}")
    
    conn.close()

if __name__ == "__main__":
    check()
