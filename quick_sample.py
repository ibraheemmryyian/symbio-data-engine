"""Quick sample of real records for verification."""
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
cur = conn.cursor()

print("="*60)
print("SAMPLE REAL RECORDS FROM DATABASE")
print("="*60)

# Get 3 random government records
cur.execute("""
    SELECT w.material, w.quantity_tons, w.source_company, w.source_location, w.year, w.source_quote
    FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'government'
    ORDER BY RANDOM()
    LIMIT 3
""")
print("\n--- GOVERNMENT SOURCE (US EPA TRI) ---")
for row in cur.fetchall():
    print(f"Material: {row[0]}")
    print(f"Quantity: {row[1]} tons")
    print(f"Company: {row[2]}")
    print(f"Location: {row[3]}")
    print(f"Year: {row[4]}")
    print(f"Citation: {row[5][:120] if row[5] else 'N/A'}...")
    print()

# Get eprtr records
cur.execute("""
    SELECT w.material, w.quantity_tons, w.source_company, w.source_location, w.year, w.source_quote
    FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'eprtr'
    LIMIT 2
""")
print("\n--- EPRTR SOURCE (EU Industrial Reporting) ---")
for row in cur.fetchall():
    print(f"Material: {row[0]}")
    print(f"Quantity: {row[1]} tons")
    print(f"Company: {row[2]}")
    print(f"Location: {row[3]}")
    print(f"Year: {row[4]}")
    print(f"Citation: {row[5][:120] if row[5] else 'N/A'}...")
    print()

# Total counts
cur.execute("SELECT COUNT(*) FROM waste_listings")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT source_company) FROM waste_listings")
companies = cur.fetchone()[0]
cur.execute("SELECT MIN(year), MAX(year) FROM waste_listings WHERE year IS NOT NULL")
years = cur.fetchone()

print("="*60)
print(f"TOTALS: {total:,} records | {companies:,} unique companies | Years: {years[0]}-{years[1]}")
print("="*60)
