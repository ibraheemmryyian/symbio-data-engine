"""
PRE-UPLOAD DATA QUALITY AUDIT
=============================
Checks for: Duplicates, BS data, null values, suspicious patterns
"""
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def audit():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    print("=" * 70)
    print("PRE-UPLOAD DATA QUALITY AUDIT")
    print("=" * 70)
    
    # 1. TOTAL RECORDS
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total = cur.fetchone()[0]
    print(f"\n1. TOTAL RECORDS: {total:,}")
    
    # 2. EXACT DUPLICATES (same material + company + year + quantity)
    print("\n2. DUPLICATE CHECK:")
    cur.execute("""
        SELECT material, source_company, year, quantity_tons, COUNT(*) as count
        FROM waste_listings
        WHERE material IS NOT NULL AND source_company IS NOT NULL
        GROUP BY material, source_company, year, quantity_tons
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """)
    dupes = cur.fetchall()
    if dupes:
        print(f"   FOUND {len(dupes)} duplicate patterns:")
        for d in dupes[:5]:
            print(f"   - {d[0][:20]}... | {d[1][:20]}... | {d[2]} | {d[3]} tons | x{d[4]}")
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT material, source_company, year, quantity_tons
                FROM waste_listings
                WHERE material IS NOT NULL AND source_company IS NOT NULL
                GROUP BY material, source_company, year, quantity_tons
                HAVING COUNT(*) > 1
            ) AS dupes
        """)
        total_dupes = cur.fetchone()[0]
        print(f"   Total duplicate patterns: {total_dupes}")
    else:
        print("   NO EXACT DUPLICATES FOUND")
    
    # 3. NULL/EMPTY VALUES
    print("\n3. NULL/EMPTY VALUE CHECK:")
    fields = [
        ("material", "material IS NULL OR material = ''"),
        ("quantity_tons", "quantity_tons IS NULL"),
        ("source_company", "source_company IS NULL OR source_company = ''"),
        ("source_location", "source_location IS NULL OR source_location = ''"),
        ("year", "year IS NULL"),
    ]
    for field, condition in fields:
        cur.execute(f"SELECT COUNT(*) FROM waste_listings WHERE {condition}")
        count = cur.fetchone()[0]
        pct = 100 * count / total if total > 0 else 0
        status = "OK" if pct < 5 else "WARN" if pct < 20 else "BAD"
        print(f"   {field}: {count:,} null ({pct:.1f}%) [{status}]")
    
    # 4. SUSPICIOUS VALUES (BS Detection)
    print("\n4. SUSPICIOUS DATA CHECK:")
    
    # Nonsense materials
    cur.execute("""
        SELECT material, COUNT(*) FROM waste_listings 
        WHERE material ~ '^[0-9]+$' OR LENGTH(material) < 2
        GROUP BY material LIMIT 5
    """)
    nonsense = cur.fetchall()
    print(f"   Numeric-only or too-short materials: {len(nonsense)}")
    
    # Extreme quantities
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE quantity_tons > 10000000")
    extreme = cur.fetchone()[0]
    print(f"   Extreme quantities (>10M tons): {extreme}")
    
    # Zero/negative quantities
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE quantity_tons <= 0")
    zero = cur.fetchone()[0]
    print(f"   Zero/negative quantities: {zero}")
    
    # Future years
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE year > 2025")
    future = cur.fetchone()[0]
    print(f"   Future years (>2025): {future}")
    
    # Very old years
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE year < 1980")
    ancient = cur.fetchone()[0]
    print(f"   Ancient years (<1980): {ancient}")
    
    # 5. TOP MATERIALS (sanity check - should look like real chemicals)
    print("\n5. TOP 10 MATERIALS (Sanity Check):")
    cur.execute("""
        SELECT material, COUNT(*) as cnt 
        FROM waste_listings 
        WHERE material IS NOT NULL
        GROUP BY material 
        ORDER BY cnt DESC 
        LIMIT 10
    """)
    for row in cur.fetchall():
        print(f"   {row[0][:40]}: {row[1]:,}")
    
    # 6. YEAR DISTRIBUTION
    print("\n6. YEAR DISTRIBUTION:")
    cur.execute("""
        SELECT year, COUNT(*) FROM waste_listings 
        WHERE year IS NOT NULL 
        GROUP BY year ORDER BY year DESC LIMIT 10
    """)
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]:,}")
    
    # 7. RECOMMENDATION
    print("\n" + "=" * 70)
    print("UPLOAD RECOMMENDATION:")
    
    issues = []
    if dupes and len(dupes) > 100:
        issues.append("Many duplicates")
    if zero > total * 0.1:
        issues.append("Many zero quantities")
    if future > 0:
        issues.append("Future year records")
    
    if issues:
        print(f"   CAUTION: {', '.join(issues)}")
        print("   Consider running deduplication before upload.")
    else:
        print("   DATA LOOKS CLEAN - OK TO UPLOAD")
    print("=" * 70)

if __name__ == "__main__":
    audit()
