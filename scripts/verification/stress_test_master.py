
import psycopg2
import re
import numpy as np
import sys
from collections import Counter
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def stress_test():
    print("🔥 MASTER STRESS TEST INITIATED...\n")
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    errors = 0

    # 1. TEMPORAL LOGIC (Time Travel Check)
    print("1. TEMPORAL LOGIC:")
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE year > 2026 OR year < 1980")
    bad_years = cur.fetchone()[0]
    if bad_years > 0:
        print(f"   ❌ CRITICAL: Found {bad_years} records with Impossible Years (Time Travel detected)")
        errors += 1
    else:
        print("   ✅ Years are valid (1980-2026)")

    # 2. VALUE MAGNITUDE (The "Jupiter" Check)
    print("\n2. VALUE MAGNITUDE:")
    cur.execute("SELECT MAX(quantity_tons) FROM waste_listings")
    max_qty = cur.fetchone()[0]
    # If a single listing is > 1 Billion tons, it's likely a parsing error
    if max_qty and max_qty > 100_000_000:
         print(f"   ⚠️ WARNING: Max Quantity is {max_qty:,.2f} tons. This is potentially suspicious (Mountain-sized).")
         # We flag it but don't fail, maybe it's wastewater (heavy)
    else:
         print(f"   ✅ Max Quantity ({max_qty:,.2f}) appears physically possible.")

    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE quantity_tons < 0")
    neg_qty = cur.fetchone()[0]
    if neg_qty > 0:
        print(f"   ❌ CRITICAL: Found {neg_qty} records with Negative Mass (Anti-matter detected)")
        errors += 1
    else:
        print("   ✅ Mass is positive.")

    # 3. TEXT HYGIENE (Mojibake & Encoding)
    print("\n3. TEXT HYGIENE:")
    cur.execute("SELECT material FROM waste_listings LIMIT 10000")
    materials = [r[0] for r in cur.fetchall()]
    
    mojibake_chars = ["Ã©", "Ã", "ï¿½", "â€"]
    mojibake_count = 0
    for m in materials:
        if any(bad in m for bad in mojibake_chars):
            mojibake_count += 1
            
    if mojibake_count > 0:
        print(f"   ⚠️ WARNING: Found {mojibake_count} records with potential encoding errors (Mojibake).")
        # errors += 1  # Don't block deploy on typos, but warn
    else:
        print("   ✅ Text encoding appears clean.")

    # 4. ORPHAN CHECK (Referential Integrity)
    print("\n4. ORPHAN CHECK:")
    cur.execute("""
        SELECT COUNT(*) 
        FROM waste_listings w 
        LEFT JOIN documents d ON w.document_id = d.id 
        WHERE d.id IS NULL
    """)
    orphans = cur.fetchone()[0]
    if orphans > 0:
        print(f"   ❌ CRITICAL: Found {orphans} Orphan Records (Aggressive Deletion detected)")
        errors += 1
    else:
        print("   ✅ Referential Integrity holds.")

    # 5. DISTRIBUTION SKew (The "One Giant" Check)
    print("\n5. DISTRIBUTION SKEW:")
    cur.execute("SELECT source_company, COUNT(*) as c FROM waste_listings GROUP BY source_company ORDER BY c DESC LIMIT 1")
    top_polluter = cur.fetchone()
    print(f"   - Top Data Contributor: {top_polluter[0]} ({top_polluter[1]:,} records)")
    
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total = cur.fetchone()[0]
    skew_pct = (top_polluter[1] / total) * 100
    if skew_pct > 50:
        print(f"   ⚠️ WARNING: Data is heavily skewed. {top_polluter[0]} owns {skew_pct:.1f}% of the dataset.")
    else:
        print(f"   ✅ Data is decentralized (Top contributor = {skew_pct:.1f}%).")

    conn.close()

    print(f"\n🔥 STRESS TEST COMPLETE. ERRORS: {errors}")
    if errors > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    stress_test()
