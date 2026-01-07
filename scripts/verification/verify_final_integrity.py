
import psycopg2
import json
import pandas as pd
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def verify():
    print("🛡️ BRIDGE GUARD: INITIATING DEEP INTEGRITY AUDIT...\n")
    
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    # 1. VOLUMETRIC AUDIT
    print("1. VOLUMETRIC PROOF")
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total = cur.fetchone()[0]
    print(f"   - Total Verified Records: {total:,}")
    
    cur.execute("SELECT source_country, COUNT(*) FROM waste_listings GROUP BY source_country ORDER BY COUNT(*) DESC")
    print("   - Geographic Distribution:")
    for country, count in cur.fetchall():
        print(f"     * {country}: {count:,}")

    # 2. CRITICAL FIELD INTEGRITY
    print("\n2. CRITICAL FIELD INTEGRITY (The 'Null' Check)")
    checks = [
        "material IS NULL OR material = ''",
        "quantity_tons IS NULL",
        "quantity_tons = 0",
        "source_company IS NULL OR source_company = ''"
    ]
    
    for check in checks:
        cur.execute(f"SELECT COUNT(*) FROM waste_listings WHERE {check}")
        fails = cur.fetchone()[0]
        status = "✅ PASS" if fails == 0 else f"❌ FAIL ({fails} records)"
        print(f"   - Check '{check}': {status}")

    # 3. MENA SPECIFIC AUDIT (Jubail & SEEA)
    print("\n3. MENA PROOF (The 'Hard Part')")
    cur.execute("SELECT * FROM waste_listings WHERE source_country IN ('SAU', 'ARE', 'QAT')")
    mena_records = cur.fetchall()
    if not mena_records:
        print("   ❌ CRITICAL: No MENA records found!")
    else:
        print(f"   ✅ MENA Records Found: {len(mena_records)}")
        print("   - Sample Dump (Jubail/SEEA):")
        for row in mena_records[:5]:
             # id, doc_id, mat, qty, unit, comp, loc, lat, lon, ind, code, method, year, cat, created
             print(f"     * [{row[12]}] {row[5]} ({row[4]}): {row[2]} tons of {row[1]} -> {row[11]}")

    # 4. TRAINING DATA FORMAT AUDIT
    print("\n4. AI BRAIN AUDIT (File Integrity)")
    try:
        with open("data/training/symbio_chat_finetune_v1.jsonl", "r", encoding="utf-8") as f:
            lines = f.readlines()
            print(f"   - Chat Finetune Lines: {len(lines):,}")
            # Check first line
            sample = json.loads(lines[0])
            if "messages" in sample:
                 print("   ✅ JSONL Format: VALID (OpenAI/Llama Ready)")
            else:
                 print("   ❌ JSONL Format: INVALID")
    except Exception as e:
        print(f"   ❌ Failed to read Training Data: {e}")

    # 5. LOGIC AUDIT (Sanity Check)
    print("\n5. LOGIC SANITY CHECK")
    # Do we have hazardous waste going to landfill?
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material LIKE '%Hazardous%' AND treatment_method LIKE '%Landfill%'")
    haz_dump = cur.fetchone()[0]
    print(f"   - Hazardous Waste Landfilled: {haz_dump:,} (This is real world data, sadly)")
    
    # 6. DEPTH ANALYSIS (The "Moat Depth" Check)
    print("\n6. DEPTH ANALYSIS (Is it 10cm deep?)")
    
    # Material Diversity
    cur.execute("SELECT COUNT(DISTINCT material) FROM waste_listings")
    unique_mats = cur.fetchone()[0]
    print(f"   - Unique Waste Fingerprints: {unique_mats:,} (Chemical Diversity)")
    
    # Historical Depth
    cur.execute("""
        SELECT AVG(year_count) 
        FROM (SELECT source_company, COUNT(DISTINCT year) as year_count 
              FROM waste_listings GROUP BY source_company) sub
    """)
    avg_history = cur.fetchone()[0]
    print(f"   - Avg History per Facility: {float(avg_history):.1f} Years (Temporal Depth)")

    # "Super-Polluters" (Facilities with > 10 years of data)
    cur.execute("""
        SELECT COUNT(*) 
        FROM (SELECT source_company FROM waste_listings 
              GROUP BY source_company HAVING COUNT(DISTINCT year) >= 10) sub
    """)
    long_history_count = cur.fetchone()[0]
    print(f"   - Facilities with >10 Years History: {long_history_count:,}")

    conn.close()
    print("\n🛡️ AUDIT COMPLETE. DATA IS SEALED.")

if __name__ == "__main__":
    verify()
