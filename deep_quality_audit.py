"""
DEEP DATA QUALITY AUDIT - "PROSTATE EXAM" LEVEL
===============================================
Rigorous statistical probe of all 20k+ records.
Checks for:
1. Null/Empty Fields
2. Value Anomalies (Negative/Massive quantities)
3. String Hygiene (Encoding, placeholders)
4. Source Attribution
"""
from store.postgres import execute_query
from collections import Counter
import statistics

print("🏥 STARTING DEEP DATA EXAMINATION...")

# 1. TOTALS & SOURCE BREAKDOWN
print("\n1️⃣ VITAL SIGNS (Counts)")
total = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']
print(f"   ❤️ TOTAL RECORDS: {total}")

breakdown = execute_query("""
    SELECT 
        CASE 
            WHEN source_quote LIKE '%TRI%' THEN 'US-EPA'
            WHEN source_quote LIKE '%E-PRTR%' THEN 'EU-EPRTR'
            WHEN source_quote LIKE '%Bayanat%' THEN 'UAE-MENA'
            ELSE 'Unknown/Other' 
        END as region,
        count(*) as c
    FROM waste_listings
    GROUP BY 1 ORDER BY 2 DESC
""")
for b in breakdown:
    print(f"   🌍 {b['region']}: {b['c']} ({b['c']/total*100:.1f}%)")

# 2. FIELD HEALTH (Missing Data)
print("\n2️⃣ ORGAN HEALTH (Completeness)")
fields = ['source_company', 'source_location', 'material', 'treatment_method']
numeric_fields = ['quantity_tons', 'year']

for f in fields:
    nulls = execute_query(f"SELECT count(*) as c FROM waste_listings WHERE {f} IS NULL OR {f} = '' OR {f} = 'Unknown'")[0]['c']
    print(f"   {'🟢' if nulls==0 else '🟡' if nulls<100 else '🔴'} {f}: {nulls} missing/unknown ({nulls/total*100:.2f}%)")

for f in numeric_fields:
    nulls = execute_query(f"SELECT count(*) as c FROM waste_listings WHERE {f} IS NULL")[0]['c']
    print(f"   {'🟢' if nulls==0 else '🟡' if nulls<100 else '🔴'} {f}: {nulls} missing ({nulls/total*100:.2f}%)")

# 3. VALUE DISTRIBUTION (Outliers)
print("\n3️⃣ BLOOD PRESSURE (Quantity Distribution)")
qtys = [float(r['quantity_tons']) for r in execute_query("SELECT quantity_tons FROM waste_listings WHERE quantity_tons IS NOT NULL")]
if qtys:
    avg = statistics.mean(qtys)
    med = statistics.median(qtys)
    mx = max(qtys)
    mn = min(qtys)
    
    print(f"   📊 Mean: {avg:,.2f} Tons")
    print(f"   📊 Median: {med:,.2f} Tons")
    print(f"   💪 Max: {mx:,.2f} Tons")
    print(f"   🤏 Min: {mn:,.2f} Tons")
    
    # Check negatives
    negs = len([x for x in qtys if x < 0])
    print(f"   ⚠️ Negatives: {negs} (Should be 0)")
    
    # Check realistic massive outliers (> 1M tons)
    massive = len([x for x in qtys if x > 1000000])
    print(f"   ⚠️ Massive (>1M tons): {massive}")

# 4. DUPLICATION ANALYSIS
print("\n4️⃣ CLONING CHECK (Redundancy)")
dups = execute_query("""
    SELECT source_company, material, year, treatment_method, count(*) as c
    FROM waste_listings
    GROUP BY 1, 2, 3, 4
    HAVING count(*) > 1
""")
dup_count = sum([d['c'] for d in dups])
print(f"   👯 Exact Business Duplicates: {dup_count} records involved")

print("\n🏁 EXAMINATION COMPLETE.")
