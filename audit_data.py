"""DEEP DATA QUALITY AUDIT - Is this too good to be true?"""
from store.postgres import execute_query

print("="*70)
print("   🔍 DEEP DATA QUALITY AUDIT")
print("="*70)

# 1. SAMPLE ACTUAL RECORDS
print("\n1️⃣ SAMPLE RECORDS (Show me the data):")
samples = execute_query("""
    SELECT material, quantity_tons, source_company, treatment_method, year, source_location
    FROM waste_listings 
    WHERE quantity_tons > 0
    ORDER BY RANDOM()
    LIMIT 8
""")
for s in samples:
    mat = s['material'][:35] if s['material'] else 'N/A'
    qty = float(s['quantity_tons']) if s['quantity_tons'] else 0
    comp = s['source_company'][:25] if s['source_company'] else 'N/A'
    meth = s['treatment_method'] or 'N/A'
    print(f"   {mat:<35} | {qty:>10.2f} MT | {meth:<18} | {s['year']}")

# 2. CHECK FOR DUPLICATES
print("\n2️⃣ DUPLICATE CHECK:")
total = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']
unique = execute_query("""
    SELECT count(*) as c FROM (
        SELECT DISTINCT material, source_company, year, treatment_method 
        FROM waste_listings
    ) x
""")[0]['c']
dup_rate = (total - unique) / total * 100 if total > 0 else 0
print(f"   Total: {total} | Unique: {unique} | Duplicate rate: {dup_rate:.1f}%")

# 3. VALUE DISTRIBUTION (Are quantities realistic?)
print("\n3️⃣ QUANTITY DISTRIBUTION:")
dist = execute_query("""
    SELECT 
        CASE 
            WHEN quantity_tons < 1 THEN '< 1 MT'
            WHEN quantity_tons < 10 THEN '1-10 MT'
            WHEN quantity_tons < 100 THEN '10-100 MT'
            WHEN quantity_tons < 1000 THEN '100-1000 MT'
            ELSE '> 1000 MT'
        END as range,
        count(*) as cnt
    FROM waste_listings
    WHERE quantity_tons > 0
    GROUP BY 1
    ORDER BY 2 DESC
""")
for d in dist:
    print(f"   {d['range']}: {d['cnt']}")

# 4. TOP CHEMICALS (Are these real EPA chemicals?)
print("\n4️⃣ TOP CHEMICALS (verify these exist in EPA TRI):")
chems = execute_query("""
    SELECT material, count(*) as cnt 
    FROM waste_listings 
    GROUP BY material 
    ORDER BY cnt DESC 
    LIMIT 10
""")
for c in chems:
    print(f"   {c['material'][:45]}: {c['cnt']} records")

# 5. YEAR DISTRIBUTION
print("\n5️⃣ YEAR DISTRIBUTION:")
years = execute_query("""
    SELECT year, count(*) as cnt 
    FROM waste_listings 
    WHERE year IS NOT NULL
    GROUP BY year 
    ORDER BY year
""")
for y in years:
    print(f"   {y['year']}: {y['cnt']} records")

# 6. NULL/EMPTY CHECK
print("\n6️⃣ DATA COMPLETENESS:")
null_mat = execute_query("SELECT count(*) as c FROM waste_listings WHERE material IS NULL OR material = ''")[0]['c']
null_qty = execute_query("SELECT count(*) as c FROM waste_listings WHERE quantity_tons IS NULL OR quantity_tons = 0")[0]['c']
null_meth = execute_query("SELECT count(*) as c FROM waste_listings WHERE treatment_method IS NULL")[0]['c']
print(f"   Missing material: {null_mat}")
print(f"   Zero/null quantity: {null_qty}")
print(f"   Missing treatment: {null_meth}")

print("\n" + "="*70)
print("   VERDICT:")
if dup_rate > 50:
    print("   ⚠️ HIGH DUPLICATE RATE - May be over-counting")
elif null_mat > total * 0.1:
    print("   ⚠️ MISSING DATA - Quality issues")
else:
    print("   ✅ DATA LOOKS LEGITIMATE")
print("="*70)
