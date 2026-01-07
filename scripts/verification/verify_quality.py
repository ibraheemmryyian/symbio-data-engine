"""Verify Data Quality - Check if processing is correct"""
from store.postgres import execute_query

print("="*60)
print("   DATA QUALITY VERIFICATION")
print("="*60)

# 1. Total counts
print("\n📊 TOTALS:")
wl = execute_query("SELECT count(*) as cnt FROM waste_listings")
docs = execute_query("SELECT count(*) as cnt FROM documents")
gov_docs = execute_query("SELECT count(*) as cnt FROM documents WHERE source = 'government'")
print(f"   Waste Listings: {wl[0]['cnt']}")
print(f"   Total Documents: {docs[0]['cnt']}")
print(f"   Government (EPA) Docs: {gov_docs[0]['cnt']}")

# 2. Treatment methods breakdown
print("\n🏭 TREATMENT METHODS:")
methods = execute_query("""
    SELECT treatment_method, count(*) as cnt, ROUND(SUM(quantity_tons)::numeric, 2) as total_tons
    FROM waste_listings 
    GROUP BY treatment_method 
    ORDER BY cnt DESC
""")
for m in methods:
    print(f"   {m['treatment_method']}: {m['cnt']} records | {m['total_tons']} metric tons")

# 3. Sample records
print("\n📋 SAMPLE RECORDS (TOP 5 BY QUANTITY):")
samples = execute_query("""
    SELECT material, quantity_tons, source_company, treatment_method, year, source_location
    FROM waste_listings 
    WHERE quantity_tons > 0
    ORDER BY quantity_tons DESC
    LIMIT 5
""")
for s in samples:
    print(f"   {s['material'][:25]:<25} | {float(s['quantity_tons']):>10.2f} MT | {s['treatment_method']:<18} | {s['year']} | {s['source_location']}")

# 4. Year distribution
print("\n📅 YEARS COVERED:")
years = execute_query("""
    SELECT year, count(*) as cnt 
    FROM waste_listings 
    WHERE year IS NOT NULL
    GROUP BY year 
    ORDER BY year
""")
year_range = [y['year'] for y in years]
if year_range:
    print(f"   Range: {min(year_range)} - {max(year_range)}")
    print(f"   Years: {len(year_range)}")

print("\n" + "="*60)
if wl[0]['cnt'] > 0:
    print("   ✅ DATA LOOKS CORRECT!")
else:
    print("   ⚠️ NO DATA YET - CHECK PIPELINE")
print("="*60)
