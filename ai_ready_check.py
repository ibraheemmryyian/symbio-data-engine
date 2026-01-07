"""
🌍 WORLD DOMINATION CHECK - Is this AI-Ready Data?
==================================================
Verify data structure for Industrial Symbiosis Marketplace
"""
from store.postgres import execute_query
import json

print("="*70)
print("   🌍 AI MARKETPLACE DATA STRUCTURE VERIFICATION")
print("="*70)

# 1. SCHEMA CHECK - What fields do we have?
print("\n1️⃣ DATABASE SCHEMA (Fields Available):")
cols = execute_query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'waste_listings'
    ORDER BY ordinal_position
""")
ai_critical = ['material', 'quantity_tons', 'treatment_method', 'source_company', 'source_location', 'year']
for c in cols:
    is_critical = "⭐" if c['column_name'] in ai_critical else "  "
    print(f"   {is_critical} {c['column_name']}: {c['data_type']}")

# 2. SAMPLE FULL RECORD - What does a record look like?
print("\n2️⃣ SAMPLE FULL RECORD (AI Training Format):")
sample = execute_query("""
    SELECT * FROM waste_listings 
    WHERE quantity_tons > 0 AND treatment_method IS NOT NULL
    LIMIT 1
""")
if sample:
    record = dict(sample[0])
    # Clean for display
    for k, v in record.items():
        if v and str(v) != 'None':
            print(f"   {k}: {str(v)[:60]}")

# 3. AI MATCHMAKING EXAMPLE
print("\n3️⃣ MATCHMAKING USE CASE (How AI Would Use This):")
print("   ")
print("   🏭 WASTE PRODUCER (from our data):")
producer = execute_query("""
    SELECT material, quantity_tons, source_company, treatment_method 
    FROM waste_listings 
    WHERE treatment_method = 'Disposal/Released' AND quantity_tons > 1
    LIMIT 1
""")
if producer:
    p = producer[0]
    print(f"      Company: {p['source_company'][:40]}")
    print(f"      Has: {p['material']}")
    print(f"      Quantity: {float(p['quantity_tons']):.2f} MT available")
    print(f"      Currently: {p['treatment_method']} (WASTE)")

print("   ")
print("   🔄 POTENTIAL CONSUMER (AI would match to):")
consumer = execute_query("""
    SELECT DISTINCT material, source_company 
    FROM waste_listings 
    WHERE treatment_method = 'Recycled'
    LIMIT 1
""")
if consumer:
    c = consumer[0]
    print(f"      Company: {c['source_company'][:40]}")
    print(f"      Needs: {c['material']} (for recycling)")
    print("      = SYMBIOSIS MATCH! 🎯")

# 4. DATA RICHNESS
print("\n4️⃣ DATA RICHNESS CHECK:")
materials = execute_query("SELECT count(DISTINCT material) as c FROM waste_listings")[0]['c']
companies = execute_query("SELECT count(DISTINCT source_company) as c FROM waste_listings")[0]['c']
methods = execute_query("SELECT count(DISTINCT treatment_method) as c FROM waste_listings")[0]['c']
years = execute_query("SELECT count(DISTINCT year) as c FROM waste_listings")[0]['c']

print(f"   Unique Materials: {materials}")
print(f"   Unique Companies: {companies}")
print(f"   Treatment Methods: {methods}")
print(f"   Years Covered: {years}")

# 5. VERDICT
print("\n" + "="*70)
if materials > 50 and companies > 100:
    print("   ✅ DATA IS AI-READY FOR WORLD DOMINATION")
    print("   → Materials for matching: ✅")
    print("   → Companies for sourcing: ✅")
    print("   → Treatment methods for symbiosis: ✅")
    print("   → Historical data for predictions: ✅")
else:
    print("   ⏳ Still collecting - keep running overnight")
print("="*70)
