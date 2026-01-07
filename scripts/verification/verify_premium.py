from store.postgres import execute_query
from rich.console import Console
from rich.table import Table

console = Console()

print("💎 VERIFYING PREMIUM DATA QUALITY...")

# 1. EU LOCATIONS (The Fix)
print("\n🇪🇺 EU LOCATION AUDIT:")
loc_counts = execute_query("""
    SELECT count(*) as c, 
           count(CASE WHEN source_location IS NOT NULL AND source_location != 'Unknown' THEN 1 END) as valid,
           count(CASE WHEN source_location = 'Unknown' THEN 1 END) as unknown
    FROM waste_listings w 
    JOIN documents d ON w.document_id = d.id 
    WHERE d.source = 'eprtr'
""")
if loc_counts:
    c, v, u = loc_counts[0]['c'], loc_counts[0]['valid'], loc_counts[0]['unknown']
    print(f"   Total EU Records: {c}")
    print(f"   ✅ Valid Locations: {v} ({(v/c*100 if c>0 else 0):.1f}%)")
    print(f"   ❌ Unknown Locations: {u}")

# 2. AI TRAINING FEATURES (The "Premium" Check)
print("\n🧠 AI TRAINING FEATURES MATCH:")

# Treatment Methods (Vital for Symbiosis Matching)
print("   🧪 Treatment Methods (Top 5):")
methods = execute_query("""
    SELECT treatment_method, count(*) as c 
    FROM waste_listings 
    WHERE treatment_method IS NOT NULL 
    GROUP BY 1 ORDER BY 2 DESC LIMIT 5
""")
for m in methods:
    print(f"      - {m['treatment_method']}: {m['c']}")

# 3. SAMPLE DATA (The Proof)
print("\n👀 SAMPLE PREMIUM RECORD:")
sample = execute_query("""
    SELECT source_company, source_location, material, quantity_tons, treatment_method, year
    FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'eprtr' AND source_location != 'Unknown'
    ORDER BY random() LIMIT 1
""")
if sample:
    r = sample[0]
    print(f"   🏭 Company:  {r['source_company']}")
    print(f"   📍 Location: {r['source_location']}")
    print(f"   📦 Material: {r['material']}")
    print(f"   ⚖️ Quantity: {r['quantity_tons']} Tons")
    print(f"   ♻️ Method:   {r['treatment_method']}")
    print(f"   📅 Year:     {r['year']}")
else:
    print("   ⚠️ No verified EU sample found yet (Backfill in progress?)")
