from store.postgres import execute_query

print("🔎 CHECKING NEWEST DATA (EU/E-PRTR)")
recs = execute_query("""
    SELECT material, quantity_tons, source_company, source_location, year, treatment_method
    FROM waste_listings
    ORDER BY created_at DESC
    LIMIT 3
""")

for r in recs:
    print(f"\n🏭 {r['source_company']}")
    print(f"   📍 {r['source_location']}")
    print(f"   🧪 {r['material']} -> {r['quantity_tons']} Tons")
    print(f"   ⚙️ {r['treatment_method']}")
