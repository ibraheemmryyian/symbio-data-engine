from store.postgres import execute_query

# Total records vs unique materials
print("="*60)
print("WASTE LISTINGS BREAKDOWN")
print("="*60)

r = execute_query("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT material) as unique_materials,
        COUNT(DISTINCT source_company) as unique_companies,
        COUNT(DISTINCT source_country) as unique_countries
    FROM waste_listings
""")
print(f"Total records: {r[0]['total_records']:,}")
print(f"Unique materials: {r[0]['unique_materials']}")
print(f"Unique companies: {r[0]['unique_companies']:,}")
print(f"Countries: {r[0]['unique_countries']}")

# Sample records
print("\n" + "="*60)
print("SAMPLE RECORDS (what a record looks like)")
print("="*60)
samples = execute_query("""
    SELECT material, quantity_tons, source_company, source_country, year 
    FROM waste_listings 
    LIMIT 5
""")
for s in samples:
    print(f"  {s['material'][:40]:<40} | {s['quantity_tons']:>10,.0f} tons | {s['source_company'][:25]:<25} | {s['source_country']}")

# Most common materials
print("\n" + "="*60)
print("TOP 10 MATERIALS BY RECORD COUNT")
print("="*60)
top = execute_query("""
    SELECT material, COUNT(*) as records, SUM(quantity_tons) as total_tons
    FROM waste_listings
    GROUP BY material
    ORDER BY records DESC
    LIMIT 10
""")
for t in top:
    print(f"  {t['material'][:35]:<35} | {t['records']:>6,} records | {t['total_tons']:>12,.0f} tons")
