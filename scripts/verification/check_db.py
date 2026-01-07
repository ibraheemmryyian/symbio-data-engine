from store.postgres import execute_query

print("=== Final Database Status ===\n")

# Waste listings count
wl = execute_query("SELECT count(*) as cnt FROM waste_listings")
print(f"📦 Total Waste Listings: {wl[0]['cnt']}")

# By treatment method
print("\n=== By Treatment Method ===")
methods = execute_query("""
    SELECT treatment_method, count(*) as cnt, SUM(quantity_tons) as total_tons
    FROM waste_listings 
    GROUP BY treatment_method 
    ORDER BY cnt DESC
""")
for m in methods:
    print(f"  {m['treatment_method']}: {m['cnt']} records, {m['total_tons']:.2f} tons")

# Sample
print("\n=== Sample Waste Listings ===")
samples = execute_query("""
    SELECT material, quantity_tons, source_company, treatment_method, year 
    FROM waste_listings 
    ORDER BY quantity_tons DESC
    LIMIT 5
""")
for s in samples:
    print(f"  {s['material'][:25]:<25} | {s['quantity_tons']:>10.2f} tons | {s['treatment_method']:<18} | {s['year']}")
