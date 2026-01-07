"""MINIMAL AUDIT - Direct output"""
from store.postgres import execute_query

total = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']
print(f"TOTAL: {total}")

# Sample 5 records
print("\nSAMPLE RECORDS:")
samples = execute_query("SELECT material, quantity_tons, treatment_method, year FROM waste_listings WHERE quantity_tons > 0 LIMIT 5")
for s in samples:
    print(f"  {s['material'][:40]} | {float(s['quantity_tons']):.2f} MT | {s['treatment_method']} | {s['year']}")

# Top chemicals
print("\nTOP 5 CHEMICALS:")
chems = execute_query("SELECT material, count(*) as c FROM waste_listings GROUP BY material ORDER BY c DESC LIMIT 5")
for c in chems:
    print(f"  {c['material']}: {c['c']}")

# Years
print("\nYEARS:")
years = execute_query("SELECT DISTINCT year FROM waste_listings ORDER BY year")
print(f"  {[y['year'] for y in years]}")

# Duplicates
unique = execute_query("SELECT count(DISTINCT (material, source_company, year)) as c FROM waste_listings")[0]['c']
print(f"\nDUPLICATE CHECK: {total} total, {unique} unique = {(total-unique)/total*100:.1f}% dups")
