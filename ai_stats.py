from store.postgres import execute_query

# Quick stats
materials = execute_query("SELECT count(DISTINCT material) as c FROM waste_listings")[0]['c']
companies = execute_query("SELECT count(DISTINCT source_company) as c FROM waste_listings")[0]['c']
total = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']

# Sample match
producer = execute_query("SELECT material, source_company FROM waste_listings WHERE treatment_method = 'Disposal/Released' LIMIT 1")
consumer = execute_query("SELECT material, source_company FROM waste_listings WHERE treatment_method = 'Recycled' LIMIT 1")

print(f"MATERIALS: {materials}")
print(f"COMPANIES: {companies}")
print(f"TOTAL RECORDS: {total}")
print(f"\nMATCH EXAMPLE:")
if producer:
    print(f"  PRODUCER: {producer[0]['source_company'][:30]} → {producer[0]['material'][:30]}")
if consumer:
    print(f"  CONSUMER: {consumer[0]['source_company'][:30]} ← {consumer[0]['material'][:30]}")
print(f"\nAI-READY: {'YES ✅' if materials > 50 else 'BUILDING...'}")
