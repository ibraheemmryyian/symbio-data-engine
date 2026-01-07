from store.postgres import execute_query

print("🔎 VERIFYING EU DATA QUALITY (Location Fix)...")

# Count ALL EU records
total = execute_query("SELECT count(*) as c FROM waste_listings WHERE source_quote LIKE '%E-PRTR%'")[0]['c']
print(f"🇪🇺 Total EU Records: {total}")

# Count Unknown Location
unknown = execute_query("SELECT count(*) as c FROM waste_listings WHERE source_quote LIKE '%E-PRTR%' AND source_location = 'Unknown'")[0]['c']
print(f"❓ Unknown Locations: {unknown}")

if total > 0:
    print(f"✅ Quality Score: {(1 - unknown/total)*100:.1f}%")
else:
    print("⚠️ No EU records found yet.")

# Sample a good one
if total > 0:
    sample = execute_query("SELECT source_location FROM waste_listings WHERE source_quote LIKE '%E-PRTR%' AND source_location != 'Unknown' LIMIT 1")
    if sample:
        print(f"examplar: {sample[0]['source_location']}")
