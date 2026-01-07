from store.postgres import execute_query

print("🔎 DATABASE SOURCE CHECK:")
rows = execute_query("""
    SELECT d.source, count(*) as c 
    FROM waste_listings w 
    JOIN documents d ON w.document_id = d.id 
    GROUP BY 1
""")
for r in rows:
    print(f"   📂 {r['source']}: {r['c']} records")
