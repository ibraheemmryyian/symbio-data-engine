from store.postgres import execute_query

print("=== Government Documents (detailed) ===")
gov = execute_query("SELECT id, source_url, document_type, status, file_path FROM documents WHERE source = 'government' LIMIT 5")
if gov:
    for g in gov:
        print(f"  ID: {str(g['id'])[:8]}")
        print(f"    Type: {g['document_type']}")
        print(f"    Status: {g['status']}")
        print(f"    File: {g['file_path'][-50:] if g['file_path'] else 'N/A'}")
        print(f"    URL: {g['source_url'][:60]}...")
else:
    print("  No government documents found!")

print("\n=== Pending Documents by Type ===")
pending = execute_query("SELECT document_type, count(*) as cnt FROM documents WHERE status = 'pending' GROUP BY document_type")
for p in pending:
    print(f"  {p['document_type']}: {p['cnt']}")

print("\n=== Total Counts ===")
wl = execute_query("SELECT count(*) as cnt FROM waste_listings")
print(f"  Waste Listings: {wl[0]['cnt']}")
