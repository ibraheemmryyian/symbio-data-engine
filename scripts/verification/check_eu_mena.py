"""Check EU and MENA status."""
from store.postgres import execute_query

print("="*60)
print("EU & MENA PIPELINE STATUS")
print("="*60)

# Document status by source
print("\n1. DOCUMENT STATUS BY SOURCE:")
docs = execute_query("""
    SELECT source, status, COUNT(*) as cnt
    FROM documents 
    GROUP BY source, status 
    ORDER BY source, status
""")
for d in docs:
    print(f"   {d['source']}: {d['status']} = {d['cnt']}")

# EU specific
print("\n2. EU (EPRTR) DETAILS:")
eu_docs = execute_query("""
    SELECT id, file_path, status, document_type
    FROM documents 
    WHERE source = 'eprtr'
    LIMIT 5
""")
for d in eu_docs:
    fp = d['file_path'][-50:] if d['file_path'] else 'N/A'
    print(f"   [{d['status']}] {d['document_type']} - ...{fp}")

# EU records in waste_listings
eu_records = execute_query("""
    SELECT COUNT(*) as cnt FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'eprtr'
""")
print(f"   -> EU waste_listings: {eu_records[0]['cnt']}")

# MENA specific
print("\n3. MENA DETAILS:")
mena_docs = execute_query("""
    SELECT COUNT(*) as cnt FROM documents WHERE source = 'mena'
""")
print(f"   Documents: {mena_docs[0]['cnt']}")

mena_records = execute_query("""
    SELECT COUNT(*) as cnt FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'mena'
""")
print(f"   waste_listings: {mena_records[0]['cnt']}")

print("\n" + "="*60)
