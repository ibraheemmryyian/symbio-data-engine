from store.postgres import execute_query
pending = execute_query("SELECT source, document_type, count(*) as c FROM documents WHERE status = 'pending' GROUP BY source, document_type")
print("QUEUE:")
for p in pending:
    print(f"  {p['source']} ({p['document_type']}): {p['c']}")
