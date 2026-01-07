from store.postgres import execute_query, update_document_status

# Get government CSV document IDs
docs = execute_query("SELECT id FROM documents WHERE source = 'government' AND document_type = 'csv'")
print(f"Found {len(docs)} government CSV documents")

for d in docs:
    doc_id = d['id']
    print(f"Resetting document {doc_id} to pending...")
    update_document_status(doc_id, "pending")
    
print("Done! All government CSV documents reset to pending.")
