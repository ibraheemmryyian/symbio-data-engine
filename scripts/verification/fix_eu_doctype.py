from store.postgres import execute_query

print("🔧 Fixing document_type for E-PRTR documents...")
execute_query("UPDATE documents SET document_type='csv', status='pending' WHERE source='eprtr'", fetch=False)
print("✅ Done! E-PRTR documents now have document_type='csv' and status='pending'")
