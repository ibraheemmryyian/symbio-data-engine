from store.postgres import execute_query

print("🔄 Resetting E-PRTR documents for re-processing...")
execute_query("UPDATE documents SET status = 'pending' WHERE source = 'eprtr'", fetch=False)
print("✅ Done. Processor will re-ingest with correct location.")
