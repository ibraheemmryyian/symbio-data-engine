from store.postgres import execute_query

print("🔎 FINAL QUALITY CHECK (EU Locations)...")

# Count Missing Location for E-PRTR
missing = execute_query("""
    SELECT count(*) as c 
    FROM waste_listings w 
    JOIN documents d ON w.document_id = d.id 
    WHERE d.source = 'eprtr' 
      AND (w.source_location IS NULL OR w.source_location = 'Unknown')
""")[0]['c']

total_eprtr = execute_query("SELECT count(*) as c FROM documents WHERE source='eprtr' AND status='processed'")[0]['c']
total_recs = execute_query("SELECT count(*) as c FROM waste_listings w JOIN documents d ON w.document_id = d.id WHERE d.source='eprtr'")[0]['c']

print(f"🇪🇺 E-PRTR Docs Processed: {total_eprtr}")
print(f"📝 E-PRTR Records: {total_recs}")
print(f"❌ Records with Missing Location: {missing}")

if total_recs > 0:
    print(f"✅ LOCATION QUALITY: {(1 - missing/total_recs)*100:.2f}%")
