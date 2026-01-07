"""DEBUG STALL - Why is counts flat?"""
from store.postgres import execute_query

print("="*60)
print("   🕵️ DEBUGGING STALLED COUNTS")
print("="*60)

# 1. QUEUE STATUS
print("\n1️⃣ QUEUE COMPOSITION:")
pending = execute_query("""
    SELECT source, document_type, count(*) as c 
    FROM documents 
    WHERE status = 'pending' 
    GROUP BY source, document_type
""")
for p in pending:
    print(f"   Pending: {p['source']} ({p['document_type']}) -> {p['c']}")

# 2. PROCESSING STATE (Is things stuck in 'processing'?)
stuck = execute_query("""
    SELECT count(*) as c FROM documents WHERE status = 'processing'
""")
print(f"\n2️⃣ IN PROGRESS: {stuck[0]['c']} docs currently processing")

# 3. RECENT ACTIVITY
print("\n3️⃣ RECENT INGESTION (Last 30 mins):")
recent = execute_query("""
    SELECT count(*) as c FROM documents 
    WHERE ingested_at > NOW() - INTERVAL '30 minutes'
""")
print(f"   New Docs Downloaded: {recent[0]['c']}")

# 4. RECENT EXTRACTIONS
recent_extract = execute_query("""
    SELECT count(*) as c FROM waste_listings 
    WHERE created_at > NOW() - INTERVAL '30 minutes'
""")
print(f"   New Listings Extracted: {recent_extract[0]['c']}")

print("\n" + "="*60)
