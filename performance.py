"""PERFORMANCE DASHBOARD - Real Metrics"""
from store.postgres import execute_query
from datetime import datetime, timedelta

print("="*65)
print("   📊 PERFORMANCE DASHBOARD")
print("="*65)

# 1. TOTAL COUNTS
print("\n📦 DATA TOTALS:")
wl = execute_query("SELECT count(*) as cnt FROM waste_listings")[0]['cnt']
docs = execute_query("SELECT count(*) as cnt FROM documents")[0]['cnt']
gov_docs = execute_query("SELECT count(*) as cnt FROM documents WHERE source = 'government'")[0]['cnt']
print(f"   Waste Listings: {wl}")
print(f"   Total Docs: {docs}")
print(f"   EPA Docs: {gov_docs}")

# 2. THROUGHPUT METRICS
print("\n⚡ THROUGHPUT:")
if gov_docs > 0 and wl > 0:
    records_per_doc = wl / gov_docs if gov_docs > 0 else 0
    print(f"   Records per EPA doc: {records_per_doc:.1f}")
else:
    print("   Not enough data yet")

# 3. TIME-BASED METRICS
print("\n🕐 TIME ANALYSIS:")
recent = execute_query("""
    SELECT count(*) as cnt FROM documents 
    WHERE ingested_at > NOW() - INTERVAL '1 hour'
""")[0]['cnt']
print(f"   Docs ingested last hour: {recent}")
print(f"   Projected per 8-hour night: {recent * 8}")

# 4. PROCESSING STATUS
print("\n⚙️ PROCESSING STATUS:")
statuses = execute_query("""
    SELECT status, count(*) as cnt 
    FROM documents 
    GROUP BY status
""")
for s in statuses:
    print(f"   {s['status']}: {s['cnt']}")

# 5. DOCUMENT QUEUE
pending = execute_query("SELECT count(*) as cnt FROM documents WHERE status = 'pending'")[0]['cnt']
print(f"\n📋 QUEUE:")
print(f"   Pending docs: {pending}")
print(f"   Processing capacity: ~100 docs/hour (single thread)")

# 6. TREATMENT BREAKDOWN (quality check)
print("\n🏭 TREATMENT BREAKDOWN:")
methods = execute_query("""
    SELECT treatment_method, count(*) as cnt 
    FROM waste_listings 
    GROUP BY treatment_method 
    ORDER BY cnt DESC
""")
for m in methods:
    print(f"   {m['treatment_method'] or 'Unknown'}: {m['cnt']}")

# 7. VERDICT
print("\n" + "="*65)
if wl > 100:
    print("   ✅ GOOD PERFORMANCE - Data is flowing")
elif wl > 10:
    print("   ⚠️ MODERATE - Some data, but could be faster")
else:
    print("   ❌ LOW - Need to check pipeline")
print("="*65)
