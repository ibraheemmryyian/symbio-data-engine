"""COMPACT PERFORMANCE CHECK"""
from store.postgres import execute_query

wl = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']
docs = execute_query("SELECT count(*) as c FROM documents")[0]['c']
gov = execute_query("SELECT count(*) as c FROM documents WHERE source = 'government'")[0]['c']
pending = execute_query("SELECT count(*) as c FROM documents WHERE status = 'pending'")[0]['c']
recent = execute_query("SELECT count(*) as c FROM documents WHERE ingested_at > NOW() - INTERVAL '1 hour'")[0]['c']

print(f"WASTE LISTINGS: {wl}")
print(f"TOTAL DOCS: {docs}")
print(f"EPA DOCS: {gov}")
print(f"PENDING: {pending}")
print(f"LAST HOUR INGESTED: {recent}")
print(f"RECORDS/DOC: {wl/gov if gov > 0 else 0:.1f}")
print(f"PROJECTED 8HR: {recent * 8} docs")
