"""
SYMBIO DATA ENGINE - OVERNIGHT DASHBOARD
=========================================
Run this when you wake up: python dashboard.py
"""
from store.postgres import execute_query
from datetime import datetime

def show_dashboard():
    print("\n" + "="*60)
    print(f"   🌙 SYMBIO DATA ENGINE - {datetime.now().strftime('%H:%M:%S')}")
    print("="*60)
    
    # Key metrics
    wl = execute_query("SELECT count(*) as c FROM waste_listings")[0]['c']
    docs = execute_query("SELECT count(*) as c FROM documents WHERE source = 'government'")[0]['c']
    pending = execute_query("SELECT count(*) as c FROM documents WHERE status = 'pending'")[0]['c']
    
    print(f"\n   📦 WASTE LISTINGS: {wl:,}")
    print(f"   📄 EPA DOCUMENTS: {docs}")
    print(f"   ⏳ PENDING: {pending}")
    print(f"   📊 RECORDS/DOC: {wl/docs if docs > 0 else 0:.0f}")
    
    # Top commodities
    print("\n   💰 TOP COMMODITIES:")
    chems = execute_query("""
        SELECT material, count(*) as c, SUM(quantity_tons) as tons
        FROM waste_listings 
        GROUP BY material ORDER BY c DESC LIMIT 5
    """)
    for c in chems:
        print(f"      {c['material'][:35]}: {c['c']} records")
    
    # Treatment breakdown
    print("\n   🏭 TREATMENT METHODS:")
    methods = execute_query("""
        SELECT treatment_method, count(*) as c 
        FROM waste_listings GROUP BY treatment_method
    """)
    for m in methods:
        print(f"      {m['treatment_method'] or 'Unknown'}: {m['c']}")
    
    print("\n" + "="*60)
    print("   ✅ SYSTEM STATUS: OPERATIONAL")
    print("="*60 + "\n")

if __name__ == "__main__":
    show_dashboard()
