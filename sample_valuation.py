"""Sample valuation query - prove the pricing system works."""
from store.postgres import execute_query

print("="*60)
print("SAMPLE WASTE VALUATION")
print("="*60)

# Get some waste listings that have pricing
result = execute_query("""
    SELECT 
        wl.material,
        wl.quantity_tons,
        wl.source_company,
        mv.price_per_ton_usd,
        ROUND((wl.quantity_tons * mv.price_per_ton_usd)::numeric, 2) as value_usd
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    WHERE wl.quantity_tons > 0
    ORDER BY value_usd DESC
    LIMIT 10
""")

if result:
    print(f"\nTop 10 highest-value waste streams (with pricing):\n")
    total_value = 0
    for r in result:
        print(f"  {r['material'][:35]:<35}")
        print(f"    {r['quantity_tons']:>15,.0f} tons x ${r['price_per_ton_usd']:>8,.0f}/ton = ${r['value_usd']:>15,.2f}")
        print(f"    Company: {r['source_company'][:40]}")
        print()
        total_value += float(r['value_usd'])
    print(f"{'='*60}")
    print(f"Top 10 total value: ${total_value:,.2f}")
else:
    print("No matched records found")

# Total valued volume
totals = execute_query("""
    SELECT 
        COUNT(*) as records,
        SUM(wl.quantity_tons) as tons,
        SUM(wl.quantity_tons * mv.price_per_ton_usd) as total_value
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    WHERE wl.quantity_tons > 0
""")

if totals and totals[0]['records']:
    t = totals[0]
    print(f"\nOVERALL COVERAGE:")
    print(f"  Records with pricing: {t['records']:,}")
    print(f"  Volume: {t['tons']:,.0f} tons")
    print(f"  Total value: ${t['total_value']:,.2f}")
