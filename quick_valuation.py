"""Quick valuation summary."""
from store.postgres import execute_query

result = execute_query("""
    SELECT 
        COUNT(*) as records,
        SUM(wl.quantity_tons) as tons,
        SUM(wl.quantity_tons * mv.price_per_ton_usd) as value
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
""")[0]

print("="*60)
print("FULL VALUATION SUMMARY")
print("="*60)
print(f"Records with pricing: {result['records']:,}")
print(f"Volume: {result['tons']:,.0f} tons")
print(f"Total Value: ${result['value']:,.0f}")
print()

# By category
print("VALUE BY CATEGORY:")
by_cat = execute_query("""
    SELECT 
        mv.material_name,
        COUNT(*) as records,
        SUM(wl.quantity_tons) as tons,
        SUM(wl.quantity_tons * mv.price_per_ton_usd) as value
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    GROUP BY mv.material_name, mv.material_type_id
    ORDER BY value DESC
""")
for row in by_cat[:10]:
    print(f"  {row['material_name']:<25} ${row['value']:>18,.0f}  ({row['tons']:,.0f} tons)")
