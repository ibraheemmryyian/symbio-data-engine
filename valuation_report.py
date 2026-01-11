"""Full valuation breakdown with disaggregated categories."""
from store.postgres import execute_query

print("="*70)
print("DISAGGREGATED VALUATION REPORT")
print("="*70)

result = execute_query("""
    SELECT 
        COUNT(*) as records,
        SUM(wl.quantity_tons) as tons,
        SUM(wl.quantity_tons * mv.price_per_ton_usd) as value
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
""")[0]

companies = execute_query("SELECT COUNT(DISTINCT source_company) as c FROM waste_listings")[0]["c"]

print(f"\nUNIQUE COMPANIES: {companies}")
print(f"Records with pricing: {result['records']:,}")
print(f"Volume: {result['tons']:,.0f} tons")
print(f"Total Value: ${result['value']:,.0f}")

# By category
print("\n" + "="*70)
print("VALUE BY CATEGORY (DISAGGREGATED):")
print("="*70)

by_cat = execute_query("""
    SELECT 
        mv.material_type_id as category,
        COUNT(DISTINCT wl.material) as unique_materials,
        SUM(wl.quantity_tons) as tons,
        mv.price_per_ton_usd as price,
        SUM(wl.quantity_tons * mv.price_per_ton_usd) as value
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    GROUP BY mv.material_type_id, mv.price_per_ton_usd
    ORDER BY value DESC
""")

print(f"\n{'Category':<20} {'Materials':>8} {'Volume (tons)':>18} {'$/ton':>8} {'Value ($)':>18}")
print("-"*75)
for row in by_cat:
    print(f"{row['category']:<20} {row['unique_materials']:>8} {row['tons']:>18,.0f} {row['price']:>8,.0f} {row['value']:>18,.0f}")

# Highlight high-confidence metals
print("\n" + "="*70)
print("HIGH-CONFIDENCE METAL VALUATIONS:")
print("="*70)
metals = ["COPPER", "ALUMINUM", "STEEL", "BRASS", "LEAD", "ZINC", "STAINLESS", "NICKEL"]
for row in by_cat:
    if row['category'] in metals:
        pct = row['value'] / result['value'] * 100 if result['value'] else 0
        print(f"  {row['category']:<15} ${row['value']:>15,.0f}  ({row['tons']:,.0f} tons @ ${row['price']}/ton)")
