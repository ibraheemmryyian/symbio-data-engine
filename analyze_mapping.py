"""Analyze mapping distribution to find the greedy catch-all."""
from store.postgres import execute_query

print("="*70)
print("MAPPING ANALYSIS")
print("="*70)

# Distribution by price category
by_cat = execute_query("""
    SELECT mtm.material_type_id, COUNT(*) as material_count
    FROM material_type_mapping mtm
    GROUP BY mtm.material_type_id
    ORDER BY material_count DESC
""")

print("\nMATERIALS BY PRICE CATEGORY:")
for row in by_cat[:20]:
    print(f"  {row['material_type_id']:<25} {row['material_count']:>5} materials")

# Volume by price category 
by_vol = execute_query("""
    SELECT 
        mv.material_name,
        COUNT(DISTINCT wl.material) as unique_materials,
        SUM(wl.quantity_tons) as total_tons
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    GROUP BY mv.material_name
    ORDER BY total_tons DESC
""")

print("\n" + "="*70)
print("VOLUME BY PRICE CATEGORY:")
print("="*70)
for row in by_vol[:15]:
    print(f"  {row['material_name']:<30} {row['unique_materials']:>5} mats  {row['total_tons']:>15,.0f} tons")

# Sample materials in "Hazardous" category
print("\n" + "="*70)
print("SAMPLE MATERIALS IN HAZARDOUS BUCKET:")
print("="*70)

hazardous = execute_query("""
    SELECT waste_material 
    FROM material_type_mapping 
    WHERE material_type_id = 'CHEM-HAZ'
    LIMIT 30
""")

for row in hazardous:
    print(f"  - {row['waste_material']}")
