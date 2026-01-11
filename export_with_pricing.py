"""Export waste listings with pricing for SymbioFlows."""
import csv
from store.postgres import execute_query

print("="*60)
print("EXPORTING PRICED WASTE DATA")
print("="*60)

# Simple query - only join columns
data = execute_query("""
    SELECT 
        wl.source_company,
        wl.material,
        wl.quantity_tons,
        mtm.material_type_id as price_category,
        mv.price_per_ton_usd,
        (wl.quantity_tons * mv.price_per_ton_usd) as estimated_value_usd
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    ORDER BY estimated_value_usd DESC
""")

print(f"Records to export: {len(data):,}")

# Export to CSV
output_path = "exports/waste_listings_with_pricing.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    if data:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

print(f"[OK] Exported to {output_path}")

# Also export material valuations
valuations = execute_query("SELECT * FROM material_valuations ORDER BY price_per_ton_usd DESC")
val_path = "exports/material_valuations.csv"
with open(val_path, "w", newline="", encoding="utf-8") as f:
    if valuations:
        writer = csv.DictWriter(f, fieldnames=valuations[0].keys())
        writer.writeheader()
        writer.writerows(valuations)
print(f"[OK] Exported to {val_path}")

# Summary
total_value = sum(r["estimated_value_usd"] or 0 for r in data)
total_volume = sum(r["quantity_tons"] or 0 for r in data)
companies = len(set(r["source_company"] for r in data if r["source_company"]))

print(f"\nSUMMARY:")
print(f"  Records: {len(data):,}")
print(f"  Companies: {companies:,}")
print(f"  Volume: {total_volume:,.0f} tons")
print(f"  Total Value: ${total_value:,.0f}")
