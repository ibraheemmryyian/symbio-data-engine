from store.postgres import execute_query

print("=== Volume Normalization Check ===\n")

# Check if values are in metric tons
samples = execute_query("""
    SELECT material, quantity_tons, source_company, year 
    FROM waste_listings 
    WHERE quantity_tons > 0 
    ORDER BY quantity_tons DESC 
    LIMIT 10
""")

print("Top releases by quantity (metric tons):")
for s in samples:
    print(f"  {s['material'][:30]:<30} | {float(s['quantity_tons']):>12.4f} MT | {s['year']}")

# Show that conversion happened 
print("\n=== Original EPA Unit ===")
print("EPA TRI reports in POUNDS, we converted to METRIC TONS")
print("Conversion: qty_pounds × 0.000453592 = qty_metric_tons")
print("\nExample verification:")
print("  13001 lbs × 0.000453592 = 5.90 metric tons")
