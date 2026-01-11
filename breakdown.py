from store.postgres import execute_query
import sys

sys.stdout.reconfigure(encoding='utf-8')

print("UNIQUE COMPANIES:", execute_query("SELECT COUNT(DISTINCT source_company) as c FROM waste_listings")[0]["c"])

by_cat = execute_query("""
    SELECT mv.material_type_id as cat, SUM(wl.quantity_tons) as tons, SUM(wl.quantity_tons * mv.price_per_ton_usd) as val
    FROM waste_listings wl
    JOIN material_type_mapping mtm ON wl.material = mtm.waste_material
    JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
    GROUP BY mv.material_type_id ORDER BY val DESC
""")

with open("breakdown_output.txt", "w") as f:
    f.write(f"UNIQUE COMPANIES: {execute_query('SELECT COUNT(DISTINCT source_company) as c FROM waste_listings')[0]['c']}\n\n")
    f.write(f"{'Category':<18} {'Tons':>18} {'Value':>18}\n")
    f.write("-"*56 + "\n")
    for r in by_cat:
        f.write(f"{r['cat']:<18} {r['tons']:>18,.0f} ${r['val']:>17,.0f}\n")

print("Saved to breakdown_output.txt")
