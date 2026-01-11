from store.postgres import execute_query

total = execute_query("SELECT SUM(quantity_tons) as t FROM waste_listings")[0]["t"]

top = execute_query("""
    SELECT material, SUM(quantity_tons) as tons, COUNT(*) as records
    FROM waste_listings
    GROUP BY material
    ORDER BY tons DESC
    LIMIT 50
""")

running = 0
lines = []
lines.append(f"Total: {total:,.0f} tons | 586 materials\n")
for i, r in enumerate(top, 1):
    running += r["tons"] or 0
    pct = (running / total * 100) if total else 0
    mat = r["material"][:40]
    lines.append(f"{i:>2}. {mat:<40} {r['tons']:>15,.0f}t  ({pct:.0f}%)")

lines.append(f"\n>>> Top 50 = {running/total*100:.1f}% of total volume")

with open("top50_results.txt", "w") as f:
    f.write("\n".join(lines))
    
print("Saved to top50_results.txt")
print(f"Top 50 = {running/total*100:.1f}% of {total:,.0f} tons")
