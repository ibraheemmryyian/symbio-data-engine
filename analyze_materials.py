"""Analyze material distribution to plan pricing coverage."""
from store.postgres import execute_query

print("="*70)
print("MATERIAL CATEGORY ANALYSIS")
print("="*70)

# Get sample of materials
materials = execute_query("""
    SELECT material, COUNT(*) as records, SUM(quantity_tons) as total_tons
    FROM waste_listings
    GROUP BY material
    ORDER BY records DESC
""")

# Categorize materials
categories = {
    "metals": [],
    "plastics": [],
    "chemicals": [],
    "organics": [],
    "paper": [],
    "glass": [],
    "electronics": [],
    "construction": [],
    "other": [],
}

metal_keywords = ["copper", "steel", "iron", "aluminum", "aluminium", "brass", "lead", "zinc", "nickel", "tin", "metal", "scrap"]
plastic_keywords = ["plastic", "pvc", "hdpe", "ldpe", "pet", "pp", "polypropylene", "polyethylene", "polymer"]
chemical_keywords = ["acid", "solvent", "oil", "chemical", "hydroxide", "chloride", "sulfate", "nitrate", "oxide", "waste oil", "lubricant"]
organic_keywords = ["organic", "food", "bio", "manure", "compost", "wood", "timber", "agricultural"]
paper_keywords = ["paper", "cardboard", "carton", "occ", "pulp"]
glass_keywords = ["glass", "cullet"]
ewaste_keywords = ["electronic", "battery", "circuit", "pcb", "cable", "wire"]
construction_keywords = ["concrete", "brick", "asphalt", "rubble", "gypsum", "demolition"]

for row in materials:
    mat = row["material"].lower()
    records = row["records"]
    tons = row["total_tons"] or 0
    
    if any(k in mat for k in metal_keywords):
        categories["metals"].append((row["material"], records, tons))
    elif any(k in mat for k in plastic_keywords):
        categories["plastics"].append((row["material"], records, tons))
    elif any(k in mat for k in chemical_keywords):
        categories["chemicals"].append((row["material"], records, tons))
    elif any(k in mat for k in organic_keywords):
        categories["organics"].append((row["material"], records, tons))
    elif any(k in mat for k in paper_keywords):
        categories["paper"].append((row["material"], records, tons))
    elif any(k in mat for k in glass_keywords):
        categories["glass"].append((row["material"], records, tons))
    elif any(k in mat for k in ewaste_keywords):
        categories["electronics"].append((row["material"], records, tons))
    elif any(k in mat for k in construction_keywords):
        categories["construction"].append((row["material"], records, tons))
    else:
        categories["other"].append((row["material"], records, tons))

# Summary
print(f"\nTotal unique materials: {len(materials)}")
print()
print(f"{'Category':<20} {'Materials':>10} {'Records':>12} {'Total Tons':>15}  Pricing Status")
print("-"*80)

for cat, items in sorted(categories.items(), key=lambda x: -len(x[1])):
    total_records = sum(i[1] for i in items)
    total_tons = sum(i[2] for i in items)
    
    if cat == "metals":
        status = "✅ COVERED (15 prices)"
    elif cat == "plastics":
        status = "🔶 PARTIAL (need RecycleInMe)"
    elif cat in ["paper", "glass"]:
        status = "🔶 PARTIAL (need more sources)"
    else:
        status = "⚪ Uses category averages"
    
    print(f"{cat:<20} {len(items):>10} {total_records:>12,} {total_tons:>15,.0f}  {status}")

# Show top materials per category
print("\n" + "="*70)
print("TOP 3 MATERIALS PER CATEGORY")
print("="*70)
for cat, items in sorted(categories.items(), key=lambda x: -len(x[1])):
    if items:
        print(f"\n{cat.upper()}:")
        for mat, records, tons in items[:3]:
            print(f"  {mat[:50]:<50} ({records:,} records)")
