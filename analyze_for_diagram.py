"""Analyze materials and generate grouping stats for diagram."""
from store.postgres import execute_query
from collections import defaultdict

materials = execute_query("SELECT DISTINCT material FROM waste_listings ORDER BY material")
print(f"Total unique materials: {len(materials)}")

# Category rules
CATEGORIES = {
    "Metals - Ferrous": ["steel", "iron", "ferrous", "stainless", "cast iron"],
    "Metals - Non-Ferrous": ["copper", "aluminum", "aluminium", "brass", "bronze", "lead", "zinc", "nickel", "tin", "titanium"],
    "Metals - Precious": ["gold", "silver", "platinum", "palladium", "precious"],
    "Plastics - Commodity": ["hdpe", "ldpe", "pet", "pp ", "polypropylene", "polyethylene", "pvc", "polystyrene", "abs"],
    "Plastics - Engineering": ["nylon", "polyamide", "polycarbonate", "acrylic", "ptfe", "teflon"],
    "Chemicals - Solvents": ["solvent", "acetone", "toluene", "xylene", "methanol", "ethanol", "alcohol"],
    "Chemicals - Acids": ["acid", "sulfuric", "nitric", "hydrochloric", "phosphoric"],
    "Chemicals - Bases": ["hydroxide", "alkali", "ammonia", "caustic", "sodium"],
    "Chemicals - Other": ["chloride", "sulfate", "nitrate", "oxide", "fluoride", "cyanide"],
    "Petroleum & Oils": ["oil", "petroleum", "fuel", "lubricant", "grease", "diesel", "bitumen"],
    "Paper & Cardboard": ["paper", "cardboard", "carton", "occ", "kraft", "pulp"],
    "Glass": ["glass", "cullet"],
    "Rubber & Tires": ["rubber", "tire", "tyre", "latex"],
    "Textiles": ["textile", "fabric", "cotton", "wool", "polyester", "nylon fiber", "leather"],
    "Electronics": ["electronic", "pcb", "circuit", "battery", "batteries", "cable", "computer", "ewaste"],
    "Construction": ["concrete", "cement", "brick", "asphalt", "gypsum", "demolition", "rubble", "aggregate"],
    "Organic & Bio": ["organic", "biomass", "compost", "food", "manure", "agricultural", "wood", "timber"],
    "Medical & Pharma": ["medical", "pharmaceutical", "clinical", "hospital", "sharps"],
    "Industrial Mixed": [],  # Catch-all
}

categorized = defaultdict(list)
for row in materials:
    mat = row["material"].lower()
    found = False
    for cat, keywords in CATEGORIES.items():
        if any(kw in mat for kw in keywords):
            categorized[cat].append(row["material"])
            found = True
            break
    if not found:
        categorized["Industrial Mixed"].append(row["material"])

# Print stats for diagram
print("\n" + "="*60)
print("CATEGORY DISTRIBUTION")
print("="*60)
total = len(materials)
covered = 0
for cat, items in sorted(categorized.items(), key=lambda x: -len(x[1])):
    count = len(items)
    pct = count / total * 100
    covered += count if cat != "Industrial Mixed" else 0
    print(f"{cat:<25} {count:>4} ({pct:>5.1f}%)  {items[:2]}")

accuracy = covered / total * 100
print(f"\nGrouped materials: {covered}/{total} = {accuracy:.1f}% coverage")
