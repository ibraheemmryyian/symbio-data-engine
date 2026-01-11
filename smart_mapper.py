"""
Smart material mapper with proper disaggregation.
Doesn't dump everything into "Hazardous" - creates proper sub-categories.
"""
from store.postgres import execute_query, get_connection
from collections import defaultdict

print("="*70)
print("SMART MATERIAL MAPPER")
print("="*70)

materials = execute_query("SELECT DISTINCT material FROM waste_listings ORDER BY material")
print(f"Total unique materials: {len(materials)}")

# More granular mapping rules - no catch-all "hazardous"
RULES = [
    # High-value metals (high confidence)
    ("copper", "COPPER"),
    ("aluminum", "ALUMINUM"),
    ("aluminium", "ALUMINUM"),
    ("steel", "STEEL"),
    ("iron", "IRON"),
    ("brass", "BRASS"),
    ("lead", "LEAD"),
    ("zinc", "ZINC"),
    ("nickel", "NICKEL"),
    ("stainless", "STAINLESS"),
    
    # Plastics
    ("plastic", "PLASTICS"),
    ("polymer", "PLASTICS"),
    ("hdpe", "PLASTICS"),
    ("ldpe", "PLASTICS"),
    ("pet", "PLASTICS"),
    ("pvc", "PLASTICS"),
    ("polypropylene", "PLASTICS"),
    ("polyethylene", "PLASTICS"),
    ("polystyrene", "PLASTICS"),
    
    # Paper
    ("paper", "PAPER"),
    ("cardboard", "PAPER"),
    ("carton", "PAPER"),
    
    # Glass
    ("glass", "GLASS"),
    
    # Organics
    ("organic", "ORGANICS"),
    ("biomass", "ORGANICS"),
    ("wood", "ORGANICS"),
    ("timber", "ORGANICS"),
    ("food", "ORGANICS"),
    
    # Construction
    ("concrete", "CONSTRUCTION"),
    ("cement", "CONSTRUCTION"),
    ("brick", "CONSTRUCTION"),
    ("demolition", "CONSTRUCTION"),
    ("asphalt", "CONSTRUCTION"),
    
    # Electronics
    ("electronic", "ELECTRONICS"),
    ("battery", "ELECTRONICS"),
    ("circuit", "ELECTRONICS"),
    ("cable", "ELECTRONICS"),
    ("wire", "ELECTRONICS"),
    
    # Textiles
    ("textile", "TEXTILES"),
    ("fabric", "TEXTILES"),
    ("cotton", "TEXTILES"),
    ("leather", "TEXTILES"),
    
    # Rubber
    ("rubber", "RUBBER"),
    ("tire", "RUBBER"),
    ("tyre", "RUBBER"),
    
    # Oils/Petroleum
    ("oil", "PETROLEUM"),
    ("petroleum", "PETROLEUM"),
    ("fuel", "PETROLEUM"),
    ("lubricant", "PETROLEUM"),
    
    # Solvents
    ("solvent", "SOLVENTS"),
    ("methanol", "SOLVENTS"),
    ("ethanol", "SOLVENTS"),
    ("acetone", "SOLVENTS"),
    ("toluene", "SOLVENTS"),
    
    # Acids
    ("acid", "ACIDS"),
    ("sulfuric", "ACIDS"),
    ("hydrochloric", "ACIDS"),
    ("phosphoric", "ACIDS"),
    ("nitric", "ACIDS"),
    
    # Bases
    ("hydroxide", "BASES"),
    ("ammonia", "BASES"),
    ("caustic", "BASES"),
    
    # Chlorinated
    ("chlor", "CHLORINATED"),
    
    # Fluorinated
    ("fluor", "FLUORINATED"),
    
    # Oxides/Salts
    ("oxide", "OXIDES-SALTS"),
    ("sulfate", "OXIDES-SALTS"),
    ("nitrate", "OXIDES-SALTS"),
    ("carbonate", "OXIDES-SALTS"),
    
    # General waste types
    ("hazardous waste", "HAZ-WASTE-GENERAL"),
    ("non-hazardous", "NONHAZ-WASTE"),
    ("nonhazardous", "NONHAZ-WASTE"),
    ("waste", "MIXED-WASTE"),
    ("sludge", "SLUDGE"),
    ("ash", "ASH"),
    ("residue", "RESIDUE"),
]

# Category prices (USD/ton)
CATEGORY_PRICES = {
    "COPPER": 10800,
    "ALUMINUM": 2000,
    "STEEL": 320,
    "IRON": 280,
    "BRASS": 6500,
    "LEAD": 1500,
    "ZINC": 900,
    "NICKEL": 12000,
    "STAINLESS": 1000,
    "PLASTICS": 1100,
    "PAPER": 100,
    "GLASS": 50,
    "ORGANICS": 30,
    "CONSTRUCTION": 25,
    "ELECTRONICS": 3000,
    "TEXTILES": 200,
    "RUBBER": 150,
    "PETROLEUM": 400,
    "SOLVENTS": 600,
    "ACIDS": 200,
    "BASES": 300,
    "CHLORINATED": 300,
    "FLUORINATED": 500,
    "OXIDES-SALTS": 150,
    "HAZ-WASTE-GENERAL": 150,
    "NONHAZ-WASTE": 80,
    "MIXED-WASTE": 50,
    "SLUDGE": 30,
    "ASH": 20,
    "RESIDUE": 25,
    "UNCLASSIFIED": 40,  # Honest catch-all
}

# Map materials
mapped = {}
category_counts = defaultdict(int)

for row in materials:
    mat = row["material"]
    mat_lower = mat.lower()
    
    found = False
    for keyword, category in RULES:
        if keyword in mat_lower:
            mapped[mat] = category
            category_counts[category] += 1
            found = True
            break
    
    if not found:
        mapped[mat] = "UNCLASSIFIED"
        category_counts["UNCLASSIFIED"] += 1

print(f"\nMapped: {len(mapped)} / {len(materials)}")

# Show distribution
print("\n" + "="*70)
print("CATEGORY DISTRIBUTION:")
print("="*70)
for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
    pct = count / len(materials) * 100
    print(f"  {cat:<20} {count:>4} materials ({pct:>5.1f}%)")

# Add category valuations to database
print("\n" + "="*70)
print("STORING CATEGORY PRICES")
print("="*70)

with get_connection() as conn:
    with conn.cursor() as cur:
        # Clear old valuations
        cur.execute("DELETE FROM material_valuations")
        
        for category, price in CATEGORY_PRICES.items():
            cur.execute("""
                INSERT INTO material_valuations 
                    (material_type_id, material_name, material_category, 
                     price_per_ton_usd, source_count, confidence_score)
                VALUES (%s, %s, %s, %s, 1, 0.7)
            """, (category, category.lower().replace("-", " "), 'category', price))
        
        conn.commit()
        
print(f"[OK] {len(CATEGORY_PRICES)} category prices stored")

# Store mappings
print("\n" + "="*70)
print("STORING MAPPINGS")
print("="*70)

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM material_type_mapping")
        
        for material, category in mapped.items():
            confidence = 0.9 if category != "UNCLASSIFIED" else 0.4
            cur.execute("""
                INSERT INTO material_type_mapping 
                    (waste_material, material_type_id, match_confidence)
                VALUES (%s, %s, %s)
            """, (material, category, confidence))
        
        conn.commit()

print(f"[OK] {len(mapped)} mappings stored")

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
val_count = execute_query("SELECT COUNT(*) as c FROM material_valuations")[0]["c"]
map_count = execute_query("SELECT COUNT(*) as c FROM material_type_mapping")[0]["c"]
companies = execute_query("SELECT COUNT(DISTINCT source_company) as c FROM waste_listings")[0]["c"]

print(f"Unique companies: {companies}")
print(f"Unique materials: {len(materials)}")
print(f"Price categories: {val_count}")
print(f"Material mappings: {map_count}")
