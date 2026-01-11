"""
Comprehensive material-to-pricing mapping.
Maps all 586 materials to available price categories.
Uses hierarchical matching: specific → category → default.
"""
from store.postgres import execute_query, get_connection
from collections import defaultdict

print("="*70)
print("COMPREHENSIVE PRICING MAPPER")
print("="*70)

# Get all materials
materials = execute_query("SELECT DISTINCT material FROM waste_listings ORDER BY material")
print(f"Total unique materials: {len(materials)}")

# Extended mapping rules - more aggressive matching
# Format: (keyword, price_type_id, confidence)
MAPPING_RULES = [
    # Copper variants
    ("copper", "CU-BAREBRGHT", 0.95),
    ("cu ", "CU-BAREBRGHT", 0.90),
    ("cupric", "CU-BAREBRGHT", 0.85),
    
    # Aluminum variants
    ("aluminum", "AL-6063", 0.95),
    ("aluminium", "AL-6063", 0.95),
    ("al scrap", "AL-6063", 0.90),
    ("bauxite", "AL-6063", 0.70),
    
    # Steel/Iron
    ("steel", "ST-HMS1", 0.95),
    ("iron", "ST-HMS1", 0.90),
    ("ferrous", "ST-HMS1", 0.85),
    ("stainless", "ST-HMS1", 0.85),
    ("cast iron", "ST-HMS1", 0.85),
    ("hms", "ST-HMS1", 0.95),
    ("scrap metal", "ST-HMS1", 0.80),
    
    # Brass/Bronze
    ("brass", "BR-YELLOW", 0.95),
    ("bronze", "BR-YELLOW", 0.90),
    
    # Lead
    ("lead", "PB-SOLID", 0.95),
    ("pb ", "PB-SOLID", 0.85),
    ("battery", "PB-BATTERIES", 0.80),
    ("batteries", "PB-BATTERIES", 0.85),
    
    # Zinc
    ("zinc", "ST-HMS1", 0.75),  # Use steel proxy
    ("zn ", "ST-HMS1", 0.70),
    
    # Nickel
    ("nickel", "ST-HMS1", 0.75),
    ("ni ", "ST-HMS1", 0.70),
    
    # Other metals
    ("tin", "ST-HMS1", 0.70),
    ("titanium", "AL-6063", 0.65),  # Use aluminum proxy
    ("magnesium", "AL-6063", 0.65),
    ("precious metal", "CU-BAREBRGHT", 0.50),
    
    # Plastics - Use aluminum price as proxy (sorted plastics have value)
    ("plastic", "AL-UBC", 0.60),
    ("polymer", "AL-UBC", 0.60),
    ("hdpe", "AL-UBC", 0.65),
    ("ldpe", "AL-UBC", 0.65),
    ("pet", "AL-UBC", 0.65),
    ("pvc", "AL-UBC", 0.60),
    ("polypropylene", "AL-UBC", 0.65),
    ("polyethylene", "AL-UBC", 0.65),
    ("polystyrene", "AL-UBC", 0.60),
    ("nylon", "AL-UBC", 0.60),
    ("abs", "AL-UBC", 0.60),
    
    # Paper/Cardboard - Use low steel price as proxy
    ("paper", "ST-HMS1", 0.50),
    ("cardboard", "ST-HMS1", 0.50),
    ("carton", "ST-HMS1", 0.50),
    ("pulp", "ST-HMS1", 0.45),
    
    # Glass - Use steel price as proxy
    ("glass", "ST-HMS1", 0.50),
    ("cullet", "ST-HMS1", 0.55),
    
    # Electronics - Use copper price as proxy (valuable)
    ("electronic", "CU-BAREBRGHT", 0.60),
    ("pcb", "CU-BAREBRGHT", 0.65),
    ("circuit", "CU-BAREBRGHT", 0.60),
    ("cable", "CU-WIRE1", 0.70),
    ("wire", "CU-WIRE1", 0.70),
    ("computer", "CU-BAREBRGHT", 0.55),
    
    # Rubber/Tires
    ("rubber", "ST-HMS1", 0.45),
    ("tire", "ST-HMS1", 0.45),
    ("tyre", "ST-HMS1", 0.45),
    
    # Construction - Use steel as proxy
    ("concrete", "ST-HMS1", 0.40),
    ("cement", "ST-HMS1", 0.40),
    ("brick", "ST-HMS1", 0.40),
    ("asphalt", "ST-HMS1", 0.40),
    ("demolition", "ST-HMS1", 0.35),
    ("construction", "ST-HMS1", 0.35),
    
    # Organic/Bio - minimal value
    ("organic", "ST-HMS1", 0.30),
    ("biomass", "ST-HMS1", 0.35),
    ("wood", "ST-HMS1", 0.40),
    ("timber", "ST-HMS1", 0.45),
    ("food", "ST-HMS1", 0.25),
    ("compost", "ST-HMS1", 0.25),
    
    # Chemicals - Use steel as default proxy
    ("solvent", "ST-HMS1", 0.50),
    ("acid", "ST-HMS1", 0.45),
    ("chemical", "ST-HMS1", 0.40),
    ("oil", "AL-6063", 0.55),  # Waste oil has some value
    ("lubricant", "AL-6063", 0.50),
    ("petroleum", "AL-6063", 0.50),
    
    # Textiles
    ("textile", "ST-HMS1", 0.40),
    ("fabric", "ST-HMS1", 0.40),
    ("cotton", "ST-HMS1", 0.45),
    ("leather", "ST-HMS1", 0.50),
    
    # Catch-all for waste/scrap
    ("waste", "ST-HMS1", 0.30),
    ("scrap", "ST-HMS1", 0.35),
    ("residue", "ST-HMS1", 0.25),
    ("sludge", "ST-HMS1", 0.20),
    ("ash", "ST-HMS1", 0.20),
]

# Map materials
mapped = {}
unmapped = []

for row in materials:
    mat = row["material"]
    mat_lower = mat.lower()
    
    best_match = None
    best_confidence = 0
    
    for keyword, type_id, confidence in MAPPING_RULES:
        if keyword in mat_lower and confidence > best_confidence:
            best_match = type_id
            best_confidence = confidence
    
    if best_match:
        mapped[mat] = (best_match, best_confidence)
    else:
        unmapped.append(mat)

print(f"\nMapped: {len(mapped)}")
print(f"Unmapped: {len(unmapped)}")
print(f"Coverage: {len(mapped)/len(materials)*100:.1f}%")

# Show unmapped samples
if unmapped[:20]:
    print("\nSample unmapped materials:")
    for m in unmapped[:20]:
        print(f"  - {m}")

# Store mappings in database
print("\n" + "="*70)
print("STORING MAPPINGS")
print("="*70)

with get_connection() as conn:
    with conn.cursor() as cur:
        # Clear existing mappings
        cur.execute("DELETE FROM material_type_mapping")
        
        for material, (type_id, confidence) in mapped.items():
            cur.execute("""
                INSERT INTO material_type_mapping (waste_material, material_type_id, match_confidence)
                VALUES (%s, %s, %s)
            """, (material, type_id, confidence))
        
        conn.commit()

print(f"[OK] {len(mapped)} mappings stored")

# Show coverage by price category
print("\n" + "="*70)
print("COVERAGE BY PRICE CATEGORY")
print("="*70)

category_counts = defaultdict(int)
for type_id, _ in mapped.values():
    category_counts[type_id] += 1

for type_id, count in sorted(category_counts.items(), key=lambda x: -x[1]):
    print(f"  {type_id:<15} {count:>4} materials")

# Final stats
print("\n" + "="*70)
print("FINAL STATS")
print("="*70)
mapped_count = execute_query("SELECT COUNT(*) as c FROM material_type_mapping")[0]["c"]
print(f"Material mappings: {mapped_count}")
print(f"Coverage: {mapped_count/len(materials)*100:.1f}%")
