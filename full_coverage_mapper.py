"""
Map ALL 586 materials to pricing using category-based defaults.
Every material gets a price - either specific or default by category.
"""
from store.postgres import execute_query, get_connection

print("="*70)
print("FULL COVERAGE PRICING MAPPER")
print("="*70)

materials = execute_query("SELECT DISTINCT material FROM waste_listings ORDER BY material")
print(f"Total unique materials: {len(materials)}")

# Default valuations to add ($/ton)
# These are estimated disposal/recovery values
DEFAULT_PRICES = {
    "CHEM-HAZ": ("Hazardous Chemicals", 150),     # Recovery/treatment value
    "CHEM-NONHAZ": ("Non-Hazardous Chemicals", 80),
    "METAL-MIXED": ("Mixed Metals", 320),          # HMS steel price
    "PLASTIC-MIXED": ("Mixed Plastics", 200),
    "PAPER-MIXED": ("Mixed Paper", 80),
    "ORGANIC": ("Organic Waste", 50),
    "CONSTRUCTION": ("Construction Waste", 30),
    "OTHER": ("Other Industrial", 50),
}

# Specific material mappings (keyword -> price_type)
SPECIFIC_RULES = [
    # High-value metals
    ("copper", "CU-BAREBRGHT"),
    ("aluminum", "AL-6063"),
    ("aluminium", "AL-6063"),
    ("steel", "ST-HMS1"),
    ("iron", "ST-HMS1"),
    ("brass", "BR-YELLOW"),
    ("lead", "PB-SOLID"),
    ("battery", "PB-BATTERIES"),
    ("zinc", "ST-HMS1"),
    
    # Plastics
    ("plastic", "PLASTIC-MIXED"),
    ("polymer", "PLASTIC-MIXED"),
    ("hdpe", "PLASTIC-MIXED"),
    ("pvc", "PLASTIC-MIXED"),
    ("pet", "PLASTIC-MIXED"),
    
    # Paper
    ("paper", "PAPER-MIXED"),
    ("cardboard", "PAPER-MIXED"),
    
    # Electronics
    ("electronic", "CU-BAREBRGHT"),
    ("cable", "CU-WIRE1"),
    ("wire", "CU-WIRE1"),
    
    # Organic
    ("organic", "ORGANIC"),
    ("biomass", "ORGANIC"),
    ("wood", "ORGANIC"),
    ("food", "ORGANIC"),
    
    # Construction
    ("concrete", "CONSTRUCTION"),
    ("cement", "CONSTRUCTION"),
    ("demolition", "CONSTRUCTION"),
]

# Category-based defaults (for chemicals and unknowns)
CATEGORY_DEFAULTS = [
    ("hazardous", "CHEM-HAZ"),
    ("non-hazardous", "CHEM-NONHAZ"),
    ("nonhazardous", "CHEM-NONHAZ"),
    ("toxic", "CHEM-HAZ"),
    ("solvent", "CHEM-HAZ"),
    ("acid", "CHEM-HAZ"),
    ("chlor", "CHEM-HAZ"),       # chloride, chloro-, etc.
    ("fluor", "CHEM-HAZ"),       # fluoride, fluoro-, etc.
    ("oxide", "CHEM-NONHAZ"),
    ("sulfate", "CHEM-NONHAZ"),
    ("oil", "CHEM-NONHAZ"),
]

# First, add default price categories to material_valuations
print("\nAdding category default prices...")
with get_connection() as conn:
    with conn.cursor() as cur:
        for type_id, (name, price) in DEFAULT_PRICES.items():
            cur.execute("DELETE FROM material_valuations WHERE material_type_id = %s", (type_id,))
            cur.execute("""
                INSERT INTO material_valuations 
                    (material_type_id, material_name, material_category, price_per_ton_usd, 
                     price_per_lb_usd, source_count, confidence_score)
                VALUES (%s, %s, 'default', %s, %s, 1, 0.5)
            """, (type_id, name, price, price * 0.000453592))
        conn.commit()
print(f"[OK] {len(DEFAULT_PRICES)} default prices added")

# Now map ALL materials
print("\nMapping all materials...")
mapped = {}

for row in materials:
    mat = row["material"]
    mat_lower = mat.lower()
    
    # Try specific rules first
    for keyword, type_id in SPECIFIC_RULES:
        if keyword in mat_lower:
            mapped[mat] = (type_id, 0.8)
            break
    else:
        # Try category defaults
        for keyword, type_id in CATEGORY_DEFAULTS:
            if keyword in mat_lower:
                mapped[mat] = (type_id, 0.6)
                break
        else:
            # Default: treat as non-hazardous chemical
            mapped[mat] = ("CHEM-NONHAZ", 0.4)

print(f"Mapped: {len(mapped)} / {len(materials)}")
print(f"Coverage: {len(mapped)/len(materials)*100:.1f}%")

# Store all mappings
print("\nStoring mappings...")
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM material_type_mapping")
        
        for material, (type_id, confidence) in mapped.items():
            cur.execute("""
                INSERT INTO material_type_mapping (waste_material, material_type_id, match_confidence)
                VALUES (%s, %s, %s)
            """, (material, type_id, confidence))
        
        conn.commit()

print(f"[OK] {len(mapped)} mappings stored")

# Verify coverage
print("\n" + "="*70)
print("COVERAGE SUMMARY")
print("="*70)

# By category
category_counts = execute_query("""
    SELECT material_type_id, COUNT(*) as count
    FROM material_type_mapping
    GROUP BY material_type_id
    ORDER BY count DESC
""")
for row in category_counts:
    print(f"  {row['material_type_id']:<20} {row['count']:>4} materials")

# Total coverage
total_mapped = execute_query("SELECT COUNT(*) as c FROM material_type_mapping")[0]["c"]
total_valuations = execute_query("SELECT COUNT(*) as c FROM material_valuations")[0]["c"]

print(f"\nTotal material mappings: {total_mapped}")
print(f"Total price categories: {total_valuations}")
print(f"Coverage: {total_mapped/len(materials)*100:.1f}%")
