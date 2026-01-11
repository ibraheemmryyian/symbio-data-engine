"""
Create material category groups for dropdown UI.
Groups 586 materials into ~15-20 categories for easy selection.
"""
from store.postgres import execute_query, get_connection
from collections import defaultdict

print("="*60)
print("CREATING MATERIAL CATEGORY GROUPS")
print("="*60)

# Get all unique materials
materials = execute_query("SELECT DISTINCT material FROM waste_listings ORDER BY material")
print(f"Total unique materials: {len(materials)}")

# Define category rules
CATEGORY_RULES = {
    "Metals - Ferrous": ["steel", "iron", "ferrous", "cast iron", "stainless"],
    "Metals - Non-Ferrous": ["copper", "aluminum", "aluminium", "brass", "bronze", "lead", "zinc", "nickel", "tin"],
    "Metals - Precious": ["gold", "silver", "platinum", "palladium"],
    "Plastics": ["plastic", "polymer", "hdpe", "ldpe", "pet", "pvc", "polypropylene", "polyethylene", "polystyrene", "nylon"],
    "Paper & Cardboard": ["paper", "cardboard", "carton", "occ", "pulp", "kraft"],
    "Glass": ["glass", "cullet"],
    "Electronics & E-Waste": ["electronic", "battery", "batteries", "circuit", "pcb", "cable", "wire", "computer"],
    "Chemicals - Organic": ["solvent", "oil", "lubricant", "grease", "fuel", "petroleum", "hydrocarbon"],
    "Chemicals - Inorganic": ["acid", "alkali", "hydroxide", "chloride", "sulfate", "nitrate", "oxide", "ammonia"],
    "Chemicals - Hazardous": ["toxic", "hazardous", "cyanide", "mercury", "arsenic", "chromium", "asbestos"],
    "Construction & Demolition": ["concrete", "brick", "asphalt", "rubble", "gypsum", "demolition", "construction", "cement"],
    "Organic & Biological": ["organic", "food", "bio", "manure", "compost", "agricultural", "wood", "timber", "biomass"],
    "Textiles": ["textile", "fabric", "cotton", "wool", "leather", "clothing"],
    "Rubber": ["rubber", "tire", "tyre"],
    "Medical & Pharmaceutical": ["medical", "pharmaceutical", "clinical", "hospital"],
    "Other Industrial": [],  # Catch-all
}

# Categorize materials
categorized = defaultdict(list)

for row in materials:
    mat = row["material"]
    mat_lower = mat.lower()
    found = False
    
    for category, keywords in CATEGORY_RULES.items():
        if any(kw in mat_lower for kw in keywords):
            categorized[category].append(mat)
            found = True
            break
    
    if not found:
        categorized["Other Industrial"].append(mat)

# Print summary
print("\nCategory breakdown:")
print("-"*60)
for cat, items in sorted(categorized.items(), key=lambda x: -len(x[1])):
    print(f"  {cat:<30} {len(items):>4} materials")

# Create category table
print("\n" + "="*60)
print("CREATING DATABASE TABLE")
print("="*60)

with get_connection() as conn:
    with conn.cursor() as cur:
        # Create category table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS material_categories (
                id SERIAL PRIMARY KEY,
                category_name VARCHAR(50) NOT NULL,
                material VARCHAR(200) NOT NULL,
                UNIQUE(material)
            )
        """)
        
        # Clear and repopulate
        cur.execute("DELETE FROM material_categories")
        
        for category, items in categorized.items():
            for mat in items:
                cur.execute("""
                    INSERT INTO material_categories (category_name, material)
                    VALUES (%s, %s)
                    ON CONFLICT (material) DO NOTHING
                """, (category, mat))
        
        conn.commit()

print("[OK] material_categories table created")

# Print final stats
cat_count = execute_query("SELECT COUNT(DISTINCT category_name) as c FROM material_categories")[0]["c"]
mat_count = execute_query("SELECT COUNT(*) as c FROM material_categories")[0]["c"]
print(f"\nCategories: {cat_count}")
print(f"Materials mapped: {mat_count}")

# Show dropdown-ready list
print("\n" + "="*60)
print("DROPDOWN OPTIONS")
print("="*60)
categories = execute_query("""
    SELECT category_name, COUNT(*) as count 
    FROM material_categories 
    GROUP BY category_name 
    ORDER BY count DESC
""")
for c in categories:
    print(f"  {c['category_name']:<30} ({c['count']} materials)")
