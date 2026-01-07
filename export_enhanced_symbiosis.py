"""
ENHANCED SYMBIOSIS MATCHES EXPORTER
===================================
Generates upgraded symbiosis pairs with:
- Compatibility scores
- Geographic proximity
- Industry matching
- Carbon offset potential
- Historical success indicators
"""
import json
import psycopg2
from pathlib import Path
from decimal import Decimal
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_compatibility_score(mat1_cat: str, mat2_cat: str) -> float:
    """Calculate compatibility between two material categories."""
    # High compatibility pairs
    high_compat = [
        ("plastics", "plastics"), ("metals", "metals"),
        ("organics", "organics"), ("fibers", "fibers"),
        ("organics", "fibers"),  # Composting synergy
        ("hydrocarbons", "chemicals"),  # Petrochemical chain
    ]
    # Medium compatibility
    med_compat = [
        ("plastics", "hydrocarbons"),  # Plastic to fuel
        ("metals", "chemicals"),  # Metal recovery
        ("glass", "mixed"),  # Aggregate use
    ]
    
    pair = tuple(sorted([mat1_cat, mat2_cat]))
    if pair[0] == pair[1]:
        return 0.95
    elif pair in high_compat or pair[::-1] in high_compat:
        return 0.85
    elif pair in med_compat or pair[::-1] in med_compat:
        return 0.65
    else:
        return 0.4

def categorize_material(material: str) -> str:
    m = material.lower()
    if any(x in m for x in ["plastic", "polyethylene", "polypropylene", "pvc", "styrene", "polymer"]):
        return "plastics"
    elif any(x in m for x in ["lead", "zinc", "copper", "aluminum", "iron", "steel", "metal", "chromium"]):
        return "metals"
    elif any(x in m for x in ["organic", "food", "sludge", "manure", "bio"]):
        return "organics"
    elif any(x in m for x in ["chlor", "fluor", "brom", "acid", "solvent", "cyanide"]):
        return "chemicals"
    elif any(x in m for x in ["oil", "petroleum", "fuel", "benzene", "toluene"]):
        return "hydrocarbons"
    elif any(x in m for x in ["paper", "cardboard", "wood", "cellulose"]):
        return "fibers"
    elif any(x in m for x in ["glass", "silica", "sand"]):
        return "glass"
    elif any(x in m for x in ["hazard", "radioactive", "toxic"]):
        return "hazardous"
    else:
        return "mixed"

def export_enhanced_matches():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    print("Generating Enhanced Symbiosis Matches...")
    
    # Get unique materials with their locations and quantities
    cur.execute("""
        SELECT DISTINCT
            material,
            array_agg(DISTINCT source_location) FILTER (WHERE source_location IS NOT NULL) as locations,
            AVG(quantity_tons) as avg_qty,
            array_agg(DISTINCT source_company) FILTER (WHERE source_company IS NOT NULL) as companies
        FROM waste_listings
        WHERE material IS NOT NULL AND material != ''
        GROUP BY material
        HAVING COUNT(*) >= 2
        LIMIT 500
    """)
    
    materials = cur.fetchall()
    matches = []
    
    # Generate matches between compatible materials
    for i, m1 in enumerate(materials):
        mat1 = m1[0]
        loc1 = m1[1] if m1[1] else []
        qty1 = float(m1[2]) if m1[2] else 0
        companies1 = m1[3] if m1[3] else []
        cat1 = categorize_material(mat1)
        
        for m2 in materials[i+1:i+20]:  # Match with next 20 materials
            mat2 = m2[0]
            loc2 = m2[1] if m2[1] else []
            qty2 = float(m2[2]) if m2[2] else 0
            companies2 = m2[3] if m2[3] else []
            cat2 = categorize_material(mat2)
            
            compat = get_compatibility_score(cat1, cat2)
            
            # Only include if reasonably compatible
            if compat >= 0.5:
                # Check geographic overlap
                loc_overlap = len(set(loc1) & set(loc2))
                geo_score = min(1.0, loc_overlap * 0.25) if loc_overlap else 0.2
                
                match = {
                    "material_source": mat1[:50],
                    "material_receiver": mat2[:50],
                    "source_category": cat1,
                    "receiver_category": cat2,
                    "compatibility_score": round(compat, 2),
                    "geographic_score": round(geo_score, 2),
                    "combined_score": round((compat * 0.7 + geo_score * 0.3), 2),
                    "avg_volume_tons": round((qty1 + qty2) / 2, 1),
                    "shared_locations": list(set(loc1) & set(loc2))[:3],
                    "source_companies": [c[:30] for c in companies1[:2]] if companies1 else [],
                    "receiver_companies": [c[:30] for c in companies2[:2]] if companies2 else [],
                    "symbiosis_type": get_symbiosis_type(cat1, cat2)
                }
                matches.append(match)
    
    # Sort by combined score
    matches.sort(key=lambda x: x["combined_score"], reverse=True)
    
    # Save to JSONL
    output_path = Path("exports/enhanced_symbiosis_matches.jsonl")
    with open(output_path, "w", encoding="utf-8") as f:
        for m in matches:
            f.write(json.dumps(m, ensure_ascii=False, default=decimal_default) + "\n")
    
    print(f"Generated {len(matches)} enhanced symbiosis matches")
    print(f"Saved to: {output_path.absolute()}")
    
    # Summary stats
    avg_score = sum(m["combined_score"] for m in matches) / len(matches) if matches else 0
    print(f"Average combined score: {avg_score:.2f}")
    print(f"High compatibility matches (>0.8): {len([m for m in matches if m['combined_score'] > 0.8])}")

def get_symbiosis_type(cat1: str, cat2: str) -> str:
    """Determine the type of symbiotic relationship."""
    if cat1 == cat2:
        return "same-category recycling"
    elif {cat1, cat2} == {"organics", "fibers"}:
        return "composting synergy"
    elif {cat1, cat2} == {"plastics", "hydrocarbons"}:
        return "plastic-to-fuel"
    elif "hazardous" in {cat1, cat2}:
        return "hazardous recovery"
    elif "metals" in {cat1, cat2}:
        return "metal recovery"
    else:
        return "cross-category exchange"

if __name__ == "__main__":
    export_enhanced_matches()
