"""
MATERIAL PROFILE GENERATOR
==========================
Creates rich profiles for each material in the database for Portfolio LLM training.

Output: exports/material_profiles.jsonl
"""
import json
import psycopg2
from pathlib import Path
from decimal import Decimal
from collections import defaultdict
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')

def generate_profiles():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    print("Generating Material Profiles...")
    
    # Get all unique materials with aggregated stats
    cur.execute("""
        SELECT 
            material,
            COUNT(*) as record_count,
            AVG(quantity_tons) as avg_quantity,
            SUM(quantity_tons) as total_quantity,
            array_agg(DISTINCT source_company) FILTER (WHERE source_company IS NOT NULL) as companies,
            array_agg(DISTINCT source_location) FILTER (WHERE source_location IS NOT NULL) as locations,
            array_agg(DISTINCT treatment_method) FILTER (WHERE treatment_method IS NOT NULL) as treatments,
            MIN(year) as earliest_year,
            MAX(year) as latest_year
        FROM waste_listings
        WHERE material IS NOT NULL AND material != ''
        GROUP BY material
        HAVING COUNT(*) >= 2
        ORDER BY total_quantity DESC NULLS LAST
        LIMIT 2000
    """)
    
    profiles = []
    for row in cur.fetchall():
        material = row[0]
        companies = row[4] if row[4] else []
        locations = row[5] if row[5] else []
        treatments = row[6] if row[6] else []
        
        # Categorize the material
        category = categorize_material(material)
        
        # Build profile
        profile = {
            "material": material,
            "category": category,
            "record_count": row[1],
            "avg_quantity_tons": round(row[2], 2) if row[2] else 0,
            "total_quantity_tons": round(row[3], 2) if row[3] else 0,
            "industry_sources": list(set([c[:50] for c in companies[:10]])) if companies else [],
            "geographic_hotspots": list(set(locations[:5])) if locations else [],
            "treatment_methods": list(set(treatments)) if treatments else [],
            "year_range": f"{row[7]}-{row[8]}" if row[7] and row[8] else "Unknown",
            "compatible_receivers": get_compatible_receivers(category),
            "carbon_offset_potential": estimate_carbon_offset(category)
        }
        profiles.append(profile)
    
    # Save to JSONL
    output_path = Path("exports/material_profiles.jsonl")
    with open(output_path, "w", encoding="utf-8") as f:
        for p in profiles:
            f.write(json.dumps(p, ensure_ascii=False, default=decimal_default) + "\n")
    
    print(f"Generated {len(profiles)} material profiles")
    print(f"Saved to: {output_path.absolute()}")
    
    # Also generate Portfolio Q&A training data
    generate_portfolio_qa(profiles)

def categorize_material(material: str) -> str:
    """Categorize material into industry sectors."""
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

def get_compatible_receivers(category: str) -> list:
    """Get compatible receiver industries for symbiosis matching."""
    receivers = {
        "plastics": ["recyclers", "injection molding", "3D printing", "construction materials"],
        "metals": ["foundries", "manufacturing", "electronics", "construction"],
        "organics": ["composting", "anaerobic digestion", "animal feed", "biogas plants"],
        "chemicals": ["chemical processing", "water treatment", "industrial cleaning"],
        "hydrocarbons": ["refineries", "energy recovery", "fuel blending"],
        "fibers": ["paper mills", "cardboard manufacturing", "insulation"],
        "glass": ["glass manufacturing", "construction aggregate", "insulation"],
        "hazardous": ["specialized treatment", "cement kilns", "incineration"],
        "mixed": ["material recovery facilities", "waste-to-energy"]
    }
    return receivers.get(category, ["general recycling"])

def estimate_carbon_offset(category: str) -> str:
    """Estimate CO2 offset potential per ton recycled."""
    offsets = {
        "plastics": "2.5 tons CO2/ton recycled",
        "metals": "4.0 tons CO2/ton recycled",
        "organics": "0.5 tons CO2/ton composted",
        "chemicals": "1.0 tons CO2/ton recovered",
        "hydrocarbons": "3.0 tons CO2/ton recovered",
        "fibers": "1.5 tons CO2/ton recycled",
        "glass": "0.3 tons CO2/ton recycled",
        "hazardous": "0.2 tons CO2/ton treated",
        "mixed": "1.0 tons CO2/ton processed"
    }
    return offsets.get(category, "1.0 tons CO2/ton")

def generate_portfolio_qa(profiles: list):
    """Generate Q&A training data for Portfolio LLM."""
    qa_pairs = []
    
    # Group profiles by category for portfolio generation
    by_category = defaultdict(list)
    for p in profiles:
        by_category[p["category"]].append(p)
    
    # Generate diverse Q&A pairs
    industries = ["food processing", "automotive manufacturing", "chemical production", 
                  "textile manufacturing", "electronics assembly", "construction"]
    locations = ["Germany", "USA", "France", "Italy", "United Kingdom", "Netherlands"]
    
    for industry in industries:
        for location in locations:
            relevant_cats = get_industry_categories(industry)
            relevant_materials = []
            for cat in relevant_cats:
                relevant_materials.extend(by_category.get(cat, [])[:3])
            
            if relevant_materials:
                question = f"I'm a {industry} company in {location} looking to optimize my waste streams. What symbiotic opportunities exist?"
                
                # Build answer from real data
                opportunities = []
                total_potential = 0
                for mat in relevant_materials[:5]:
                    receivers = mat.get("compatible_receivers", [])[:2]
                    avg_qty = mat.get("avg_quantity_tons", 100)
                    co2 = mat.get("carbon_offset_potential", "1.0 tons CO2/ton")
                    
                    opportunities.append({
                        "material": mat["material"][:40],
                        "receivers": receivers,
                        "estimated_volume": f"{avg_qty:.0f} tons/year",
                        "carbon_benefit": co2
                    })
                    total_potential += avg_qty
                
                answer = f"Based on industrial symbiosis data for {industry} in {location}, here are your top opportunities:\n\n"
                for i, opp in enumerate(opportunities, 1):
                    answer += f"{i}. **{opp['material']}**: Partner with {', '.join(opp['receivers'])} ({opp['estimated_volume']}, {opp['carbon_benefit']})\n"
                answer += f"\nTotal symbiotic potential: ~{total_potential:.0f} tons/year with significant CO2 reduction."
                
                qa_pairs.append({
                    "prompt": question,
                    "completion": answer
                })
    
    # Save Q&A pairs
    output_path = Path("exports/portfolio_qa_training.jsonl")
    with open(output_path, "w", encoding="utf-8") as f:
        for qa in qa_pairs:
            f.write(json.dumps(qa, ensure_ascii=False, default=decimal_default) + "\n")
    
    print(f"Generated {len(qa_pairs)} Portfolio Q&A training pairs")
    print(f"Saved to: {output_path.absolute()}")

def get_industry_categories(industry: str) -> list:
    """Map industry to relevant waste categories."""
    mapping = {
        "food processing": ["organics", "plastics", "fibers"],
        "automotive manufacturing": ["metals", "plastics", "chemicals"],
        "chemical production": ["chemicals", "hydrocarbons", "hazardous"],
        "textile manufacturing": ["fibers", "chemicals", "plastics"],
        "electronics assembly": ["metals", "plastics", "hazardous"],
        "construction": ["mixed", "metals", "fibers", "glass"]
    }
    return mapping.get(industry, ["mixed"])

if __name__ == "__main__":
    generate_profiles()
