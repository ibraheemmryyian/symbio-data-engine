"""
Build two-tier industry pricing with:
- 8 Parent categories (dropdown tier 1)
- 20 Sub-industries (dropdown tier 2, optional)
- Smart defaults (blended averages when sub-industry blank)
"""
import json
from datetime import datetime
from pathlib import Path

print("="*70)
print("BUILDING TWO-TIER INDUSTRY PRICING")
print("="*70)

# Base material prices (from research + CSR extraction)
MATERIAL_PRICES = {
    # Metals
    "scrap_steel": {"price_low": 150, "price_high": 350, "unit": "usd/ton", "co2_factor": 1.74, "recyclability": "high"},
    "scrap_aluminum": {"price_low": 800, "price_high": 1800, "unit": "usd/ton", "co2_factor": 9.7, "recyclability": "high"},
    "scrap_copper": {"price_low": 4000, "price_high": 8000, "unit": "usd/ton", "co2_factor": 2.6, "recyclability": "high"},
    "mixed_metals": {"price_low": 100, "price_high": 400, "unit": "usd/ton", "co2_factor": 3.0, "recyclability": "high"},
    "precious_metals": {"price_low": 10000, "price_high": 50000, "unit": "usd/ton", "co2_factor": 12.0, "recyclability": "high"},
    
    # Plastics
    "pet_plastic": {"price_low": 200, "price_high": 500, "unit": "usd/ton", "co2_factor": 2.0, "recyclability": "high"},
    "hdpe_plastic": {"price_low": 250, "price_high": 600, "unit": "usd/ton", "co2_factor": 1.9, "recyclability": "high"},
    "mixed_plastics": {"price_low": 50, "price_high": 200, "unit": "usd/ton", "co2_factor": 2.2, "recyclability": "medium"},
    "pvc_plastic": {"price_low": 100, "price_high": 300, "unit": "usd/ton", "co2_factor": 2.4, "recyclability": "low"},
    
    # Paper & Cardboard
    "corrugated_cardboard": {"price_low": 80, "price_high": 200, "unit": "usd/ton", "co2_factor": 0.9, "recyclability": "high"},
    "mixed_paper": {"price_low": 40, "price_high": 120, "unit": "usd/ton", "co2_factor": 0.8, "recyclability": "medium"},
    "office_paper": {"price_low": 100, "price_high": 250, "unit": "usd/ton", "co2_factor": 0.7, "recyclability": "high"},
    
    # Glass
    "clear_glass": {"price_low": 30, "price_high": 80, "unit": "usd/ton", "co2_factor": 0.5, "recyclability": "high"},
    "mixed_glass": {"price_low": 15, "price_high": 50, "unit": "usd/ton", "co2_factor": 0.4, "recyclability": "medium"},
    
    # Organic
    "food_waste": {"price_low": -20, "price_high": 50, "unit": "usd/ton", "co2_factor": 0.4, "recyclability": "high"},
    "yard_waste": {"price_low": -10, "price_high": 30, "unit": "usd/ton", "co2_factor": 0.2, "recyclability": "high"},
    "agricultural_waste": {"price_low": -15, "price_high": 40, "unit": "usd/ton", "co2_factor": 0.3, "recyclability": "high"},
    
    # Construction & Demolition
    "concrete_rubble": {"price_low": -5, "price_high": 15, "unit": "usd/ton", "co2_factor": 0.1, "recyclability": "medium"},
    "wood_waste": {"price_low": 10, "price_high": 60, "unit": "usd/ton", "co2_factor": 0.3, "recyclability": "high"},
    "asphalt": {"price_low": 5, "price_high": 30, "unit": "usd/ton", "co2_factor": 0.15, "recyclability": "high"},
    "gypsum": {"price_low": -10, "price_high": 20, "unit": "usd/ton", "co2_factor": 0.1, "recyclability": "medium"},
    
    # Industrial
    "fly_ash": {"price_low": 5, "price_high": 50, "unit": "usd/ton", "co2_factor": 0.1, "recyclability": "high"},
    "slag": {"price_low": 10, "price_high": 40, "unit": "usd/ton", "co2_factor": 0.2, "recyclability": "high"},
    "used_oil": {"price_low": 50, "price_high": 200, "unit": "usd/ton", "co2_factor": 2.5, "recyclability": "high"},
    "solvents": {"price_low": 100, "price_high": 500, "unit": "usd/ton", "co2_factor": 2.0, "recyclability": "medium"},
    "spent_catalysts": {"price_low": 500, "price_high": 5000, "unit": "usd/ton", "co2_factor": 8.0, "recyclability": "high"},
    
    # Mining
    "tailings": {"price_low": 5, "price_high": 25, "unit": "usd/ton", "co2_factor": 0.1, "recyclability": "medium"},
    "waste_rock": {"price_low": 2, "price_high": 15, "unit": "usd/ton", "co2_factor": 0.05, "recyclability": "low"},
    "mining_chemicals": {"price_low": 50, "price_high": 300, "unit": "usd/ton", "co2_factor": 1.5, "recyclability": "medium"},
    
    # Oil & Gas
    "drilling_waste": {"price_low": -50, "price_high": 20, "unit": "usd/ton", "co2_factor": 0.5, "recyclability": "low"},
    "produced_water": {"price_low": -30, "price_high": 10, "unit": "usd/ton", "co2_factor": 0.2, "recyclability": "medium"},
    "tank_bottoms": {"price_low": -20, "price_high": 100, "unit": "usd/ton", "co2_factor": 1.0, "recyclability": "medium"},
    
    # Electronics
    "e_waste": {"price_low": 200, "price_high": 2000, "unit": "usd/ton", "co2_factor": 20.0, "recyclability": "high"},
    "circuit_boards": {"price_low": 3000, "price_high": 15000, "unit": "usd/ton", "co2_factor": 25.0, "recyclability": "high"},
    "batteries": {"price_low": 100, "price_high": 1000, "unit": "usd/ton", "co2_factor": 5.0, "recyclability": "medium"},
    
    # Textiles
    "textile_waste": {"price_low": 50, "price_high": 300, "unit": "usd/ton", "co2_factor": 1.5, "recyclability": "medium"},
    "fiber_waste": {"price_low": 100, "price_high": 500, "unit": "usd/ton", "co2_factor": 1.2, "recyclability": "high"},
    
    # Healthcare
    "medical_waste": {"price_low": -500, "price_high": -100, "unit": "usd/ton", "co2_factor": 3.0, "recyclability": "low"},
    "pharmaceutical_waste": {"price_low": -300, "price_high": 50, "unit": "usd/ton", "co2_factor": 2.0, "recyclability": "low"},
    
    # Tires & Rubber
    "scrap_tires": {"price_low": -50, "price_high": 100, "unit": "usd/ton", "co2_factor": 2.8, "recyclability": "medium"},
    "rubber_waste": {"price_low": 50, "price_high": 200, "unit": "usd/ton", "co2_factor": 2.5, "recyclability": "medium"},
}

# Sub-industry profiles (20 specific industries)
SUB_INDUSTRIES = {
    "Manufacturing - General": {
        "materials": ["scrap_steel", "mixed_plastics", "corrugated_cardboard", "used_oil", "solvents"],
        "waste_profile": [
            {"material": "scrap_steel", "percent": 35},
            {"material": "corrugated_cardboard", "percent": 25},
            {"material": "mixed_plastics", "percent": 20},
            {"material": "used_oil", "percent": 12},
            {"material": "solvents", "percent": 8}
        ],
        "baseline_diversion": 0.40, "max_diversion": 0.75
    },
    "Automotive": {
        "materials": ["scrap_steel", "scrap_aluminum", "mixed_plastics", "used_oil", "scrap_tires"],
        "waste_profile": [
            {"material": "scrap_steel", "percent": 45},
            {"material": "scrap_aluminum", "percent": 20},
            {"material": "mixed_plastics", "percent": 15},
            {"material": "used_oil", "percent": 12},
            {"material": "scrap_tires", "percent": 8}
        ],
        "baseline_diversion": 0.50, "max_diversion": 0.85
    },
    "Electronics": {
        "materials": ["e_waste", "circuit_boards", "mixed_plastics", "scrap_copper", "batteries"],
        "waste_profile": [
            {"material": "e_waste", "percent": 30},
            {"material": "circuit_boards", "percent": 25},
            {"material": "mixed_plastics", "percent": 25},
            {"material": "scrap_copper", "percent": 15},
            {"material": "batteries", "percent": 5}
        ],
        "baseline_diversion": 0.35, "max_diversion": 0.70
    },
    "Mining": {
        "materials": ["scrap_steel", "tailings", "waste_rock", "mining_chemicals", "mixed_metals"],
        "waste_profile": [
            {"material": "tailings", "percent": 45},
            {"material": "waste_rock", "percent": 35},
            {"material": "scrap_steel", "percent": 12},
            {"material": "mining_chemicals", "percent": 5},
            {"material": "mixed_metals", "percent": 3}
        ],
        "baseline_diversion": 0.30, "max_diversion": 0.60
    },
    "Oil & Gas": {
        "materials": ["drilling_waste", "produced_water", "tank_bottoms", "spent_catalysts", "used_oil"],
        "waste_profile": [
            {"material": "drilling_waste", "percent": 40},
            {"material": "produced_water", "percent": 30},
            {"material": "tank_bottoms", "percent": 15},
            {"material": "used_oil", "percent": 10},
            {"material": "spent_catalysts", "percent": 5}
        ],
        "baseline_diversion": 0.25, "max_diversion": 0.55
    },
    "Power & Utilities": {
        "materials": ["fly_ash", "slag", "scrap_steel", "used_oil", "batteries"],
        "waste_profile": [
            {"material": "fly_ash", "percent": 45},
            {"material": "slag", "percent": 30},
            {"material": "scrap_steel", "percent": 15},
            {"material": "used_oil", "percent": 7},
            {"material": "batteries", "percent": 3}
        ],
        "baseline_diversion": 0.50, "max_diversion": 0.80
    },
    "Food Processing": {
        "materials": ["food_waste", "corrugated_cardboard", "mixed_plastics", "clear_glass", "used_oil"],
        "waste_profile": [
            {"material": "food_waste", "percent": 50},
            {"material": "corrugated_cardboard", "percent": 25},
            {"material": "mixed_plastics", "percent": 15},
            {"material": "clear_glass", "percent": 7},
            {"material": "used_oil", "percent": 3}
        ],
        "baseline_diversion": 0.35, "max_diversion": 0.80
    },
    "Agriculture": {
        "materials": ["agricultural_waste", "food_waste", "mixed_plastics", "used_oil", "scrap_steel"],
        "waste_profile": [
            {"material": "agricultural_waste", "percent": 60},
            {"material": "food_waste", "percent": 15},
            {"material": "mixed_plastics", "percent": 12},
            {"material": "used_oil", "percent": 8},
            {"material": "scrap_steel", "percent": 5}
        ],
        "baseline_diversion": 0.40, "max_diversion": 0.85
    },
    "Construction": {
        "materials": ["concrete_rubble", "wood_waste", "scrap_steel", "gypsum", "asphalt"],
        "waste_profile": [
            {"material": "concrete_rubble", "percent": 40},
            {"material": "wood_waste", "percent": 25},
            {"material": "scrap_steel", "percent": 20},
            {"material": "gypsum", "percent": 10},
            {"material": "asphalt", "percent": 5}
        ],
        "baseline_diversion": 0.45, "max_diversion": 0.80
    },
    "Cement & Concrete": {
        "materials": ["concrete_rubble", "fly_ash", "slag", "scrap_steel", "used_oil"],
        "waste_profile": [
            {"material": "concrete_rubble", "percent": 40},
            {"material": "fly_ash", "percent": 25},
            {"material": "slag", "percent": 20},
            {"material": "scrap_steel", "percent": 10},
            {"material": "used_oil", "percent": 5}
        ],
        "baseline_diversion": 0.60, "max_diversion": 0.90
    },
    "Steel & Metals": {
        "materials": ["scrap_steel", "slag", "scrap_aluminum", "mixed_metals", "used_oil"],
        "waste_profile": [
            {"material": "scrap_steel", "percent": 50},
            {"material": "slag", "percent": 25},
            {"material": "scrap_aluminum", "percent": 12},
            {"material": "mixed_metals", "percent": 8},
            {"material": "used_oil", "percent": 5}
        ],
        "baseline_diversion": 0.65, "max_diversion": 0.90
    },
    "Healthcare": {
        "materials": ["medical_waste", "pharmaceutical_waste", "mixed_plastics", "corrugated_cardboard", "clear_glass"],
        "waste_profile": [
            {"material": "medical_waste", "percent": 35},
            {"material": "pharmaceutical_waste", "percent": 20},
            {"material": "mixed_plastics", "percent": 25},
            {"material": "corrugated_cardboard", "percent": 15},
            {"material": "clear_glass", "percent": 5}
        ],
        "baseline_diversion": 0.20, "max_diversion": 0.45
    },
    "Pharmaceutical": {
        "materials": ["pharmaceutical_waste", "solvents", "mixed_plastics", "corrugated_cardboard", "clear_glass"],
        "waste_profile": [
            {"material": "solvents", "percent": 35},
            {"material": "pharmaceutical_waste", "percent": 25},
            {"material": "mixed_plastics", "percent": 20},
            {"material": "corrugated_cardboard", "percent": 12},
            {"material": "clear_glass", "percent": 8}
        ],
        "baseline_diversion": 0.25, "max_diversion": 0.55
    },
    "Retail & Wholesale": {
        "materials": ["corrugated_cardboard", "mixed_plastics", "mixed_paper", "food_waste", "textile_waste"],
        "waste_profile": [
            {"material": "corrugated_cardboard", "percent": 45},
            {"material": "mixed_plastics", "percent": 25},
            {"material": "mixed_paper", "percent": 15},
            {"material": "food_waste", "percent": 10},
            {"material": "textile_waste", "percent": 5}
        ],
        "baseline_diversion": 0.45, "max_diversion": 0.80
    },
    "Hospitality": {
        "materials": ["food_waste", "corrugated_cardboard", "mixed_plastics", "clear_glass", "textile_waste"],
        "waste_profile": [
            {"material": "food_waste", "percent": 50},
            {"material": "corrugated_cardboard", "percent": 20},
            {"material": "mixed_plastics", "percent": 15},
            {"material": "clear_glass", "percent": 10},
            {"material": "textile_waste", "percent": 5}
        ],
        "baseline_diversion": 0.30, "max_diversion": 0.70
    },
    "Transportation & Logistics": {
        "materials": ["scrap_steel", "used_oil", "scrap_tires", "corrugated_cardboard", "batteries"],
        "waste_profile": [
            {"material": "scrap_steel", "percent": 35},
            {"material": "used_oil", "percent": 25},
            {"material": "scrap_tires", "percent": 20},
            {"material": "corrugated_cardboard", "percent": 15},
            {"material": "batteries", "percent": 5}
        ],
        "baseline_diversion": 0.40, "max_diversion": 0.70
    },
    "Chemical": {
        "materials": ["solvents", "mixed_plastics", "spent_catalysts", "used_oil", "scrap_steel"],
        "waste_profile": [
            {"material": "solvents", "percent": 35},
            {"material": "mixed_plastics", "percent": 25},
            {"material": "spent_catalysts", "percent": 15},
            {"material": "used_oil", "percent": 15},
            {"material": "scrap_steel", "percent": 10}
        ],
        "baseline_diversion": 0.30, "max_diversion": 0.65
    },
    "Plastics & Rubber": {
        "materials": ["mixed_plastics", "rubber_waste", "scrap_tires", "solvents", "corrugated_cardboard"],
        "waste_profile": [
            {"material": "mixed_plastics", "percent": 45},
            {"material": "rubber_waste", "percent": 25},
            {"material": "scrap_tires", "percent": 15},
            {"material": "solvents", "percent": 10},
            {"material": "corrugated_cardboard", "percent": 5}
        ],
        "baseline_diversion": 0.40, "max_diversion": 0.75
    },
    "Pulp & Paper": {
        "materials": ["mixed_paper", "wood_waste", "solvents", "used_oil", "scrap_steel"],
        "waste_profile": [
            {"material": "mixed_paper", "percent": 45},
            {"material": "wood_waste", "percent": 30},
            {"material": "solvents", "percent": 12},
            {"material": "used_oil", "percent": 8},
            {"material": "scrap_steel", "percent": 5}
        ],
        "baseline_diversion": 0.55, "max_diversion": 0.85
    },
    "Textiles & Apparel": {
        "materials": ["textile_waste", "fiber_waste", "mixed_plastics", "corrugated_cardboard", "solvents"],
        "waste_profile": [
            {"material": "textile_waste", "percent": 45},
            {"material": "fiber_waste", "percent": 25},
            {"material": "mixed_plastics", "percent": 15},
            {"material": "corrugated_cardboard", "percent": 10},
            {"material": "solvents", "percent": 5}
        ],
        "baseline_diversion": 0.35, "max_diversion": 0.70
    },
}

# Parent categories with their sub-industries
PARENT_CATEGORIES = {
    "Manufacturing": {
        "sub_industries": ["Manufacturing - General", "Automotive", "Electronics"],
        "description": "General manufacturing, automotive, electronics production"
    },
    "Energy & Mining": {
        "sub_industries": ["Mining", "Oil & Gas", "Power & Utilities"],
        "description": "Mining, oil & gas extraction, power generation"
    },
    "Food & Agriculture": {
        "sub_industries": ["Food Processing", "Agriculture"],
        "description": "Food production, farming, agriculture"
    },
    "Construction & Materials": {
        "sub_industries": ["Construction", "Cement & Concrete", "Steel & Metals"],
        "description": "Construction, building materials, metals processing"
    },
    "Healthcare & Pharma": {
        "sub_industries": ["Healthcare", "Pharmaceutical"],
        "description": "Hospitals, clinics, pharmaceutical manufacturing"
    },
    "Commercial & Services": {
        "sub_industries": ["Retail & Wholesale", "Hospitality", "Transportation & Logistics"],
        "description": "Retail, hotels, restaurants, logistics"
    },
    "Chemical & Plastics": {
        "sub_industries": ["Chemical", "Plastics & Rubber"],
        "description": "Chemical manufacturing, plastics, rubber products"
    },
    "Paper & Textiles": {
        "sub_industries": ["Pulp & Paper", "Textiles & Apparel"],
        "description": "Paper manufacturing, textile and apparel production"
    }
}

# Volume tiers
VOLUME_TIERS = {
    "small": {"range": "0-1,000 tons/year", "multiplier": 500, "description": "Small operations"},
    "medium": {"range": "1,000-10,000 tons/year", "multiplier": 5000, "description": "Medium operations"},
    "large": {"range": "10,000-100,000 tons/year", "multiplier": 50000, "description": "Large operations"},
    "enterprise": {"range": "100,000+ tons/year", "multiplier": 200000, "description": "Enterprise scale"}
}

# Regional modifiers
REGIONAL_MODIFIERS = {
    "north_america": {"modifier": 1.00, "countries": ["USA", "Canada", "Mexico"], "label": "North America"},
    "europe": {"modifier": 1.15, "countries": ["Germany", "UK", "France", "Italy", "Spain"], "label": "Europe"},
    "mena": {"modifier": 0.85, "countries": ["UAE", "Saudi Arabia", "Qatar", "Kuwait", "Egypt"], "label": "Middle East & Africa"},
    "asia_pacific": {"modifier": 0.95, "countries": ["China", "Japan", "South Korea", "Australia", "India"], "label": "Asia Pacific"},
    "latin_america": {"modifier": 0.75, "countries": ["Brazil", "Argentina", "Chile", "Colombia"], "label": "Latin America"},
    "africa": {"modifier": 0.65, "countries": ["South Africa", "Nigeria", "Kenya", "Morocco"], "label": "Africa"}
}


def calculate_blended_default(parent_key, sub_industries_list):
    """Calculate a blended average waste profile for parent category."""
    all_materials = {}
    total_diversion_base = 0
    total_diversion_max = 0
    count = 0
    
    for sub_key in sub_industries_list:
        if sub_key not in SUB_INDUSTRIES:
            continue
        sub = SUB_INDUSTRIES[sub_key]
        count += 1
        total_diversion_base += sub["baseline_diversion"]
        total_diversion_max += sub["max_diversion"]
        
        for item in sub["waste_profile"]:
            mat = item["material"]
            pct = item["percent"]
            if mat in all_materials:
                all_materials[mat] += pct
            else:
                all_materials[mat] = pct
    
    if count == 0:
        return None
    
    # Average the percentages and normalize to 100%
    for mat in all_materials:
        all_materials[mat] /= count
    
    total_pct = sum(all_materials.values())
    normalized = []
    for mat, pct in sorted(all_materials.items(), key=lambda x: -x[1])[:5]:
        normalized.append({
            "material": mat,
            "percent": round(pct * 100 / total_pct, 1)
        })
    
    return {
        "materials": list(all_materials.keys())[:8],
        "waste_profile": normalized,
        "baseline_diversion": round(total_diversion_base / count, 2),
        "max_diversion": round(total_diversion_max / count, 2),
        "is_blended": True
    }

# Build the output
output = {
    "version": "3.0",
    "generated": datetime.utcnow().isoformat() + "Z",
    "source": "symbio_data_engine",
    "structure": "two_tier",
    "materials": MATERIAL_PRICES,
    "volume_tiers": VOLUME_TIERS,
    "regional_modifiers": REGIONAL_MODIFIERS,
    "parent_categories": {},
    "sub_industries": {}
}

# Build parent categories with blended defaults
for parent_key, parent_data in PARENT_CATEGORIES.items():
    blended = calculate_blended_default(parent_key, parent_data["sub_industries"])
    
    output["parent_categories"][parent_key] = {
        "description": parent_data["description"],
        "sub_industries": parent_data["sub_industries"],
        "default": blended  # Smart default when sub-industry not selected
    }

# Build sub-industries with full material data
for sub_key, sub_data in SUB_INDUSTRIES.items():
    materials_data = {}
    for mat_key in sub_data["materials"]:
        if mat_key in MATERIAL_PRICES:
            materials_data[mat_key] = MATERIAL_PRICES[mat_key].copy()
    
    output["sub_industries"][sub_key] = {
        "materials": materials_data,
        "waste_profile": sub_data["waste_profile"],
        "baseline_diversion_rate": sub_data["baseline_diversion"],
        "max_diversion_rate": sub_data["max_diversion"]
    }

# Save
output_path = Path("exports/industry_pricing.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2)

print(f"\n[OK] Generated: {output_path}")
print(f"Parent categories: {len(output['parent_categories'])}")
print(f"Sub-industries: {len(output['sub_industries'])}")
print(f"Materials: {len(output['materials'])}")
print(f"Volume tiers: {len(output['volume_tiers'])}")
print(f"Regions: {len(output['regional_modifiers'])}")

print("\n" + "="*70)
print("PARENT CATEGORIES (Dropdown Tier 1)")
print("="*70)
for parent, data in output["parent_categories"].items():
    subs = ", ".join(data["sub_industries"])
    print(f"\n{parent}:")
    print(f"  Sub-industries: {subs}")
    if data["default"]:
        print(f"  Default diversion: {data['default']['baseline_diversion']:.0%} - {data['default']['max_diversion']:.0%}")
