# Report Module Integration Guide

## Files to Copy

Copy these 3 files from `symbio_data_engine/exports/` to your report module:

```
industry_pricing.json   (main pricing data)
co2_factors.json        (carbon reduction factors)
regional_modifiers.json (price modifiers by region)
```

---

## Quick Integration

### 1. Load the Data

```python
import json

# Load once at startup
with open("data/industry_pricing.json") as f:
    PRICING = json.load(f)
```

### 2. Get Materials for Industry

```python
def get_industry_materials(industry_name, region="north_america", volume_tier="medium"):
    """
    Get materials with adjusted prices for an industry.
    
    Args:
        industry_name: e.g. "Mining & Minerals"
        region: "north_america", "europe", "mena", "asia_pacific", "latin_america", "africa"
        volume_tier: "small", "medium", "large", "enterprise"
    
    Returns:
        dict of materials with price_low, price_high, annual_volume
    """
    industry = PRICING["industries"].get(industry_name)
    if not industry:
        return {}
    
    region_mod = PRICING["regional_modifiers"].get(region, {}).get("modifier", 1.0)
    volume_mult = PRICING["volume_tiers"].get(volume_tier, {}).get("multiplier", 5000)
    
    result = {}
    for material_key, material_data in industry["materials"].items():
        result[material_key] = {
            "price_low": material_data["price_low"] * region_mod,
            "price_high": material_data["price_high"] * region_mod,
            "unit": material_data["unit"],
            "co2_factor": material_data["co2_factor"],
            "recyclability": material_data["recyclability"]
        }
    
    return result, industry["waste_profile"], volume_mult
```

### 3. Calculate Annual Value

```python
def calculate_annual_value(industry_name, region, volume_tier):
    """Calculate total annual valorization potential."""
    materials, waste_profile, base_volume = get_industry_materials(
        industry_name, region, volume_tier
    )
    
    total_value = 0
    total_co2 = 0
    
    for item in waste_profile:
        material_key = item["material"]
        percent = item["percent"] / 100
        volume = base_volume * percent
        
        if material_key in materials:
            mat = materials[material_key]
            avg_price = (mat["price_low"] + mat["price_high"]) / 2
            value = volume * avg_price
            co2_saved = volume * mat["co2_factor"]
            
            total_value += value
            total_co2 += co2_saved
    
    return {
        "annual_value_usd": total_value,
        "co2_reduction_tonnes": total_co2,
        "base_volume_tonnes": base_volume
    }
```

---

## Available Industries (20)

1. Mining & Minerals
2. Oil & Gas
3. Manufacturing - General
4. Food & Beverage
5. Chemical Manufacturing
6. Automotive Manufacturing
7. Pharmaceutical
8. Electronics Manufacturing
9. Construction
10. Agriculture
11. Power & Utilities
12. Healthcare & Medical
13. Retail & Wholesale
14. Hospitality & Tourism
15. Transportation & Logistics
16. Textiles & Apparel
17. Pulp & Paper
18. Cement & Concrete
19. Steel & Metal Processing
20. Plastics & Rubber

---

## Volume Tiers

| Tier | Range | Base Volume |
|------|-------|-------------|
| small | 0-1,000 tons/year | 500 tons |
| medium | 1,000-10,000 tons/year | 5,000 tons |
| large | 10,000-100,000 tons/year | 50,000 tons |
| enterprise | 100,000+ tons/year | 200,000 tons |

---

## Regions

| Region | Price Modifier |
|--------|----------------|
| north_america | 1.00x (baseline) |
| europe | 1.15x |
| mena | 0.85x |
| asia_pacific | 0.95x |
| latin_america | 0.75x |
| africa | 0.65x |
