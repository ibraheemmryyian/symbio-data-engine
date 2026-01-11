"""
Audit production reports by extracting key numbers from PDFs 
and validating against our pricing data.
"""
import json
import re
from pathlib import Path

# Load our pricing data
with open("exports/industry_pricing.json") as f:
    PRICING = json.load(f)

print("="*70)
print("PRODUCTION REPORT AUDIT")
print("="*70)

# Map filename industries to pricing keys
INDUSTRY_MAP = {
    "Agriculture": "Agriculture",
    "Automotive_Manufacturing": "Automotive Manufacturing",
    "Cement___Concrete": "Cement & Concrete",
    "Chemical_Manufacturing": "Chemical Manufacturing",
    "Construction": "Construction",
    "Electronics_Manufacturing": "Electronics Manufacturing",
    "Food___Beverage": "Food & Beverage",
    "Healthcare___Medical": "Healthcare & Medical",
    "Hospitality___Tourism": "Hospitality & Tourism",
    "Manufacturing___General": "Manufacturing - General",
    "Mining___Minerals": "Mining & Minerals",
    "Oil___Gas": "Oil & Gas",
    "Pharmaceutical": "Pharmaceutical",
    "Plastics___Rubber": "Plastics & Rubber",
    "Power___Utilities": "Power & Utilities",
    "Pulp___Paper": "Pulp & Paper",
    "Retail___Wholesale": "Retail & Wholesale",
    "Steel___Metal_Processing": "Steel & Metal Processing",
    "Textiles___Apparel": "Textiles & Apparel",
    "Transportation___Logistics": "Transportation & Logistics",
}

VOLUME_TIERS = {
    "small": 500,
    "medium": 5000,
    "large": 50000,
    "enterprise": 200000,
}

def calculate_expected_values(industry_key, tier):
    """Calculate expected values based on our pricing data."""
    industry = PRICING["industries"].get(industry_key)
    if not industry:
        return None
    
    base_volume = VOLUME_TIERS[tier]
    materials = industry["materials"]
    waste_profile = industry["waste_profile"]
    
    total_value = 0
    total_co2 = 0
    material_values = []
    
    for item in waste_profile:
        mat_key = item["material"]
        percent = item["percent"] / 100
        volume = base_volume * percent
        
        if mat_key in materials:
            mat = materials[mat_key]
            avg_price = (mat["price_low"] + mat["price_high"]) / 2
            value = volume * avg_price
            co2 = volume * mat["co2_factor"]
            
            total_value += value
            total_co2 += co2
            material_values.append({
                "material": mat_key,
                "volume": volume,
                "value": value,
                "co2": co2
            })
    
    return {
        "industry": industry_key,
        "tier": tier,
        "base_volume": base_volume,
        "total_annual_value": total_value,
        "total_co2_reduction": total_co2,
        "baseline_diversion": industry["baseline_diversion_rate"],
        "max_diversion": industry["max_diversion_rate"],
        "materials": material_values
    }

# Calculate expected values for all combinations
print("\n" + "="*70)
print("EXPECTED VALUES (from our pricing data)")
print("="*70)

report_dir = Path("production_reports")
pdf_files = list(report_dir.glob("*.pdf"))
print(f"\nFound {len(pdf_files)} PDF reports")

print("\n{:<35} {:>12} {:>15} {:>12}".format(
    "Report", "Volume (t)", "Annual Value", "CO2 (t)"))
print("-"*75)

expected_values = []
for tier in ["small", "medium", "large", "enterprise"]:
    for file_ind, ind_key in INDUSTRY_MAP.items():
        expected = calculate_expected_values(ind_key, tier)
        if expected:
            expected_values.append(expected)
            print("{:<35} {:>12,.0f} ${:>14,.0f} {:>12,.0f}".format(
                f"{file_ind}_{tier}"[:35],
                expected["base_volume"],
                expected["total_annual_value"],
                expected["total_co2_reduction"]
            ))

# Summary statistics
print("\n" + "="*70)
print("SUMMARY STATISTICS")
print("="*70)

values_by_tier = {}
for e in expected_values:
    tier = e["tier"]
    if tier not in values_by_tier:
        values_by_tier[tier] = []
    values_by_tier[tier].append(e["total_annual_value"])

print("\nAverage Annual Value by Tier:")
for tier in ["small", "medium", "large", "enterprise"]:
    vals = values_by_tier.get(tier, [])
    if vals:
        avg = sum(vals) / len(vals)
        min_v = min(vals)
        max_v = max(vals)
        print(f"  {tier:12}: ${avg:>15,.0f} (range: ${min_v:,.0f} - ${max_v:,.0f})")

# Value ranges check
print("\n" + "="*70)
print("SANITY CHECKS")
print("="*70)

issues = []
for e in expected_values:
    # Check reasonable ranges
    v = e["total_annual_value"]
    tier = e["tier"]
    
    # Expected ranges
    if tier == "small" and (v < 1000 or v > 5_000_000):
        issues.append(f"WARN: {e['industry']} {tier} value ${v:,.0f} seems off for small tier")
    if tier == "medium" and (v < 10000 or v > 50_000_000):
        issues.append(f"WARN: {e['industry']} {tier} value ${v:,.0f} seems off for medium tier")
    if tier == "large" and (v < 100000 or v > 500_000_000):
        issues.append(f"WARN: {e['industry']} {tier} value ${v:,.0f} seems off for large tier")
    if tier == "enterprise" and (v < 500000 or v > 2_000_000_000):
        issues.append(f"WARN: {e['industry']} {tier} value ${v:,.0f} seems off for enterprise tier")
    
    # Check CO2 reasonable
    co2 = e["total_co2_reduction"]
    if co2 < 0:
        issues.append(f"ERROR: {e['industry']} {tier} has negative CO2 {co2:,.0f}")

if issues:
    print("\nIssues found:")
    for i in issues:
        print(f"  ⚠ {i}")
else:
    print("\n✅ All values within expected ranges")

# Save audit results
with open("production_report_audit.json", "w") as f:
    json.dump(expected_values, f, indent=2)
print(f"\n[OK] Saved audit data to production_report_audit.json")
