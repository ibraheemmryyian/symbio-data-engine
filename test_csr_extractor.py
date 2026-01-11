"""Test CSR extractor with sample text."""
from processors.csr_extractor import CSRExtractor

# Sample CSR report text
SAMPLE_TEXT = """
BOROUGE SUSTAINABILITY REPORT 2023

Waste Management Performance
----------------------------
In 2023, we recycled 45,000 tonnes of plastic waste from our operations.
Total waste generated was 125,000 tonnes, with a recycling rate of 36%.
We disposed 15,000 tonnes of hazardous waste through certified contractors.
Organic waste diverted from landfill: 8,500 tonnes.

Carbon Emissions
----------------
Our total CO2 emissions in 2023 were 2.5 million tonnes.
Scope 1: 1,200,000 tCO2e
Scope 2: 850,000 tCO2e  
Scope 3: 450,000 tCO2e
We reduced emissions by 12% compared to 2022.

Financial Performance - Sustainability
--------------------------------------
Waste disposal costs of $12.5 million were incurred.
Recycling revenue: EUR 8.5 million generated from material sales.
We saved $5.2M through energy efficiency improvements.
Environmental investments of $45M in circular economy initiatives.
"""

extractor = CSRExtractor()

# Test extraction
print("="*60)
print("TESTING CSR EXTRACTOR WITH SAMPLE TEXT")
print("="*60)

# Manually call extraction methods
waste = extractor._extract_waste(SAMPLE_TEXT, "borouge", 2023)
emissions = extractor._extract_emissions(SAMPLE_TEXT, "borouge", 2023)
financials = extractor._extract_financials(SAMPLE_TEXT, "borouge", 2023)

print(f"\nWaste Data ({len(waste)} records):")
for w in waste:
    print(f"  - {w.material}: {w.quantity_tons:,.0f} tons ({w.waste_type})")
    print(f"    Context: {w.context}")

print(f"\nEmissions ({len(emissions)} records):")
for e in emissions:
    print(f"  - {e.emission_type}: {e.value:,.0f} {e.unit} {e.scope}")

print(f"\nFinancials ({len(financials)} records):")
for f in financials:
    print(f"  - {f.category}: ${f.value:,.0f} {f.currency}")

print("\n" + "="*60)
print("VALIDATION:")
print("="*60)
print(f"Waste extraction: {'PASS' if len(waste) >= 3 else 'FAIL'} ({len(waste)} records)")
print(f"Emission extraction: {'PASS' if len(emissions) >= 3 else 'FAIL'} ({len(emissions)} records)")
print(f"Financial extraction: {'PASS' if len(financials) >= 3 else 'FAIL'} ({len(financials)} records)")
