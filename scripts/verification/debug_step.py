from pathlib import Path
import csv
from processors.models import WasteListingExtraction
from store.postgres import execute_query

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = Path(docs[0]['file_path'])

# Read first row manually
with open(file_path, "r", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    row = next(reader)

# Key column mapping (from what we know works)
mapping = {
    "company": "4. FACILITY NAME",
    "material": "37. CHEMICAL", 
    "unit": "50. UNIT OF MEASURE",
    "year": "1. YEAR",
    "released": "107. TOTAL RELEASES",
}

# Extract values
company = row.get(mapping["company"], "").strip()
material = row.get(mapping["material"], "").strip()
unit = row.get(mapping["unit"], "lbs").strip()
year = row.get(mapping["year"], "")
quantity_str = row.get(mapping["released"], "0").strip()

print(f"company: {company}")
print(f"material: {material}")
print(f"unit: {unit}")
print(f"year: {year}")
print(f"quantity_str: {quantity_str}")

# Parse quantity 
msg = quantity_str.replace(",", "")
print(f"msg after comma replace: {msg}")

quantity = float(msg)
print(f"quantity: {quantity}")

# Convert to tons
quantity_tons = quantity * 0.000453592  # pounds to tons
print(f"quantity_tons: {quantity_tons}")

# Try creating extraction object
print("\nCreating WasteListingExtraction...")
try:
    waste = WasteListingExtraction(
        waste_type=material,
        quantity_tons=quantity_tons,
        treatment_method="Disposal/Released",
        source_company=company,
        location="USA",
        year=int(year) if str(year).isdigit() else 2024,
        extraction_confidence=1.0,
        source_quote=f"{company} released {quantity} {unit}"[:500]
    )
    print(f"SUCCESS! Created: {waste.waste_type}, {waste.quantity_tons} tons")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")
