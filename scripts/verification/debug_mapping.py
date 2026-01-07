from store.postgres import execute_query
import csv
from processors.gov_processor import GovProcessor

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = docs[0]['file_path']

# Get headers and first data row
with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    first_row = next(reader, None)

if not first_row:
    print("No data rows!")
    exit()

# Test mapping detection
gp = GovProcessor()
mapping = gp._identify_mapping(headers)
print(f"Mapping: {mapping}")

# Print the actual values for key columns
print("\n=== Key Column Values from First Row ===")
for key in ["company", "material", "unit", "year", "released", "recycled"]:
    col = mapping.get(key)
    if col:
        value = first_row.get(col, "NOT FOUND")
        print(f"  {key}: [{col}] = '{value}'")

# Try extracting manually
print("\n=== Testing _extract_row ===")
results = gp._extract_row(first_row, mapping)
print(f"Results from _extract_row: {len(results)}")
if results:
    for r in results[:3]:
        print(f"  waste_type: {r.waste_type}, qty: {r.quantity_tons}, method: {r.treatment_method}")
