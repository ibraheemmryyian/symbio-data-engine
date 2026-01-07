from store.postgres import execute_query
import csv

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = docs[0]['file_path']

# Read rows
with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    rows = [next(reader) for _ in range(5)]  # Get first 5 rows

# Key columns based on what we found
key_cols = {
    "company": "4. FACILITY NAME",
    "material": "37. CHEMICAL",
    "year": "1. YEAR",
    "released": "107. TOTAL RELEASES",
    "recycled": "94. OFF-SITE RECYCLED TOTAL",
    "unit": "50. UNIT OF MEASURE"
}

print("=== Values for First 5 Rows ===\n")
for i, row in enumerate(rows):
    print(f"Row {i}:")
    for name, col in key_cols.items():
        val = row.get(col, "COL_NOT_FOUND")
        print(f"  {name}: [{val}]")
    print()
