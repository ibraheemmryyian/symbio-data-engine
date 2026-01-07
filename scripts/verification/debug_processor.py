from pathlib import Path
from processors.gov_processor import GovProcessor

# Get the gov CSV file path from database
from store.postgres import execute_query
docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
if not docs:
    print("No government CSV documents found!")
    exit()

file_path = docs[0]['file_path']
print(f"Testing file: {file_path}")

# Check if file exists
p = Path(file_path)
if not p.exists():
    print(f"ERROR: File does not exist at path: {file_path}")
    exit()

print(f"File exists, size: {p.stat().st_size} bytes")

# Read first few lines
print("\n=== First 5 lines of CSV ===")
with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    for i, line in enumerate(f):
        if i >= 5:
            break
        print(f"  {i}: {line[:150]}...")

# Test GovProcessor
print("\n=== Testing GovProcessor ===")
gp = GovProcessor()
results = gp.process_csv(file_path)
print(f"Results count: {len(results)}")

if results:
    valid_count = sum(1 for r in results if r.is_valid)
    print(f"Valid results: {valid_count}")
    
    for i, r in enumerate(results[:5]):
        print(f"  Result {i}: valid={r.is_valid}, type={r.record_type}")
        if r.data:
            # Print key fields from the extracted data
            print(f"    waste_type: {getattr(r.data, 'waste_type', 'N/A')[:50] if hasattr(r.data, 'waste_type') else 'N/A'}")
            print(f"    quantity_tons: {getattr(r.data, 'quantity_tons', 'N/A')}")
            print(f"    treatment: {getattr(r.data, 'treatment_method', 'N/A')}")
else:
    print("No results returned from GovProcessor!")
