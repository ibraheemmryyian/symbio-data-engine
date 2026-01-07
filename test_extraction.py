from pathlib import Path
from processors.gov_processor import GovProcessor
from store.postgres import execute_query

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = Path(docs[0]['file_path'])

print(f"Testing GovProcessor.process_csv on: {file_path.name}")

gp = GovProcessor()
results = gp.process_csv(file_path)

valid_count = sum(1 for r in results if r.is_valid)
print(f"\nResults: {len(results)} total, {valid_count} valid")

if results:
    # Show sample results - data might be dict or object
    for i, r in enumerate(results[:5]):
        if r.is_valid and r.data:
            d = r.data
            # Handle both dict and object access
            if isinstance(d, dict):
                mat = d.get('material', 'N/A')
                qty = d.get('quantity_tons', 0)
                meth = d.get('treatment_method', 'N/A')
            else:
                mat = getattr(d, 'material', 'N/A')
                qty = getattr(d, 'quantity_tons', 0)
                meth = getattr(d, 'treatment_method', 'N/A')
            print(f"  [{i}] {str(mat)[:30]} | {qty:.2f} tons | {meth}")
else:
    print("NO RESULTS!")
