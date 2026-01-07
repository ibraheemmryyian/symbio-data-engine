from pathlib import Path
from processors.gov_processor import GovProcessor
from store.postgres import execute_query

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = Path(docs[0]['file_path'])

gp = GovProcessor()
results = gp.process_csv(file_path)

# Get first valid result
for r in results:
    if r.is_valid and r.data:
        print("=== ALL Dict keys ===")
        for k in sorted(r.data.keys()):
            print(f"  - {k}")
        
        print("\n=== DB waste_listings columns ===")
        db_cols = [
            "id", "document_id", "material", "material_category", "material_subcategory",
            "cas_number", "quantity_tons", "quantity_unit", "price_per_ton", "currency",
            "price_type", "source_company", "source_industry", "source_location", 
            "source_country", "source_coordinates", "quality_grade", "purity_percentage",
            "availability_status", "listing_date", "expiry_date", "extraction_confidence",
            "data_source_url", "year", "created_at", "updated_at"
        ]
        
        print("\n=== Keys NOT in DB ===")
        for k in r.data.keys():
            if k not in db_cols:
                print(f"  ⚠️ {k} (not a DB column!)")
        break
