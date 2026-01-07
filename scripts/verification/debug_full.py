from pathlib import Path
import csv
from processors.gov_processor import GovProcessor
from processors.models import ExtractionResult
from store.postgres import execute_query

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = Path(docs[0]['file_path'])

print(f"File: {file_path}")
print(f"Exists: {file_path.exists()}")

gp = GovProcessor()

# Manually run process_csv logic with debug
with open(file_path, "r", encoding="utf-8", errors="replace") as f:
    sample = f.read(4096)
    f.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample)
        print(f"Dialect detected: delimiter={repr(dialect.delimiter)}")
    except csv.Error:
        dialect = None
        print("Failed to detect dialect, using default")
    
    reader = csv.DictReader(f, dialect=dialect)
    headers = reader.fieldnames or []
    print(f"Headers count: {len(headers)}")
    
    mapping = gp._identify_mapping(headers)
    print(f"Mapping: {mapping}")
    
    if not mapping:
        print("MAPPING FAILED!")
        exit()
    
    # Process rows
    results = []
    for i, row in enumerate(reader):
        wastes = gp._extract_row(row, mapping)
        if wastes:
            results.extend(wastes)
        if i < 5 and not wastes:
            # Print debug for failed rows
            print(f"\nRow {i} failed extraction:")
            for key in ["company", "material", "released"]:
                col = mapping.get(key)
                if col:
                    val = row.get(col, "NOT_FOUND")
                    print(f"  {key}: {val[:50] if val else 'None'}")
        if i >= 100:
            break
    
    print(f"\n=== Results ===")
    print(f"Total wastes extracted: {len(results)}")
