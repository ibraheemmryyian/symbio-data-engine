from store.postgres import execute_query
import csv

docs = execute_query("SELECT file_path FROM documents WHERE source = 'government' AND document_type = 'csv' LIMIT 1")
file_path = docs[0]['file_path']

# Key columns
key_cols = {
    "released": "107. TOTAL RELEASES",
    "recycled": "94. OFF-SITE RECYCLED TOTAL",
}

# Scan for non-zero values
non_zero_count = 0
total_rows = 0
with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    for row in reader:
        total_rows += 1
        for name, col in key_cols.items():
            val = row.get(col, "0").strip().replace(",", "")
            try:
                num = float(val)
                if num > 0:
                    non_zero_count += 1
                    if non_zero_count <= 5:
                        chemical = row.get("37. CHEMICAL", "?")
                        facility = row.get("4. FACILITY NAME", "?")
                        print(f"Found non-zero: {name}={num}, chemical={chemical[:30]}, facility={facility[:30]}")
                    break
            except ValueError:
                pass
        if non_zero_count >= 100:
            break

print(f"\n=== Summary ===")
print(f"Total rows scanned: {total_rows}")
print(f"Rows with non-zero values: {non_zero_count}")
