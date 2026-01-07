"""
Re-ingest ALL 16 EU files from the User-friendly-CSV folder.
Clears old HTML-based eprtr docs and ingests real CSVs.
"""
import psycopg2
import hashlib
import json
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

EU_CSV_DIR = Path("data/raw/eprtr/eea_t_ied-eprtr_p_2007-2023_v15_r00/User-friendly-CSV")

def reingest():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("1. Clearing old (HTML-based) eprtr documents...")
    # First delete waste_listings that reference eprtr documents (FK constraint)
    cur.execute("DELETE FROM waste_listings WHERE document_id IN (SELECT id FROM documents WHERE source = 'eprtr')")
    cur.execute("DELETE FROM documents WHERE source = 'eprtr'")
    print("   Done - cleared old EU docs and their waste_listings")
    
    print("\n2. Ingesting real EU CSV files...")
    count = 0
    for csv_file in sorted(EU_CSV_DIR.glob("*.csv")):
        size_bytes = csv_file.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        
        # Hash based on filename + size
        hash_input = f"{csv_file.name}:{size_bytes}"
        file_hash = hashlib.sha256(hash_input.encode()).hexdigest()
        
        abs_path = csv_file.resolve()
        abs_url = f"file://{abs_path}"
        
        meta_json = json.dumps({
            "manual_ingest": True,
            "filename": csv_file.name,
            "file_path": str(abs_path),
            "size_mb": round(size_mb, 2)
        })
        
        cur.execute("""
            INSERT INTO documents (source, source_url, file_path, document_type, content_hash, status, metadata)
            VALUES (%s, %s, %s, %s, %s, 'pending', %s)
        """, ("eprtr", abs_url, str(abs_path), "csv", file_hash, meta_json))
        
        print(f"   [+] {csv_file.name} ({size_mb:.1f} MB)")
        count += 1
    
    print(f"\n3. Ingested {count} EU CSV files. Ready for processing.")
    print("   Run: python main.py process --source eprtr")

if __name__ == "__main__":
    reingest()
