import os
import hashlib
import psycopg2
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, RAW_DIR

def ingest_manual_eu():
    # Setup DB connection
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    
    # Setup file logging
    log_f = open("ingest_debug.log", "w", encoding="utf-8")
    def log(msg):
        print(msg)
        log_f.write(msg + "\n")
        
    log(f"DEBUG: Connecting to DBname='{POSTGRES_DB}' User='{POSTGRES_USER}' Host='{POSTGRES_HOST}'")

    conn.autocommit = True
    cur = conn.cursor()
    
    # List tables
    try:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = [r[0] for r in cur.fetchall()]
        log(f"DEBUG: Tables in DB: {tables}")
    except Exception as e:
        log(f"DEBUG: Failed to list tables: {e}")




    # Target Directory (The unzipped one)
    # We look for the "User-friendly-CSV" folder
    base_dir = RAW_DIR / "eprtr"
    # Find the dynamic unzipped folder (starts with eea_t_ied...) or just use the one we found
    target_dir = base_dir / "eea_t_ied-eprtr_p_2007-2023_v15_r00" / "User-friendly-CSV"
    
    if not target_dir.exists():
        log(f"[ERROR] Directory not found: {target_dir}")
        # Fallback to base in case user moved files
        target_dir = base_dir

    log(f"[INFO] Scanning {target_dir} ...")
    
    # Debug: List all files to see what is happening
    all_files = list(target_dir.iterdir())
    log(f"Debug: Found {len(all_files)} files in directory.")
    
    files = []
    for f in all_files:
        if f.is_file() and f.suffix.lower() == '.csv':
            files.append(f)
        else:
            log(f"   Ignoring: {f.name} (Suffix: {f.suffix})")
            
    log(f"Debug: Filtered {len(files)} CSV files.")
    count = 0
    skipped = 0

    for file_path in files:
        try:
            # 1. Getting File Info (Do NOT read full content for large files)
            file_stats = file_path.stat()
            size_mb = file_stats.st_size / (1024*1024)
            
            # 2. Hash: For huge files, maybe just hash filename + size to be fast? 
            # Or read in chunks? Reading 500MB is fast on SSD.
            # Let's read first 4KB for hash + filename (good enough for now)
            hasher = hashlib.sha256()
            hasher.update(file_path.name.encode())
            hasher.update(str(file_stats.st_size).encode())
            file_hash = hasher.hexdigest()
            
            # 3. Check if exists
            cur.execute("SELECT id FROM documents WHERE content_hash = %s", (file_hash,))
            if cur.fetchone():
                log(f"   [WARN] Skipped (Already Ingested): {file_path.name}")
                skipped += 1
                continue

            # 4. Insert into 'documents'
            # Content is EMPTY/NULL because 'documents' doesn't have content column in this schema version
            abs_path = file_path.resolve()
            # source_url is the key, not url
            abs_url = f"file://{abs_path}"
            
            # Metadata still useful
            import json
            meta_json = json.dumps({
                "manual_ingest": True, 
                "filename": file_path.name,
                "file_path": str(abs_path) 
            })
            
            # Using source_url and file_path columns
            cur.execute("""
                INSERT INTO documents (source, source_url, file_path, document_type, content_hash, status, metadata)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s)
            """, (
                "eprtr", 
                abs_url, 
                str(abs_path),
                "csv",  # CRITICAL: Must be 'csv' for GovProcessor to handle it
                file_hash,
                meta_json
            ))
            
            log(f"   [OK] Ingested (Ref Only): {file_path.name} ({size_mb:.2f} MB)")
            count += 1

        except Exception as e:
            log(f"   [ERROR] Error processing {file_path.name}: {e}")

    log(f"\n[DONE] Manual Ingestion Complete.")
    log(f"   - Added:   {count}")
    log(f"   - Skipped: {skipped}")
    
    conn.close()

if __name__ == "__main__":
    ingest_manual_eu()
