import sys
import hashlib
from pathlib import Path
from store.postgres import insert_document

def ingest_file(file_path: str):
    path = Path(file_path).resolve()
    if not path.exists():
        print(f"❌ File not found: {path}")
        return

    print(f"📥 Ingesting manual file: {path.name}")
    
    # Calculate hash
    with open(path, "rb") as f:
        content_hash = hashlib.sha256(f.read()).hexdigest()
    
    # Check extension
    ext = path.suffix.lower().lstrip(".")
    doc_type = "csv" if ext == "csv" else "pdf" if ext == "pdf" else "raw"
    
    try:
        doc_id = insert_document(
            url=f"file:///{path}",
            file_path=str(path),
            domain="manual",
            source="user_upload",
            document_type=doc_type,
            content_hash=content_hash
        )
        print(f"✅ Successfully queued document ID: {doc_id}")
        print("   The processor will pick this up automatically.")
    except Exception as e:
        print(f"❌ Error inserting document: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ingest_file.py <path_to_file>")
        sys.exit(1)
    
    ingest_file(sys.argv[1])
