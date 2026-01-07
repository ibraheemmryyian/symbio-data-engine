
import os
import pandas as pd
import psycopg2
import uuid
import re
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

# Paths
RAW_DIR = Path("data/raw/mena")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Database Connection
def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

# Fuzzy Header Matcher
def identify_column(columns, candidates):
    """Finds the first column that matches any of the candidate keywords (case-insensitive)."""
    for col in columns:
        clean_col = str(col).lower().strip().replace("_", "")
        for cand in candidates:
            if cand in clean_col:
                return col
    return None

def ingest_file(filepath):
    print(f"\nProcessing: {filepath.name}...")
    
    # 1. Determine Country from Filename
    filename = filepath.name.lower()
    country = "SAU" if "saudi" in filename or "ksa" in filename else \
              "ARE" if "uae" in filename or "emirates" in filename or "dubai" in filename else \
              "QAT" if "qatar" in filename else "MENA"
              
    print(f"   Detected Region: {country}")
    
    # 2. Load Data (Excel or CSV)
    try:
        if filepath.suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath)
        else:
            df = pd.read_csv(filepath, encoding='utf-8', errors='replace')
    except Exception as e:
        print(f"   ❌ Failed to read file: {e}")
        return

    # 3. Map Columns (The Magic)
    cols = df.columns
    
    # Keyword Dictionary for Auto-Detection
    map_config = {
        "material": ["waste", "type", "material", "item", "substance", "class"],
        "quantity": ["qty", "quant", "weight", "ton", "amount", "volume", "total"],
        "company": ["facility", "company", "generator", "source", "name", "entity"],
        "location": ["city", "region", "location", "area", "zone", "address"],
        "year": ["year", "date", "period"]
    }
    
    col_map = {}
    for field, keywords in map_config.items():
        found = identify_column(cols, keywords)
        if found:
            col_map[field] = found
            print(f"   ✅ Mapped '{field}' -> '{found}'")
        else:
            print(f"   ⚠️ Could not map '{field}' (Will use placeholders)")

    if "material" not in col_map or "quantity" not in col_map:
        print("   ❌ CRITICAL: Could not find 'Waste' or 'Quantity' columns. Skipping file.")
        return

    # 4. Ingest Rows
    conn = get_db_connection()
    cur = conn.cursor()
    conn.autocommit = True
    
    inserted_count = 0
    # Generate a persistent Document ID for this file
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, filepath.name))
    
    # Register document first
    try:
        cur.execute("""
            INSERT INTO documents (id, source, source_url, document_type, status, size_bytes)
            VALUES (%s, 'mena_manual', %s, 'csv', 'completed', %s)
            ON CONFLICT (id) DO NOTHING
        """, (doc_id, filepath.name, os.path.getsize(filepath)))
    except Exception as e:
        print(f"Doc error: {e}")
    
    for idx, row in df.iterrows():
        try:
            # Extract Values
            mat = str(row[col_map['material']]).strip() if 'material' in col_map else "Unspecified Waste"
            
            # Handle Quantity Cleaning (remove 'tons', commas)
            qty_raw = str(row[col_map['quantity']]) if 'quantity' in col_map else "0"
            qty_clean = re.sub(r'[^\d.]', '', qty_raw)
            try:
                qty = float(qty_clean)
            except:
                qty = 0.0
                
            comp = str(row[col_map['company']]).strip() if 'company' in col_map else "Anonymous Generator"
            loc = str(row[col_map['location']]).strip() if 'location' in col_map else f"{country} (General)"
            year = int(row[col_map['year']]) if 'year' in col_map and str(row[col_map['year']]).isdigit() else 2024
            
            if qty > 0 and len(mat) > 2:
                cur.execute("""
                    INSERT INTO waste_listings 
                    (document_id, material, quantity_tons, source_company, source_location, source_country, year, material_category, treatment_method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'MENA Industrial', 'Unknown')
                    ON CONFLICT (document_id, material, source_company, year, quantity_tons) DO NOTHING
                """, (doc_id, mat, qty, comp, loc, country, year))
                inserted_count += 1
                
        except Exception as e:
            continue # Skip bad rows
            
    print(f"   🚀 Ingested {inserted_count} records from {filepath.name}")
    conn.close()

if __name__ == "__main__":
    all_files = list(RAW_DIR.glob("*"))
    files = [f for f in all_files if f.suffix.lower() in ['.csv', '.xlsx', '.xls']]
    if not files:
        print(f"No files found in {RAW_DIR.absolute()}")
        print(f"Directory contents: {[f.name for f in all_files]}")
    else:
        for f in files:
            ingest_file(f)
