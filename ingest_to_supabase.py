"""
BULK INGESTION: SYMBIO DATA ENGINE -> SUPABASE
==============================================
Target Table: waste_listings
Batch Size: 1000
Start: python ingest_to_supabase.py
"""
import os
import pandas as pd
from supabase import create_client, Client

# CONFIG (User should ensure these are in .env)
import os
from dotenv import load_dotenv
load_dotenv()

URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
CSV_FILE = "exports/symbio_data_engine_READY.csv"

def ingest():
    if not URL or not KEY:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_KEY in environment.")
        return

    print(f"Connecting to Supabase: {URL}")
    supabase: Client = create_client(URL, KEY)

    print(f"Reading CSV: {CSV_FILE}")
    # Chunk size logic for memory safety
    chunk_size = 5000
    
    total_inserted = 0
    
    # Read CSV in chunks
    for chunk in pd.read_csv(CSV_FILE, chunksize=chunk_size):
        # 1. CLEANING
        # Fix Column Names (deduplicate)
        # We manually map CSV columns to DB columns
        records = []
        for _, row in chunk.iterrows():
            record = {
                "source_company": str(row.get('source_company', '')),
                "material": str(row.get('material', '') or row.get('waste_description', '')), # Fallback
                "quantity_onsite": float(pd.to_numeric(row.get('quantity_onsite'), errors='coerce') or 50),
                "price_per_ton_usd": float(pd.to_numeric(row.get('price_per_ton_usd'), errors='coerce') or 0),
                "region": str(row.get('region', '')),
                "lat": float(pd.to_numeric(row.get('lat'), errors='coerce') or 0),
                "lon": float(pd.to_numeric(row.get('lon'), errors='coerce') or 0),
                "chemical_profile": row.get('chemical_profile', '{}'), # JSON string
                "co2_factor": float(pd.to_numeric(row.get('co2_factor'), errors='coerce') or 0),
                "process_context": str(row.get('process_context', '')),
                "cas_numbers": str(row.get('cas_numbers', '')),
                "is_alpha_verified": str(row.get('is_alpha_verified', 'False')).lower() == 'true'
            }
            records.append(record)
            
        # 2. UPSERT
        try:
            # Upsert in smaller batches if needed, but 5000 might work depending on payload limit
            # Supabase free tier usually handles 1000 well.
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                supabase.table("waste_listings").upsert(batch).execute()
                total_inserted += len(batch)
                print(f"   Inserted {total_inserted} rows...", end='\r')
                
        except Exception as e:
            print(f"\nError inserting batch: {e}")
            
    print(f"\nCOMPLETE. Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    ingest()
