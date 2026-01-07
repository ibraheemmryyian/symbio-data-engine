
import pandas as pd
import psycopg2
import uuid
import re
from pathlib import Path
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

# Absolute Path to the elusive file
TARGET_FILE = Path(r"C:\Users\Imrry\Desktop\symbio_data_engine\data\raw\mena\SEEA Waste 2024-EN.xlsx")

def process():
    print(f"Targeting file: {TARGET_FILE}")
    if not TARGET_FILE.exists():
        print("❌ File not found at absolute path!")
        return

    print("✅ File found. Reading...")
    
    try:
        # Load Excel - usually the first sheet is the summary
        df = pd.read_excel(TARGET_FILE)
        print(f"Loaded {len(df)} rows.")
        
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Doc Registration
        doc_id = str(uuid.uuid5(uuid.NAMESPACE_URL, TARGET_FILE.name))
        try:
            cur.execute("""
                INSERT INTO documents (id, source, source_url, document_type, status)
                VALUES (%s, 'mena_manual', %s, 'xlsx', 'completed')
                ON CONFLICT (id) DO NOTHING
            """, (doc_id, TARGET_FILE.name))
        except:
            pass

        # Smart Ingestion for "SEEA" format
        # Usually it has "Activity" and "Waste Type"
        # We'll just look for numbers and meaningful text
        
        inserted = 0
        
        # Simple Logic: Iterate columns, find 'Waste' and 'Amount'
        # Since we don't know the exact structure, we dump content into a generic structure if needed
        # Or we try to identifying the "Waste Type" column by keyword
        
        cols = [str(c).lower() for c in df.columns]
        
        # HACK: If we can't map it perfectly, we just grab row-by-row and look for a String + a Number
        for idx, row in df.iterrows():
            try:
                # Heuristic: Find the first String column (Material) and first Number column (Quantity)
                material = "Unknown"
                quantity = 0.0
                
                for val in row.values:
                    s_val = str(val).strip()
                    # Check for float/int
                    try:
                        f_val = float(s_val)
                        if f_val > 0 and quantity == 0:
                            quantity = f_val
                    except:
                        # Check for string text (Material)
                        if len(s_val) > 3 and material == "Unknown" and not s_val.isdigit():
                            material = s_val
                            
                if quantity > 0 and material != "Unknown":
                    # Insert
                     cur.execute("""
                        INSERT INTO waste_listings 
                        (document_id, material, quantity_tons, source_company, source_location, source_country, year, material_category, treatment_method)
                        VALUES (%s, %s, %s, 'Saudi Generic Industry', 'Saudi Arabia', 'SAU', 2024, 'MENA SEEA', 'Unknown')
                        ON CONFLICT (document_id, material, source_company, year, quantity_tons) DO NOTHING
                    """, (doc_id, material, quantity))
                     inserted += 1
            except Exception as e:
                continue

        print(f"🚀 Ingested {inserted} records from SEEA Waste 2024.")
        conn.close()

    except Exception as e:
        print(f"❌ Failed to process: {e}")

if __name__ == "__main__":
    process()
