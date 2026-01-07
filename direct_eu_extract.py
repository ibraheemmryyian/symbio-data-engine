"""
DIRECT EU WASTE TRANSFERS EXTRACTION
====================================
Directly parses F4_2_WasteTransfers_Facilities.csv and inserts into waste_listings.
Bypasses the pipeline for maximum data extraction.
"""
import csv
import psycopg2
from pathlib import Path
from decimal import Decimal
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

EU_FILE = Path("data/raw/eprtr/eea_t_ied-eprtr_p_2007-2023_v15_r00/User-friendly-CSV/F4_2_WasteTransfers_Facilities.csv")

def extract_eu():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    
    print(f"Loading EU WasteTransfers file ({EU_FILE.stat().st_size / (1024*1024):.1f} MB)...")
    
    # First, get or create a document entry for this
    cur.execute("SELECT id FROM documents WHERE source = 'eprtr' AND file_path LIKE '%F4_2_WasteTransfers%' LIMIT 1")
    doc = cur.fetchone()
    if doc:
        doc_id = doc[0]
    else:
        cur.execute("""
            INSERT INTO documents (source, source_url, file_path, document_type, content_hash, status)
            VALUES ('eprtr', 'file://EU_WasteTransfers', %s, 'csv', 'direct_extract', 'processing')
            RETURNING id
        """, (str(EU_FILE.resolve()),))
        doc_id = cur.fetchone()[0]
    
    print(f"Using document ID: {doc_id}")
    
    # Clear any existing EU waste listings for clean insert
    cur.execute("DELETE FROM waste_listings WHERE document_id = %s", (doc_id,))
    
    count = 0
    batch = []
    batch_size = 500
    
    with open(EU_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Extract fields using EU column names
                facility = row.get('facilityName', '').strip()
                waste_class = row.get('wasteClassification', '').strip()  # HW/NONHW
                treatment = row.get('wasteTreatment', '').strip()  # D/R
                quantity = row.get('wasteTransfers', '0').strip()
                year = row.get('reportingYear', '').strip()
                country = row.get('countryName', '').strip()
                city = row.get('city', '').strip()
                
                # Parse quantity (comes as float string)
                try:
                    qty_tons = float(quantity) if quantity else 0
                except:
                    qty_tons = 0
                
                if qty_tons <= 0:
                    continue
                
                # Map waste classification
                if waste_class.lower() == 'hw':
                    material = 'Hazardous Waste'
                elif waste_class.lower() == 'nonhw':
                    material = 'Non-Hazardous Waste'
                else:
                    material = waste_class if waste_class else 'Mixed Waste'
                
                # Map treatment method
                if treatment == 'D':
                    treatment_method = 'Disposal'
                elif treatment == 'R':
                    treatment_method = 'Recovery/Recycled'
                else:
                    treatment_method = treatment if treatment else 'Unknown'
                
                # Location
                location = f"{city}, {country}" if city else country
                
                # Build citation
                citation = f"EU E-PRTR WasteTransfers {year}: {facility} - {material} ({treatment_method})"
                
                batch.append((
                    doc_id,
                    material,
                    qty_tons,
                    treatment_method,
                    facility[:200] if facility else None,
                    location[:100] if location else None,
                    int(year) if year.isdigit() else None,
                    citation[:500],
                    1.0  # confidence
                ))
                count += 1
                
                if len(batch) >= batch_size:
                    insert_batch(cur, batch)
                    batch = []
                    if count % 5000 == 0:
                        print(f"   Processed {count:,} records...")
                        
            except Exception as e:
                continue
    
    # Insert remaining
    if batch:
        insert_batch(cur, batch)
    
    # Update document status
    cur.execute("UPDATE documents SET status = 'completed' WHERE id = %s", (doc_id,))
    
    print(f"\nDONE! Extracted {count:,} EU waste transfer records.")
    print("Now regenerating training data exports...")

def insert_batch(cur, batch):
    """Insert batch of records."""
    cur.executemany("""
        INSERT INTO waste_listings 
        (document_id, material, quantity_tons, treatment_method, source_company, source_location, year, source_quote, extraction_confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, batch)

if __name__ == "__main__":
    extract_eu()
