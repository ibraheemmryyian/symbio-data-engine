"""
AUDIT TOTAL VOLUME
==================
Goal: Count total records across all major data assets to quantify the "Data Moat".
Files:
- symbio_data_engine_v1.csv (Main Listings)
- waste_listings_granular.csv (Granular Listings)
- process_knowledge_v1.csv (New Intelligence)
"""
import os
from pathlib import Path
import pandas as pd

def count_lines(filepath):
    try:
        with open(filepath, 'rb') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0

def audit():
    print("AUDITING TOTAL DATA ASSETS...\n")
    
    files = [
        "exports/symbio_data_engine_v1.csv",
        "exports/waste_listings_granular.csv",
        "exports/process_knowledge_v1.csv",
        "exports/industry_pricing.json"
    ]
    
    total_listings = 0
    
    print(f"{'FILE':<40} | {'SIZE (MB)':<10} | {'ROWS (Est)':<10}")
    print("-" * 65)
    
    for fname in files:
        if os.path.exists(fname):
            size_mb = os.path.getsize(fname) / (1024 * 1024)
            # Efficient line counting
            rows = count_lines(fname)
            print(f"{fname:<40} | {size_mb:>9.2f} | {rows:>10,}")
            
            if "symbio" in fname or "waste_listings" in fname:
                total_listings = max(total_listings, rows) # Take max of main files to avoid double count
        else:
            print(f"{fname:<40} | {'MISSING':>9} | {'-':>10}")

    print("-" * 65)
    print(f"TOTAL UNIQUE LISTINGS (Est): {total_listings:,}")

if __name__ == "__main__":
    audit()
