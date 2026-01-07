"""Scan all EU CSV files and extract headers for column mapping."""
import csv
from pathlib import Path

EU_DIR = Path("data/raw/eprtr/eea_t_ied-eprtr_p_2007-2023_v15_r00/User-friendly-CSV")

for csv_file in EU_DIR.glob("*.csv"):
    print(f"\n=== {csv_file.name} ===")
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        headers = next(csv.reader(f))
        for h in headers:
            print(f"  {h}")
