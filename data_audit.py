"""
Comprehensive data audit script
"""
from pathlib import Path
import json

print("="*70)
print("COMPREHENSIVE DATA AUDIT")
print("="*70)

# 1. Raw PDFs
pdf_dir = Path("data/raw/csr_reports")
pdfs = list(pdf_dir.glob("*.pdf")) if pdf_dir.exists() else []
pdf_size = sum(p.stat().st_size for p in pdfs) / 1024 / 1024
print(f"\n1. RAW CSR PDFs")
print(f"   Files: {len(pdfs)}")
print(f"   Size: {pdf_size:.1f} MB")

# 2. Extracted CSR data
print(f"\n2. EXTRACTED CSR DATA")
exports = [
    ("exports/csr_waste_data.csv", "Waste"),
    ("exports/csr_emissions_data.csv", "Emissions"),
    ("exports/csr_financial_data.csv", "Financial"),
    ("exports/csr_energy_data.csv", "Energy"),
    ("exports/csr_carbon_credits.csv", "Carbon Credits"),
]
total_rows = 0
for path, name in exports:
    p = Path(path)
    if p.exists():
        with open(p, encoding="utf-8", errors="ignore") as f:
            rows = len(f.readlines()) - 1
        size = p.stat().st_size / 1024
        print(f"   {name}: {rows:,} rows ({size:.1f} KB)")
        total_rows += rows
    else:
        print(f"   {name}: NOT FOUND")
print(f"   Total extracted rows: {total_rows:,}")

# 3. Waste Listings (EPA/EU)
print(f"\n3. WASTE LISTINGS (EPA/EU)")
waste_files = [
    "data/processed/waste_listings_with_pricing.csv",
    "data/raw/eu_waste/waste_listings.csv",
]
for wf in waste_files:
    p = Path(wf)
    if p.exists():
        with open(p, encoding="utf-8", errors="ignore") as f:
            rows = len(f.readlines()) - 1
        size = p.stat().st_size / 1024 / 1024
        print(f"   {wf}: {rows:,} rows ({size:.1f} MB)")

# 4. Material Valuations
print(f"\n4. MATERIAL VALUATIONS")
mv = Path("data/processed/material_valuations.csv")
if mv.exists():
    with open(mv, encoding="utf-8") as f:
        rows = len(f.readlines()) - 1
    print(f"   Materials: {rows} unique materials with pricing")

# 5. Industry Pricing
print(f"\n5. INDUSTRY PRICING")
ip = Path("exports/industry_pricing.json")
if ip.exists():
    with open(ip, encoding="utf-8") as f:
        data = json.load(f)
    print(f"   Parent categories: {len(data.get('parent_categories', {}))}")
    print(f"   Sub-industries: {len(data.get('sub_industries', {}))}")
    print(f"   Materials: {len(data.get('materials', {}))}")
    print(f"   Volume tiers: {len(data.get('volume_tiers', {}))}")
    print(f"   Regions: {len(data.get('regional_modifiers', {}))}")

# 6. Total disk usage
print(f"\n6. TOTAL DISK USAGE")
data_dir = Path("data")
total_size = 0
if data_dir.exists():
    for f in data_dir.rglob("*"):
        if f.is_file():
            total_size += f.stat().st_size
print(f"   data/ folder: {total_size/1024/1024:.1f} MB")

exports_dir = Path("exports")
export_size = 0
if exports_dir.exists():
    for f in exports_dir.rglob("*"):
        if f.is_file():
            export_size += f.stat().st_size
print(f"   exports/ folder: {export_size/1024/1024:.1f} MB")
print(f"   TOTAL: {(total_size + export_size)/1024/1024:.1f} MB")
