"""Audit data readiness for report generation."""
import csv
import json

print("="*70)
print("DATA READINESS AUDIT FOR REPORT GENERATION")
print("="*70)

# 1. Check waste listings with pricing
print("\n[1] WASTE LISTINGS WITH PRICING")
with open("exports/waste_listings_with_pricing.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Total records: {len(rows):,}")
fields = list(rows[0].keys()) if rows else []
print(f"Fields: {fields}")

# Count priced vs unpriced
priced = sum(1 for r in rows if r.get("price_per_ton") and float(r.get("price_per_ton") or 0) > 0)
print(f"With pricing: {priced:,} ({100*priced/len(rows):.1f}%)")
print(f"Without pricing: {len(rows) - priced:,}")

# Check industry field
industries = set(r.get("industry", r.get("naics_code", "unknown")) for r in rows)
print(f"Unique industries: {len(industries)}")

# Check region field
regions = set(r.get("region", r.get("state", "unknown")) for r in rows)
print(f"Unique regions: {len(regions)}")

# Sample row
print("\nSample row fields:")
for k, v in list(rows[0].items())[:20]:
    val = str(v)[:40] + "..." if len(str(v)) > 40 else v
    print(f"  {k}: {val}")

# 2. Check CSR financial data
print("\n[2] CSR FINANCIAL DATA")
with open("exports/csr_financial_data.csv", "r", encoding="utf-8") as f:
    fin = list(csv.DictReader(f))
print(f"Total records: {len(fin)}")
categories = set(r["category"] for r in fin)
print(f"Categories: {categories}")

# 3. Check material valuations
print("\n[3] MATERIAL VALUATIONS")
with open("exports/material_valuations.csv", "r", encoding="utf-8") as f:
    vals = list(csv.DictReader(f))
print(f"Total materials: {len(vals)}")
if vals:
    print(f"Fields: {list(vals[0].keys())}")
    print("Sample:")
    for v in vals[:5]:
        print(f"  {v}")

# 4. Summary
print("\n" + "="*70)
print("READINESS SUMMARY")
print("="*70)
