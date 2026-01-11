"""Analyze waste listings structure for pricing export."""
import csv

with open("exports/waste_listings_with_pricing.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    sample = next(reader)

print("="*60)
print("WASTE LISTINGS STRUCTURE ANALYSIS")
print("="*60)

print("\nALL FIELDS:")
for k, v in sample.items():
    print(f"  {k}: {str(v)[:60]}")

# Read more for distribution
with open("exports/waste_listings_with_pricing.csv", "r", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

print(f"\nTotal records: {len(rows):,}")

# Check material_category distribution
categories = {}
for r in rows:
    cat = r.get("material_category", r.get("category", "unknown"))
    categories[cat] = categories.get(cat, 0) + 1

print(f"\nUnique material categories: {len(categories)}")
print("\nTop 40 categories:")
for cat, count in sorted(categories.items(), key=lambda x: -x[1])[:40]:
    print(f"  {cat}: {count:,}")

# Check states/regions
states = {}
for r in rows:
    state = r.get("state", r.get("region", "unknown"))
    states[state] = states.get(state, 0) + 1

print(f"\nUnique states/regions: {len(states)}")
print("Top 15 states:")
for s, count in sorted(states.items(), key=lambda x: -x[1])[:15]:
    print(f"  {s}: {count:,}")
