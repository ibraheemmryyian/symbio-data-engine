"""Verify 5-category CSR extraction results."""
import csv

def check_csv(filename, label):
    try:
        with open(f"exports/{filename}", "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        print(f"{label}: {len(rows)} records")
        return rows
    except FileNotFoundError:
        print(f"{label}: FILE NOT FOUND")
        return []

print("="*60)
print("CSR v3 EXTRACTION RESULTS - 5 CATEGORIES")
print("="*60)

waste = check_csv("csr_waste_data.csv", "Waste")
emissions = check_csv("csr_emissions_data.csv", "Emissions")
financials = check_csv("csr_financial_data.csv", "Financials")
energy = check_csv("csr_energy_data.csv", "Energy")
cc = check_csv("csr_carbon_credits.csv", "Carbon Credits")

print(f"\nTOTAL RECORDS: {len(waste) + len(emissions) + len(financials) + len(energy) + len(cc)}")

# Show carbon credit breakdown
if cc:
    print("\n" + "="*60)
    print("CARBON CREDIT BREAKDOWN BY CATEGORY")
    print("="*60)
    categories = {}
    for r in cc:
        cat = r.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

# Sample carbon credits
if cc[:5]:
    print("\nSAMPLE CARBON CREDITS:")
    for r in cc[:5]:
        print(f"  {r['source_company']}: {r['category']} - {r['value']} {r['unit']}")
        print(f"    Context: {r['context'][:60]}...")
