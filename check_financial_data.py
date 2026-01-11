"""Check financial data for MWh parsing bug."""
import csv

# Check financial data
with open("exports/csr_financial_data.csv", "r", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

print("="*60)
print("FINANCIAL DATA CHECK")
print("="*60)
print(f"Total records: {len(rows)}")

# Check for any huge values
huge = [r for r in rows if float(r["value"]) > 1_000_000_000]
print(f"Values > $1B: {len(huge)}")

if huge:
    print("\nHuge values found:")
    for r in huge:
        print(f"  {r['source_company']}: ${float(r['value']):,.0f} - {r['context'][:50]}")

# Check max value
if rows:
    max_val = max(rows, key=lambda x: float(x["value"]))
    print(f"\nMax value: {max_val['source_company']}: ${float(max_val['value']):,.0f}")
    print(f"  Context: {max_val['context']}")
    
    # Check if any MWh entries slipped through
    mwh_in_fin = [r for r in rows if "mwh" in r["context"].lower()]
    print(f"\nMWh in financials (should be 0): {len(mwh_in_fin)}")

# Check energy data
print("\n" + "="*60)
print("ENERGY DATA CHECK")
print("="*60)
with open("exports/csr_energy_data.csv", "r", encoding="utf-8") as f:
    energy = list(csv.DictReader(f))
print(f"Total energy records: {len(energy)}")
if energy[:5]:
    print("Sample energy data:")
    for e in energy[:5]:
        print(f"  {e['source_company']}: {e['category']} - {float(e['value']):,.0f} {e['unit']}")
