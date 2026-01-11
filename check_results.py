import csv

print("="*60)
print("ENERGY DATA CHECK")
print("="*60)
with open("exports/csr_energy_data.csv", "r", encoding="utf-8") as f:
    r = list(csv.DictReader(f))
print(f"Energy records: {len(r)}")
for x in r[:10]:
    print(f"  {x['source_company']}: {x['category']} - {x['value']} {x['unit']}")

print("\n" + "="*60)
print("FINANCIAL DATA CHECK")
print("="*60)
with open("exports/csr_financial_data.csv", "r", encoding="utf-8") as f:
    fin = list(csv.DictReader(f))
print(f"Financial records: {len(fin)}")

# Check for MWh in financials
mwh_in_fin = [x for x in fin if "mwh" in x.get("context", "").lower()]
print(f"MWh in financials (should be 0): {len(mwh_in_fin)}")

# Check max value
max_val = max(fin, key=lambda x: float(x["value"]))
print(f"\nMax financial value: ${float(max_val['value']):,.0f}")
print(f"  Company: {max_val['source_company']}")
print(f"  Context: {max_val['context']}")

# Sample financial data
print("\nSample financials:")
for x in fin[:5]:
    print(f"  {x['source_company']}: {x['category']} - ${float(x['value']):,.0f}")
