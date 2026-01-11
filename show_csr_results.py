"""Show what we extracted from 177 CSR PDFs - save to file."""
import csv

output = []
output.append("="*70)
output.append("CSR EXTRACTION SUMMARY - 177 PDFs")
output.append("="*70)

# Waste data
with open("exports/csr_waste_data.csv", "r", encoding="utf-8") as f:
    waste = list(csv.DictReader(f))
output.append(f"\nWASTE RECORDS: {len(waste)}")
output.append("Sample waste data:")
for w in waste[:20]:
    company = w["source_company"]
    material = w["material"][:35]
    tons = float(w["quantity_tons"])
    wtype = w["waste_type"]
    output.append(f"  {company}: {material} - {tons:,.0f} tons ({wtype})")

waste_companies = set(w["source_company"] for w in waste)
output.append(f"\nUnique companies with waste data: {len(waste_companies)}")

# Emissions
with open("exports/csr_emissions_data.csv", "r", encoding="utf-8") as f:
    emissions = list(csv.DictReader(f))
output.append(f"\n{'='*70}")
output.append(f"EMISSION RECORDS: {len(emissions)}")
output.append("Sample emissions:")
for e in emissions[:20]:
    company = e["source_company"]
    etype = e["emission_type"]
    value = float(e["value"])
    unit = e["unit"]
    scope = e["scope"]
    output.append(f"  {company}: {etype} - {value:,.0f} {unit} ({scope})")

emit_companies = set(e["source_company"] for e in emissions)
output.append(f"\nUnique companies with emissions: {len(emit_companies)}")

# Financials
with open("exports/csr_financial_data.csv", "r", encoding="utf-8") as f:
    financials = list(csv.DictReader(f))
output.append(f"\n{'='*70}")
output.append(f"FINANCIAL RECORDS: {len(financials)}")
output.append("Sample financials:")
for fin in financials[:20]:
    company = fin["source_company"]
    category = fin["category"]
    value = float(fin["value"])
    output.append(f"  {company}: {category} - ${value:,.0f}")

fin_companies = set(f["source_company"] for f in financials)
output.append(f"\nUnique companies with financial data: {len(fin_companies)}")

output.append(f"\n{'='*70}")
output.append(f"GRAND TOTAL: {len(waste) + len(emissions) + len(financials)} records")
output.append(f"Unique companies: {len(waste_companies | emit_companies | fin_companies)}")

# Write to file
with open("csr_summary.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(output))

print("Saved to csr_summary.txt")
print("\n".join(output))
