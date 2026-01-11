"""
Extract data from all downloaded CSR PDFs v2.
Extracts: Waste, Emissions, Financials, Energy (NEW)
"""
import logging
import csv
from pathlib import Path
from datetime import datetime

from processors.csr_extractor import CSRExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_all_csr_data():
    """Extract data from all CSR PDFs."""
    
    pdf_dir = Path("data/raw/csr_reports")
    pdfs = list(pdf_dir.glob("*.pdf"))
    
    print("="*70)
    print("CSR DATA EXTRACTION v2 - 4 Categories")
    print("="*70)
    print(f"PDFs found: {len(pdfs)}")
    
    extractor = CSRExtractor()
    
    all_waste = []
    all_emissions = []
    all_financials = []
    all_energy = []
    all_carbon_credits = []  # NEW
    
    for i, pdf in enumerate(pdfs, 1):
        company = pdf.stem.split("_")[0]
        
        if i % 10 == 0 or i == 1:
            print(f"\n[{i}/{len(pdfs)}] Processing {pdf.name[:50]}...")
        
        try:
            results = extractor.extract_from_pdf(pdf, company)
            
            all_waste.extend(results['waste_data'])
            all_emissions.extend(results['emissions'])
            all_financials.extend(results['financials'])
            all_energy.extend(results.get('energy', []))
            all_carbon_credits.extend(results.get('carbon_credits', []))
            
            w = len(results['waste_data'])
            e = len(results['emissions'])
            f = len(results['financials'])
            en = len(results.get('energy', []))
            cc = len(results.get('carbon_credits', []))
            
            if w or e or f or en or cc:
                logger.info(f"  {pdf.stem[:40]}: W={w} E={e} F={f} En={en} CC={cc}")
                
        except Exception as ex:
            logger.debug(f"  Error: {ex}")
    
    print("\n" + "="*70)
    print("EXTRACTION SUMMARY")
    print("="*70)
    print(f"PDFs processed: {len(pdfs)}")
    print(f"Waste records: {len(all_waste)}")
    print(f"Emission records: {len(all_emissions)}")
    print(f"Financial records: {len(all_financials)}")
    print(f"Energy records: {len(all_energy)}")
    print(f"Carbon credit records: {len(all_carbon_credits)}")
    
    output_dir = Path("exports")
    
    # Waste data
    if all_waste:
        waste_file = output_dir / "csr_waste_data.csv"
        with open(waste_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "material", "quantity_tons", "waste_type", "year", 
                "source_company", "context"
            ])
            writer.writeheader()
            for w in all_waste:
                writer.writerow({
                    "material": w.material,
                    "quantity_tons": w.quantity_tons,
                    "waste_type": w.waste_type,
                    "year": w.year,
                    "source_company": w.source_company,
                    "context": w.context,
                })
        print(f"\n[OK] Exported {len(all_waste)} waste records to {waste_file}")
    
    # Emissions
    if all_emissions:
        emissions_file = output_dir / "csr_emissions_data.csv"
        with open(emissions_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "emission_type", "value", "unit", "scope", "year",
                "source_company", "context"
            ])
            writer.writeheader()
            for e in all_emissions:
                writer.writerow({
                    "emission_type": e.emission_type,
                    "value": e.value,
                    "unit": e.unit,
                    "scope": e.scope,
                    "year": e.year,
                    "source_company": e.source_company,
                    "context": e.context,
                })
        print(f"[OK] Exported {len(all_emissions)} emission records to {emissions_file}")
    
    # Financials
    if all_financials:
        fin_file = output_dir / "csr_financial_data.csv"
        with open(fin_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "category", "value", "currency", "year",
                "source_company", "context"
            ])
            writer.writeheader()
            for fin in all_financials:
                writer.writerow({
                    "category": fin.category,
                    "value": fin.value,
                    "currency": fin.currency,
                    "year": fin.year,
                    "source_company": fin.source_company,
                    "context": fin.context,
                })
        print(f"[OK] Exported {len(all_financials)} financial records to {fin_file}")
    
    # Energy (NEW)
    if all_energy:
        energy_file = output_dir / "csr_energy_data.csv"
        with open(energy_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "category", "value", "unit", "year",
                "source_company", "context"
            ])
            writer.writeheader()
            for en in all_energy:
                writer.writerow({
                    "category": en.category,
                    "value": en.value,
                    "unit": en.unit,
                    "year": en.year,
                    "source_company": en.source_company,
                    "context": en.context,
                })
        print(f"[OK] Exported {len(all_energy)} energy records to {energy_file}")
    
    # Carbon Credits (NEW)
    if all_carbon_credits:
        cc_file = output_dir / "csr_carbon_credits.csv"
        with open(cc_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "category", "value", "unit", "year",
                "source_company", "context"
            ])
            writer.writeheader()
            for cc in all_carbon_credits:
                writer.writerow({
                    "category": cc.category,
                    "value": cc.value,
                    "unit": cc.unit,
                    "year": cc.year,
                    "source_company": cc.source_company,
                    "context": cc.context,
                })
        print(f"[OK] Exported {len(all_carbon_credits)} carbon credit records to {cc_file}")
    
    # Show samples
    if all_waste[:3]:
        print("\nSAMPLE WASTE DATA:")
        for w in all_waste[:3]:
            print(f"  {w.source_company}: {w.material} - {w.quantity_tons:,.0f} tons ({w.waste_type})")
    
    if all_emissions[:3]:
        print("\nSAMPLE EMISSIONS:")
        for e in all_emissions[:3]:
            print(f"  {e.source_company}: {e.emission_type} - {e.value:,.0f} {e.unit}")
    
    if all_financials[:3]:
        print("\nSAMPLE FINANCIALS:")
        for fin in all_financials[:3]:
            print(f"  {fin.source_company}: {fin.category} - ${fin.value:,.0f}")
    
    if all_energy[:3]:
        print("\nSAMPLE ENERGY:")
        for en in all_energy[:3]:
            print(f"  {en.source_company}: {en.category} - {en.value:,.0f} {en.unit}")
    
    if all_carbon_credits[:3]:
        print("\nSAMPLE CARBON CREDITS:")
        for cc in all_carbon_credits[:3]:
            print(f"  {cc.source_company}: {cc.category} - {cc.value:,.0f} {cc.unit}")
    
    return {
        "pdfs": len(pdfs),
        "waste": all_waste,
        "emissions": all_emissions,
        "financials": all_financials,
        "energy": all_energy,
        "carbon_credits": all_carbon_credits,
    }



if __name__ == "__main__":
    extract_all_csr_data()
