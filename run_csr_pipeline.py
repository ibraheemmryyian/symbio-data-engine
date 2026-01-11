"""
Run full CSR pipeline: spider → PDF processor → extractor → database.
"""
import logging
from pathlib import Path

from spiders.csr_spider import CSRSpider
from processors.csr_extractor import CSRExtractor
from processors.pdf_processor import PDFProcessor
from store.postgres import execute_query, get_connection, insert_waste_listing, insert_carbon_emission

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def run_csr_pipeline(limit: int = 5, store_results: bool = False):
    """
    Full CSR data extraction pipeline.
    
    1. Download CSR PDFs (if not already downloaded)
    2. Extract text from each PDF
    3. Run extractors for waste, emissions, financials
    4. Optionally store in database
    """
    
    print("="*60)
    print("CSR DATA EXTRACTION PIPELINE")
    print("="*60)
    
    # Check for existing PDFs first
    raw_dir = Path("data/raw/csr_reports")
    existing_pdfs = list(raw_dir.glob("*.pdf")) if raw_dir.exists() else []
    
    if existing_pdfs:
        print(f"\nFound {len(existing_pdfs)} existing PDFs in {raw_dir}")
        pdfs_to_process = existing_pdfs[:limit]
    else:
        print("\nNo existing PDFs found. Running CSR spider...")
        spider = CSRSpider(domain="symbiotrust", limit=limit)
        results = spider.run()
        print(f"Spider fetched {results.get('documents', 0)} documents")
        pdfs_to_process = list(raw_dir.glob("*.pdf")) if raw_dir.exists() else []
    
    if not pdfs_to_process:
        print("No PDFs to process. Exiting.")
        return
    
    # Initialize extractor
    extractor = CSRExtractor()
    
    # Process each PDF
    all_waste = []
    all_emissions = []
    all_financials = []
    
    for pdf_path in pdfs_to_process:
        print(f"\n{'─'*40}")
        print(f"Processing: {pdf_path.name}")
        print(f"{'─'*40}")
        
        # Determine company from path or filename
        company = "unknown"
        for comp in ["borouge", "adnoc", "sabic"]:
            if comp in pdf_path.name.lower() or comp in str(pdf_path).lower():
                company = comp
                break
        
        # Extract data
        results = extractor.extract_from_pdf(pdf_path, company)
        
        print(f"  Year: {results.get('year')}")
        print(f"  Waste records: {len(results['waste_data'])}")
        print(f"  Emission records: {len(results['emissions'])}")
        print(f"  Financial records: {len(results['financials'])}")
        
        all_waste.extend(results['waste_data'])
        all_emissions.extend(results['emissions'])
        all_financials.extend(results['financials'])
    
    # Summary
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)
    print(f"PDFs processed: {len(pdfs_to_process)}")
    print(f"Waste records: {len(all_waste)}")
    print(f"Emission records: {len(all_emissions)}")
    print(f"Financial records: {len(all_financials)}")
    
    # Show samples
    if all_waste:
        print("\nSample Waste Data:")
        for w in all_waste[:3]:
            print(f"  - {w.material}: {w.quantity_tons:,.0f} tons ({w.waste_type})")
    
    if all_emissions:
        print("\nSample Emissions:")
        for e in all_emissions[:3]:
            print(f"  - {e.emission_type}: {e.value:,.0f} {e.unit}")
    
    if all_financials:
        print("\nSample Financials:")
        for f in all_financials[:3]:
            print(f"  - {f.category}: ${f.value:,.0f}")
    
    # Store results if requested
    if store_results and (all_waste or all_emissions):
        print("\n" + "="*60)
        print("STORING RESULTS")
        print("="*60)
        
        waste_stored = 0
        emission_stored = 0
        
        # Store waste listings
        for w in all_waste:
            try:
                insert_waste_listing({
                    "material": w.material,
                    "quantity_tons": w.quantity_tons,
                    "source_company": w.source_company,
                    "year": w.year,
                    "source": "csr_report",
                    "metadata": {"context": w.context, "waste_type": w.waste_type},
                })
                waste_stored += 1
            except Exception as e:
                logger.debug(f"Failed to store waste: {e}")
        
        # Store emissions
        for e in all_emissions:
            try:
                insert_carbon_emission({
                    "company": e.source_company,
                    "year": e.year,
                    "scope": e.scope or "unknown",
                    "emissions_tons": e.value if e.unit in ["tonnes", "tons", "tCO2e"] else None,
                    "source": "csr_report",
                    "metadata": {"type": e.emission_type, "context": e.context},
                })
                emission_stored += 1
            except Exception as e:
                logger.debug(f"Failed to store emission: {e}")
        
        print(f"Waste records stored: {waste_stored}")
        print(f"Emission records stored: {emission_stored}")
    
    return {
        "pdfs_processed": len(pdfs_to_process),
        "waste": all_waste,
        "emissions": all_emissions,
        "financials": all_financials,
    }


if __name__ == "__main__":
    import sys
    
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    store = "--store" in sys.argv
    
    run_csr_pipeline(limit=limit, store_results=store)
