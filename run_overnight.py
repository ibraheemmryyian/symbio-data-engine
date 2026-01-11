"""
OVERNIGHT CSR PIPELINE
======================
Runs the full CSR data collection and processing pipeline.
Estimated runtime: 4-8 hours depending on network/servers.

Usage: python run_overnight.py
"""
import asyncio
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

def log(msg):
    """Print with timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def run_command(cmd, description):
    """Run a command and return success status."""
    log(f"STARTING: {description}")
    start = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=False,  # Show output in real-time
            text=True
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:
            log(f"COMPLETED: {description} ({elapsed/60:.1f} min)")
            return True
        else:
            log(f"FAILED: {description} (exit code {result.returncode})")
            return False
    except Exception as e:
        log(f"ERROR: {description} - {e}")
        return False


def main():
    print("="*70)
    print("OVERNIGHT CSR PIPELINE")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results = {}
    
    # Count current PDFs
    pdf_dir = Path("data/raw/csr_reports")
    pdfs_before = len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0
    log(f"Current PDFs: {pdfs_before}")
    
    # Step 1: Run the CSR Spider
    print("\n" + "="*70)
    print("PHASE 1: CSR SPIDER (Download Reports)")
    print("="*70)
    results["spider"] = run_command(
        "python global_csr_spider.py",
        "Global CSR Spider (249 companies × 10 years)"
    )
    
    # Count new PDFs
    pdfs_after = len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0
    new_pdfs = pdfs_after - pdfs_before
    log(f"New PDFs downloaded: {new_pdfs}")
    log(f"Total PDFs: {pdfs_after}")
    
    # Step 2: Run Extraction
    print("\n" + "="*70)
    print("PHASE 2: CSR EXTRACTION (Process PDFs)")
    print("="*70)
    results["extraction"] = run_command(
        "python extract_all_csr.py",
        "CSR Extraction (5 categories: waste, emissions, financials, energy, carbon credits)"
    )
    
    # Step 3: Update pricing data
    print("\n" + "="*70)
    print("PHASE 3: UPDATE PRICING EXPORT")
    print("="*70)
    results["pricing"] = run_command(
        "python build_pricing_export.py",
        "Rebuild industry_pricing.json with latest data"
    )
    
    # Summary
    print("\n" + "="*70)
    print("OVERNIGHT PIPELINE COMPLETE")
    print("="*70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Results:")
    for step, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"  {step}: {status}")
    
    print()
    print(f"PDFs: {pdfs_before} → {pdfs_after} (+{new_pdfs} new)")
    
    # Check export files
    exports = [
        "exports/csr_waste_data.csv",
        "exports/csr_emissions_data.csv",
        "exports/csr_financial_data.csv",
        "exports/csr_energy_data.csv",
        "exports/csr_carbon_credits.csv",
        "exports/industry_pricing.json"
    ]
    
    print("\nExport files:")
    for exp in exports:
        p = Path(exp)
        if p.exists():
            size = p.stat().st_size
            print(f"  ✅ {exp} ({size:,} bytes)")
        else:
            print(f"  ❌ {exp} (missing)")
    
    # Save run log
    log_data = {
        "started": datetime.now().isoformat(),
        "pdfs_before": pdfs_before,
        "pdfs_after": pdfs_after,
        "new_pdfs": new_pdfs,
        "results": {k: v for k, v in results.items()}
    }
    
    import json
    with open("overnight_run.log", "w") as f:
        json.dump(log_data, f, indent=2)
    print(f"\nLog saved to: overnight_run.log")


if __name__ == "__main__":
    main()
