"""
MEGA OVERNIGHT CSR PIPELINE
===========================
Runs ALL spiders for maximum data collection.

Target: 20,000+ PDFs

Spiders:
1. global_csr_spider.py - 488 companies × 10 years
2. multi_source_spider.py - Report databases
3. wayback_csr_spider.py - Historical archives

Estimated time: 6-12 hours
"""
import subprocess
import sys
import time
import json
from datetime import datetime
from pathlib import Path


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def count_pdfs():
    pdf_dir = Path("data/raw/csr_reports")
    return len(list(pdf_dir.glob("*.pdf"))) if pdf_dir.exists() else 0


def run_spider(name, script):
    log(f"STARTING: {name}")
    start = time.time()
    pdfs_before = count_pdfs()
    
    try:
        result = subprocess.run(
            [sys.executable, script],
            capture_output=False,
            text=True
        )
        success = result.returncode == 0
    except Exception as e:
        log(f"ERROR: {e}")
        success = False
    
    pdfs_after = count_pdfs()
    elapsed = (time.time() - start) / 60
    
    log(f"COMPLETED: {name} in {elapsed:.1f} min (+{pdfs_after - pdfs_before} PDFs)")
    return {
        "name": name,
        "success": success,
        "duration_min": round(elapsed, 1),
        "pdfs_added": pdfs_after - pdfs_before
    }


def main():
    start_time = datetime.now()
    pdfs_start = count_pdfs()
    
    print("="*70)
    print("MEGA OVERNIGHT CSR PIPELINE")
    print("="*70)
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Starting PDFs: {pdfs_start}")
    print()
    
    results = []
    
    # Spider 1: Global CSR Spider (company pages)
    print("\n" + "="*70)
    print("PHASE 1: GLOBAL CSR SPIDER (488 companies)")
    print("="*70)
    results.append(run_spider("Global CSR Spider", "global_csr_spider.py"))
    
    # Spider 2: Multi-Source Spider (databases)
    print("\n" + "="*70)
    print("PHASE 2: MULTI-SOURCE SPIDER (Report Databases)")
    print("="*70)
    results.append(run_spider("Multi-Source Spider", "multi_source_spider.py"))
    
    # Spider 3: Wayback Spider (historical)
    print("\n" + "="*70)
    print("PHASE 3: WAYBACK SPIDER (Historical Archives)")
    print("="*70)
    results.append(run_spider("Wayback Spider", "wayback_csr_spider.py"))
    
    # Phase 4: Run extraction on all PDFs
    print("\n" + "="*70)
    print("PHASE 4: CSR DATA EXTRACTION")
    print("="*70)
    results.append(run_spider("CSR Extraction", "extract_all_csr.py"))
    
    # Phase 5: Rebuild pricing data
    print("\n" + "="*70)
    print("PHASE 5: REBUILD PRICING DATA")
    print("="*70)
    results.append(run_spider("Pricing Export", "build_pricing_export.py"))
    
    # Final Summary
    pdfs_end = count_pdfs()
    total_time = (datetime.now() - start_time).total_seconds() / 60
    
    print("\n" + "="*70)
    print("MEGA PIPELINE COMPLETE")
    print("="*70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Time: {total_time:.1f} minutes ({total_time/60:.1f} hours)")
    print()
    print("Results by spider:")
    for r in results:
        status = "✅" if r["success"] else "❌"
        print(f"  {status} {r['name']}: {r['duration_min']} min, +{r['pdfs_added']} PDFs")
    print()
    print(f"PDFs: {pdfs_start} → {pdfs_end} (+{pdfs_end - pdfs_start} total new)")
    
    # Check exports
    print("\nExport files:")
    exports = [
        "exports/csr_waste_data.csv",
        "exports/csr_emissions_data.csv", 
        "exports/csr_financial_data.csv",
        "exports/csr_energy_data.csv",
        "exports/csr_carbon_credits.csv",
        "exports/industry_pricing.json"
    ]
    for exp in exports:
        p = Path(exp)
        if p.exists():
            size = p.stat().st_size
            print(f"  ✅ {exp} ({size:,} bytes)")
        else:
            print(f"  ❌ {exp} (missing)")
    
    # Save detailed log
    log_data = {
        "started": start_time.isoformat(),
        "finished": datetime.now().isoformat(),
        "total_minutes": round(total_time, 1),
        "pdfs_before": pdfs_start,
        "pdfs_after": pdfs_end,
        "new_pdfs": pdfs_end - pdfs_start,
        "results": results
    }
    
    with open("mega_overnight_run.log", "w") as f:
        json.dump(log_data, f, indent=2)
    print(f"\nDetailed log saved to: mega_overnight_run.log")


if __name__ == "__main__":
    main()
