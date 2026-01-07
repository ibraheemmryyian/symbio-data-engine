"""
🚀 TURBO MODE - CONCURRENT DATA COLLECTION
===========================================
5 parallel workers | All sources | Maximum speed
"""

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from spiders import run_spider

console = Console()

# TURBO CONFIG
WORKERS = 5  # Parallel downloads
INTERVAL_MINUTES = 5  # Run every 5 minutes

def collect_source(source: str, limit: int) -> tuple:
    """Collect from single source (called by worker)."""
    try:
        result = run_spider(domain="symbiotrust", source=source, limit=limit)
        return (source, result.get("documents", 0), None)
    except Exception as e:
        return (source, 0, str(e))

def collect_all_parallel():
    """Run all collections in parallel."""
    start = datetime.now()
    console.print(f"\n[bold cyan]🚀 TURBO COLLECTION - {WORKERS} PARALLEL WORKERS[/bold cyan]")
    console.print(f"   Started: {start.strftime('%H:%M:%S')}")
    
    # Define collection jobs - MENA + EU PRIORITY
    jobs = [
        ("mena", 100),   # UAE/Saudi - HOME TURF FIRST
        ("eprtr", 100),  # E-PRTR (EU) - PRIORITY
        ("gov", 25),     # EPA (USA) - continue light
    ]
    
    total_docs = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(collect_source, source, limit): (source, limit) 
            for source, limit in jobs
        }
        
        for future in as_completed(futures):
            source, docs, error = future.result()
            if error:
                console.print(f"   [red]✗ {source}: {error}[/red]")
            else:
                console.print(f"   [green]✓ {source}: {docs} docs[/green]")
                total_docs += docs
    
    elapsed = (datetime.now() - start).seconds
    console.print(f"\n[bold green]⚡ {total_docs} docs in {elapsed}s ({total_docs/max(elapsed,1):.1f} docs/sec)[/bold green]")
    return total_docs

def main():
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]")
    console.print("[bold magenta]   ⚡ TURBO MODE - CONCURRENT DATA COLLECTION[/bold magenta]")
    console.print("[bold magenta]   5 parallel workers | Every 5 minutes[/bold magenta]")
    console.print("[bold magenta]   Started: " + datetime.now().strftime('%Y-%m-%d %H:%M') + "[/bold magenta]")
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]")
    
    run_count = 0
    total_session = 0
    while True:
        run_count += 1
        console.print(f"\n[bold]═══ RUN #{run_count} ═══[/bold]")
        docs = collect_all_parallel()
        total_session += docs
        console.print(f"[dim]SESSION: {total_session} total docs | Next run in {INTERVAL_MINUTES} min[/dim]")
        time.sleep(INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    main()
