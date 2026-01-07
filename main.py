#!/usr/bin/env python3
"""
Symbio Data Engine - CLI Entry Point
=====================================
The Library of Alexandria for Industrial Symbiosis

Usage:
    python main.py ingest symbioflows
    python main.py process --source wayback
    python main.py export research --format jsonl
    python main.py status
"""

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from datetime import datetime

from rich.logging import RichHandler
import logging

# Configure Logging to show Spider activity
# Use basic logging for wider compatibility if Rich misbehaves in some terminals
logging.basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("main.log", encoding='utf-8')
    ]
)

console = Console()
logger = logging.getLogger("main")


@click.group()
@click.version_option(version="0.1.0", prog_name="Symbio Data Engine")
def cli():
    """🏭 Symbio Data Engine - Industrial Symbiosis Data Pipeline"""
    pass


# ============================================
# INGEST COMMAND
# ============================================
@cli.command()
@click.argument("domain", type=click.Choice(["symbioflows", "symbiotrust", "research", "all"]))
@click.option("--source", "-s", type=click.Choice(["wayback", "gov", "csr", "scrap", "all"]), default="all")
@click.option("--limit", "-l", type=int, default=None, help="Limit number of documents to ingest")
@click.option("--dry-run", is_flag=True, help="Show what would be ingested without actually doing it")
def ingest(domain: str, source: str, limit: int, dry_run: bool):
    """
    Ingest data from external sources.
    
    DOMAIN: Target data domain (symbioflows, symbiotrust, research, all)
    """
    console.print(Panel(
        f"[bold green]Starting Ingestion[/bold green]\n"
        f"Domain: {domain}\n"
        f"Source: {source}\n"
        f"Limit: {limit or 'No limit'}\n"
        f"Dry Run: {dry_run}",
        title="🕷️ Spider Activation"
    ))
    
    if dry_run:
        console.print("[yellow]DRY RUN: No actual ingestion will occur[/yellow]")
        return
    
    # TODO: Implement actual spider orchestration
    from spiders import run_spider
    
    try:
        result = run_spider(domain=domain, source=source, limit=limit)
        console.print(f"[green]✓ Ingestion complete: {result['documents']} documents[/green]")
    except Exception as e:
        console.print(f"[red]✗ Ingestion failed: {e}[/red]")
        raise click.Abort()


# ============================================
# PROCESS COMMAND
# ============================================
@cli.command()
@click.option("--source", "-s", type=click.Choice(["wayback", "gov", "csr", "scrap", "eprtr", "mena", "all"]), default="all")
@click.option("--reprocess", is_flag=True, help="Reprocess already processed documents")
@click.option("--batch-size", "-b", type=int, default=100, help="Batch size for processing")
@click.option("--continuous", "-c", is_flag=True, help="Run in continuous 'Night Mode' (loop forever)")
def process(source: str, reprocess: bool, batch_size: int, continuous: bool):
    """
    Process raw documents through the cleaning pipeline.
    
    Pipeline: Raw → Clean → Normalize → Extract → Store
    """
    import time
    
    console.print(Panel(
        f"[bold blue]Starting Processing Pipeline[/bold blue]\n"
        f"Source: {source}\n"
        f"Mode: {'🌙 NIGHT MODE (Continuous)' if continuous else 'Run Once'}\n"
        f"Batch Size: {batch_size}",
        title="⚙️ Processor Activation"
    ))
    
    from processors import run_pipeline
    
    while True:
        try:
            result = run_pipeline(source=source, reprocess=reprocess, batch_size=batch_size)
            
            processed = result['processed']
            if processed > 0:
                console.print(f"[green]✓ Processed {processed} documents[/green]")
            else:
                if not continuous:
                    console.print("[yellow]No pending documents found.[/yellow]")
                    break
                
                # Sleep if continuous and no data
                with console.status("[dim]🌙 Night Mode: Waiting for new data...[/dim]", spinner="moon"):
                    time.sleep(10)
                    
            if not continuous:
                break
                
        except KeyboardInterrupt:
            console.print("\n[yellow]🛑 Stopping pipeline...[/yellow]")
            break
        except Exception as e:
            console.print(f"[red]✗ Pipeline error: {e}[/red]")
            if not continuous:
                raise click.Abort()
            time.sleep(5)  # Backoff on error


# ============================================
# EXPORT COMMAND
# ============================================
@cli.command()
@click.argument("domain", type=click.Choice(["symbioflows", "symbiotrust", "research", "unified"]))
@click.option("--format", "-f", "output_format", type=click.Choice(["jsonl", "parquet", "csv"]), default="jsonl")
@click.option("--output", "-o", type=click.Path(), default=None, help="Custom output path")
def export(domain: str, output_format: str, output: str):
    """
    Export processed data for LLM training or analysis.
    
    DOMAIN: Data domain to export
    """
    from pathlib import Path
    import json
    import config
    
    output_path = Path(output) if output else config.EXPORTS_DIR / domain / f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{output_format}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    console.print(Panel(
        f"[bold magenta]Starting Export[/bold magenta]\n"
        f"Domain: {domain}\n"
        f"Format: {output_format}\n"
        f"Output: {output_path}",
        title="📤 Export Activation"
    ))
    
    try:
        from store.postgres import execute_query
        
        # Determine which table(s) to export
        if domain == "symbioflows":
            query = "SELECT * FROM waste_listings ORDER BY year DESC, source_company ASC, created_at DESC"
        elif domain == "symbiotrust":
            query = "SELECT * FROM carbon_emissions ORDER BY year DESC, company ASC, created_at DESC"
        elif domain == "research":
            query = "SELECT * FROM symbiosis_exchanges ORDER BY year DESC, source_company ASC, created_at DESC"
        else:  # unified
            # Combined export for LLM training
            query = """
                SELECT * FROM (
                    SELECT 'waste' as record_type, material as content, source_company, year, extraction_confidence
                    FROM waste_listings
                    UNION ALL
                    SELECT 'carbon' as record_type, company as content, NULL as source_company, year, extraction_confidence
                    FROM carbon_emissions
                    UNION ALL
                    SELECT 'symbiosis' as record_type, material as content, source_company, year, extraction_confidence
                    FROM symbiosis_exchanges
                ) as unified_data
                ORDER BY year DESC, record_type ASC
            """
        
        records = execute_query(query) or []
        
        if not records:
            console.print("[yellow]⚠ No records found to export[/yellow]")
            return
        
        # Export based on format
        if output_format == "jsonl":
            with open(output_path, "w", encoding="utf-8") as f:
                for record in records:
                    # Convert non-serializable types
                    clean_record = {}
                    for k, v in record.items():
                        if hasattr(v, 'isoformat'):
                            clean_record[k] = v.isoformat()
                        elif isinstance(v, (int, float, str, bool, type(None))):
                            clean_record[k] = v
                        else:
                            clean_record[k] = str(v)
                    f.write(json.dumps(clean_record) + "\n")
        
        elif output_format == "csv":
            import csv
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                if records:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    for record in records:
                        writer.writerow({k: str(v) if v is not None else "" for k, v in record.items()})
        
        elif output_format == "parquet":
            try:
                import pandas as pd
                df = pd.DataFrame(records)
                df.to_parquet(output_path, index=False)
            except ImportError:
                console.print("[red]✗ Parquet export requires pandas: pip install pandas pyarrow[/red]")
                return
        
        console.print(f"[green]✓ Export complete: {len(records)} records → {output_path}[/green]")
        
    except Exception as e:
        console.print(f"[red]✗ Export failed: {e}[/red]")
        raise click.Abort()


# ============================================
# STATUS COMMAND
# ============================================
@cli.command()
@click.option("--verbose", "-v", is_flag=True, help="Show detailed statistics")
def status(verbose: bool):
    """
    Show current pipeline status and statistics.
    """
    console.print(Panel("[bold cyan]Pipeline Status[/bold cyan]", title="📊 Status"))
    
    # Fetch actual statistics from database
    try:
        from store.postgres import get_pipeline_stats
        stats = get_pipeline_stats()
    except Exception as e:
        console.print(f"[yellow]⚠ Could not connect to database: {e}[/yellow]")
        stats = {}
    
    # Create status table
    table = Table(title="Data Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right", style="green")
    table.add_column("Last Updated", style="yellow")
    
    table.add_row("Raw Documents", str(stats.get("total_documents", 0)), "-")
    table.add_row("Processed Documents", str(stats.get("processed_documents", 0)), "-")
    table.add_row("Pending Documents", str(stats.get("pending_documents", 0)), "-")
    table.add_row("Stuck Documents", str(stats.get("stuck_documents", 0)), "-")
    table.add_row("Waste Listings", str(stats.get("waste_listings", 0)), "-")
    table.add_row("Carbon Records", str(stats.get("carbon_records", 0)), "-")
    table.add_row("Symbiosis Exchanges", str(stats.get("symbiosis_exchanges", 0)), "-")
    table.add_row("Open Fraud Flags", str(stats.get("open_fraud_flags", 0)), "-")
    
    console.print(table)
    
    if verbose:
        # Show per-source breakdown
        try:
            from store.postgres import execute_query
            source_stats = execute_query("""
                SELECT source, status, COUNT(*) as count
                FROM documents
                GROUP BY source, status
                ORDER BY source, status
            """) or []
            
            source_table = Table(title="Per-Source Statistics")
            source_table.add_column("Source", style="cyan")
            source_table.add_column("Status", style="yellow")
            source_table.add_column("Count", justify="right")
            
            for row in source_stats:
                source_table.add_row(
                    row.get("source", "unknown"),
                    row.get("status", "unknown"),
                    str(row.get("count", 0))
                )
            
            console.print(source_table)
        except Exception as e:
            console.print(f"[yellow]⚠ Could not fetch source stats: {e}[/yellow]")


# ============================================
# INIT COMMAND
# ============================================
@cli.command()
@click.option("--reset", is_flag=True, help="Reset database (WARNING: deletes all data)")
@click.confirmation_option(prompt="This will initialize the database. Continue?")
def init(reset: bool):
    """
    Initialize database schemas and directories.
    """
    console.print("[bold]Initializing Symbio Data Engine...[/bold]")
    
    # Create directories
    import config
    console.print(f"  ✓ Data directory: {config.DATA_DIR}")
    console.print(f"  ✓ Raw directory: {config.RAW_DIR}")
    console.print(f"  ✓ Processed directory: {config.PROCESSED_DIR}")
    console.print(f"  ✓ Exports directory: {config.EXPORTS_DIR}")
    console.print(f"  ✓ Logs directory: {config.LOG_DIR}")
    
    # Initialize database
    try:
        from store.postgres import init_database
        init_database(reset=reset)
        console.print("  ✓ PostgreSQL database initialized")
    except Exception as e:
        console.print(f"  [yellow]⚠ PostgreSQL: {e}[/yellow]")
    
    # Initialize ChromaDB
    try:
        from store.vectors import init_vectorstore
        init_vectorstore()
        console.print("  ✓ ChromaDB vector store initialized")
    except Exception as e:
        console.print(f"  [yellow]⚠ ChromaDB: {e}[/yellow]")
    
    console.print("\n[green]✓ Initialization complete![/green]")


if __name__ == "__main__":
    cli()
