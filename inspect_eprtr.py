from store.postgres import execute_query
from rich.console import Console

console = Console()

print("🔎 INSPECTING E-PRTR DATABASE RECORDS...")

# Fetch 3 records linked to E-PRTR documents
recs = execute_query("""
    SELECT source_company, source_location, material, quantity_tons, year
    FROM waste_listings w
    JOIN documents d ON w.document_id = d.id
    WHERE d.source = 'eprtr'
    LIMIT 3
""")

if not recs:
    console.print("[bold red]❌ NO E-PRTR RECORDS FOUND IN DB![/bold red]")
else:
    console.print(f"[bold green]✅ FOUND {len(recs)} RECORDS (Sample):[/bold green]")
    for r in recs:
        console.print(f"🏭 {r['source_company']}")
        console.print(f"   📍 {r['source_location']}")
        console.print(f"   📦 {r['material']}")
        console.print(f"   ⚖️ {r['quantity_tons']}")
        console.print("-" * 20)
