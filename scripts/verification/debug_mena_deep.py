import httpx
from rich.console import Console

console = Console()

def debug_mena():
    url = "https://bayanat.ae/api/explore/v2.1/catalog/datasets?where=theme%3D%22Environment%22&limit=50"
    console.print(f"[bold cyan]🔍 Debugging MENA API:[/bold cyan] {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    try:
        # standard timeout
        client = httpx.Client(timeout=30.0, follow_redirects=True)
        response = client.get(url, headers=headers)
        
        console.print(f"\n[bold]Status Code:[/bold] {response.status_code}")
        console.print(f"[bold]Content-Type:[/bold] {response.headers.get('content-type', 'N/A')}")
        console.print(f"[bold]Server:[/bold] {response.headers.get('server', 'N/A')}")
        
        if response.status_code != 200:
            console.print(f"[bold red]❌ Failed![/bold red]")
        
        # Save content to file for inspection
        with open("mena_test.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        console.print("\n[bold]Content Saved to mena_test.html[/bold]")
        
        # Try JSON parse
        try:
            data = response.json()
            console.print(f"\n[bold green]✅ JSON Parsed![/bold green] Found {len(data.get('results', []))} results")
        except Exception as e:
            console.print(f"\n[bold red]❌ JSON Parse Error:[/bold red] {e}")

    except Exception as e:
        console.print(f"[bold red]❌ Connection Error:[/bold red] {e}")

if __name__ == "__main__":
    debug_mena()
