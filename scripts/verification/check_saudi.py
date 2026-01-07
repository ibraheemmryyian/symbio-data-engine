"""Quick check of Saudi Open Data API"""
import httpx

url = "https://data.gov.sa/Data/en/api/3/action/package_search?q=waste"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

try:
    print(f"Connecting to {url}...")
    resp = httpx.get(url, headers=headers, timeout=30.0, follow_redirects=True)
    print(f"Status: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        count = data.get('result', {}).get('count', 0)
        print(f"✅ Saudi Open Data Connection Successful!")
        print(f"   Found {count} datasets matching 'waste'")
        
        results = data.get('result', {}).get('results', [])[:3]
        for r in results:
            print(f"   - {r.get('title')}")
            for res in r.get('resources', []):
                 print(f"     -> {res.get('format')} : {res.get('url')}")
    else:
        print(f"⚠️ Failed: {resp.text[:200]}")

except Exception as e:
    print(f"❌ Error: {e}")
