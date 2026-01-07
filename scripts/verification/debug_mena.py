import httpx

print("Testing Bayanat API connection...")
url = "https://bayanat.ae/api/explore/v2.1/catalog/datasets?where=theme%3D%22Environment%22&limit=50"

try:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = httpx.get(url, headers=headers, timeout=30.0, follow_redirects=True)
    print(f"Status Code: {response.status_code}")
    print(f"Content Type: {response.headers.get('content-type')}")
    print(f"Content Preview: {response.text[:200]}")
    
    data = response.json()
    print("✅ JSON parsed successfully")
    print(f"Found {len(data.get('results', []))} datasets")
    
except Exception as e:
    print(f"❌ Error: {e}")
