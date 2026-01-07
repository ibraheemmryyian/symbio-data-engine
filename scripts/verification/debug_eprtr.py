import httpx

url = "https://www.eea.europa.eu/data-and-maps/data/member-states-reporting-art-7-under-the-european-pollutant-release-and-transfer-register-e-prtr-regulation-23/e-prtr-releases/releases.csv/at_download/file"

print(f"Connecting to {url}...")
try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(url)
        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('content-type')}")
        print(f"URL Final: {resp.url}")
        
        # Save first 1KB of content
        content = resp.text[:1000]
        print("\n--- CONTENT START ---")
        print(content)
        print("--- CONTENT END ---")
        
        with open("eprtr_debug.html", "w", encoding="utf-8") as f:
            f.write(resp.text)

except Exception as e:
    print(f"Error: {e}")
