import httpx

links = [
    "https://sdi.eea.europa.eu/data/dc7bbfa4-4bf4-40d0-ad38-737a26ed9a76",
    "https://sdi.eea.europa.eu/data/3461f4ab-a3ee-4af2-bc11-95e651a8d0ba",
    "https://sdi.eea.europa.eu/data/9f373400-35b7-4978-9a34-a3cf839e053f"
]

print("🔎 CHECKING SDI LINKS...")
with httpx.Client(timeout=30.0, follow_redirects=True) as client:
    for url in links:
        try:
            print(f"\nLink: {url}")
            resp = client.head(url)
            ct = resp.headers.get("Content-Type", "Unknown")
            cl = resp.headers.get("Content-Length", "Unknown")
            print(f"   Status: {resp.status_code}")
            print(f"   Type:   {ct}")
            print(f"   Size:   {cl} bytes")
            if "zip" in ct or "csv" in ct or "application/octet-stream" in ct:
                print("   ✅ MATCH! This is a data file.")
        except Exception as e:
            print(f"   ❌ Error: {e}")
