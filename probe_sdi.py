import httpx

links = [
    "https://sdi.eea.europa.eu/data/3461f4ab-a3ee-4af2-bc11-95e651a8d0ba",
    "https://sdi.eea.europa.eu/data/9f373400-35b7-4978-9a34-a3cf839e053f",
    "https://sdi.eea.europa.eu/data/dc7bbfa4-4bf4-40d0-ad38-737a26ed9a76",
    "https://sdi.eea.europa.eu/data/ff47e25d-5d4c-491d-b9ce-de17ca61fe6d"
]

print("🔎 PROBING SDI LINKS FOR DATA...")
with httpx.Client(timeout=30.0, follow_redirects=True) as client:
    for url in links:
        try:
            print(f"\nTarget: {url}")
            resp = client.head(url)
            ct = resp.headers.get("Content-Type", "Unknown")
            cl = resp.headers.get("Content-Length", "Unknown")
            print(f"   Status: {resp.status_code}")
            print(f"   Type:   {ct}")
            print(f"   Size:   {cl} bytes")
            
            if "zip" in ct or "octet-stream" in ct or "access" in ct or "excel" in ct:
                print("   ✅ MATCH! AUTOMATION POSSIBLE.")
        except Exception as e:
            print(f"   ❌ Error: {e}")
