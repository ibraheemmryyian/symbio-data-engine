import httpx

url = "https://data.europa.eu/data/datasets?allowed_in_country=EU&query=E-PRTR"
print(f"Checking access to {url}...")

try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(url)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print("✅ Data.Europa.EU is accessible.")
        else:
            print("❌ Blocked.")
except Exception as e:
    print(f"Error: {e}")
