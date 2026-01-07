import httpx

url = "https://www.eea.europa.eu/en/datahub/datahubitem-view/9405f714-8015-4b5b-a63c-280b82861b3d"

print(f"Checking URL: {url} ...")
try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(url)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print("✅ Valid Link! This is likely the dataset page.")
            # Verify if it contains "Industrial Reporting"
            if "Industrial" in resp.text:
                print("✅ Found keyword 'Industrial' in page content.")
        else:
            print("❌ Invalid link.")

except Exception as e:
    print(f"Error: {e}")
