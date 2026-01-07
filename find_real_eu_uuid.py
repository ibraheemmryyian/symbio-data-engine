import httpx
import json

# Broader search to find the Real Title and UUID
API_URL = "https://data.europa.eu/api/hub/search/search"
PARAMS = {
    "q": "E-PRTR",
    "filter": "dataset",
    "limit": 5
}

print("🔍 Searching for 'E-PRTR' on Data.Europa API...")

try:
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(API_URL, params=PARAMS)
        print(f"Status: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("result", {}).get("results", [])
            
            with open("uuid_list.txt", "w", encoding="utf-8") as f:
                f.write(f"Found {len(results)} datasets:\n\n")
                for i, r in enumerate(results):
                    title = r.get("title", {}).get("en", "No Title")
                    uuid = r.get("id", "No ID")
                    publisher = r.get("publisher_name", {}).get("en", "Unknown Publisher")
                    
                    f.write(f"{i+1}. [{publisher}] {title}\n")
                    f.write(f"   UUID: {uuid}\n")
                    f.write(f"   Link: https://data.europa.eu/data/datasets/{uuid}?locale=en\n")
                    f.write("-" * 40 + "\n")
            print("Done. Saved to uuid_list.txt")
        else:
            print("❌ API Request Failed.")

except Exception as e:
    print(f"Error: {e}")
