import httpx
import re

# Parent Page for E-PRTR v23
BASE_URL = "https://www.eea.europa.eu/data-and-maps/data/member-states-reporting-art-7-under-the-european-pollutant-release-and-transfer-register-e-prtr-regulation-23"

print(f"Connecting to {BASE_URL}...")
try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(BASE_URL)
        print(f"Status: {resp.status_code}")
        
        # Simple Regex to find links ending in .csv or .zip
        links = re.findall(r'href=["\'](https?://[^"\']+\.(?:csv|zip|xlsx))["\']', resp.text)
        
        print("\n🔎 Found Download Links:")
        unique_links = list(set(links))
        for l in unique_links:
            print(f"   -> {l}")
            
        with open("found_links.txt", "w", encoding="utf-8") as f:
            f.write("Found Links:\n")
            # Regex for href="..." capturing both absolute and relative
            dl_links = re.findall(r'href=["\']([^"\']+)["\']', resp.text)
            
            for l in set(dl_links):
                # Filter for download-like keywords
                if any(x in l.lower() for x in ["download", "file", "csv", "zip", "xlsx", "data"]):
                     # Resolve relative URL
                    if l.startswith("/"):
                        full_link = f"https://www.eea.europa.eu{l}"
                    elif l.startswith("http"):
                        full_link = l
                    else:
                        full_link = f"{BASE_URL}/{l}"
                    
                    f.write(f"{full_link}\n")
        
        print("Done. Saved to found_links.txt")

except Exception as e:
    print(f"Error: {e}")
