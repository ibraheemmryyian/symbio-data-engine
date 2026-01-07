import httpx
import re

url = "https://data.gov.sa/Data/en/api/3/action/package_search?q=waste"
# Note: This endpoint returned HTML last time, so we scrape headers/links
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

print(f"Scouring {url} for links...")
try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(url, headers=headers)
        
        # Look for resource URLs
        # CKAN usually links to /dataset/...
        links = re.findall(r'href=["\'](https?://[^"\']+)["\']', resp.text)
        
        found = []
        for l in set(links):
            if "download" in l.lower() or "csv" in l.lower() or "xlsx" in l.lower():
                found.append(l)
                
        with open("mena_links.txt", "w", encoding="utf-8") as f:
            f.write("# MANUAL DOWNLOAD LINKS (MENA)\n")
            f.write("# Click these, verify content, and save to 'data/raw/mena/'\n\n")
            if not found:
                 f.write("# No direct links found. Try visiting: https://data.gov.sa/Data/en/search?q=waste\n")
            for l in found:
                f.write(f"{l}\n")
        
        print(f"Found {len(found)} potential download links.")

except Exception as e:
    print(f"Error: {e}")
