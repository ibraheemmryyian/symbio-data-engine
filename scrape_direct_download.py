import httpx
import re

# The URL from the user's screenshot context (The "Holy Grail" link I provided)
TARGET_URL = "https://www.eea.europa.eu/en/datahub/datahubitem-view/9405f714-8015-4b5b-a63c-280b82861b3d"

print(f"🕵️ SCRAPING TARGET: {TARGET_URL}")

try:
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        resp = client.get(TARGET_URL)
        print(f"Status: {resp.status_code}")
        
        # Look for the specific "Direct download" pattern or the file link
        # In the screenshot, it says "Direct download". 
        # The link usually points to a .zip or .xlsx or .csv
        
        # Regex to find hrefs near "Direct download" or just all file links
        file_links = re.findall(r'href=["\']([^"\']+\.(?:zip|xlsx|csv|mdb|accdb))["\']', resp.text)
        
        with open("direct_links.txt", "w", encoding="utf-8") as f:
            f.write("Found Direct Links:\n")
            unique_links = list(set(file_links))
            for l in unique_links:
                f.write(f"{l}\n")
            
            sdi_links = re.findall(r'href=["\'](https?://sdi\.eea\.europa\.eu/[^"\']+)["\']', resp.text)
            for l in set(sdi_links):
                f.write(f"{l}\n")
        
        print("Done. Saved to direct_links.txt")

except Exception as e:
    print(f"Error: {e}")
