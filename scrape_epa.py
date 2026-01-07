import requests
from bs4 import BeautifulSoup

url = "https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present"
print(f"Fetching {url}...")

try:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    r = requests.get(url, headers=headers, timeout=15)
    print(f"Status: {r.status_code}")
    
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Find all links
    links = soup.find_all('a', href=True)
    print(f"Found {len(links)} links.")
    
    # Filter for CSV
    csv_links = [l['href'] for l in links if "csv" in l['href'].lower() and "basic" in l['href'].lower()]
    
    print("\nPotential CSV Links found:")
    for l in csv_links[:10]:
        print(l)
    
    # Check for "2023" specifically
    y23 = [l for l in csv_links if "2023" in l]
    if y23:
        print(f"\n2023 Matching Link: {y23[0]}")
    else:
        print("\nNo 2023 CSV link found directly.")
        
except Exception as e:
    print(f"Error: {e}")
