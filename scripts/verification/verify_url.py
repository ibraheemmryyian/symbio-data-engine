import requests

url = "https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/2023_US/csv"
print(f"Testing: {url}")

try:
    with requests.get(url, stream=True, timeout=30) as r:
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type', 'N/A')}")
        if r.status_code == 200:
            bytes_read = 0
            for chunk in r.iter_content(chunk_size=8192):
                bytes_read += len(chunk)
                if bytes_read > 10000:
                    break
            print(f"SUCCESS! Downloaded {bytes_read} bytes (sample)")
        else:
            print("FAILED - non-200 status")
except Exception as e:
    print(f"Exception: {e}")
