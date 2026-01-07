"""Test MENA API endpoints for automated collection."""
import httpx

def test_bayanat():
    print("Testing UAE Bayanat API...")
    try:
        r = httpx.get(
            "https://bayanat.ae/api/explore/v2.1/catalog/datasets",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        print(f"  Status: {r.status_code}")
        print(f"  Content-Type: {r.headers.get('content-type', 'unknown')}")
        print(f"  Body length: {len(r.text)} chars")
        
        if r.text and r.headers.get('content-type', '').startswith('application/json'):
            data = r.json()
            print(f"  Total datasets: {data.get('total_count', 0)}")
            for ds in data.get("results", [])[:3]:
                print(f"    - {ds.get('dataset_id')}: {ds.get('metas', {}).get('default', {}).get('title', 'No title')}")
            return True
        else:
            print(f"  Response preview: {r.text[:200]}")
            return False
    except Exception as e:
        print(f"  Error: {e}")
        return False

def test_saudi():
    print("\nTesting Saudi Open Data API...")
    try:
        r = httpx.get(
            "https://data.gov.sa/Data/en/api/3/action/package_search?q=waste",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        print(f"  Status: {r.status_code}")
        print(f"  Content-Type: {r.headers.get('content-type', 'unknown')}")
        
        if r.text.startswith("{"):
            data = r.json()
            results = data.get("result", {}).get("results", [])
            print(f"  Datasets found: {len(results)}")
            for pkg in results[:3]:
                print(f"    - {pkg.get('title', 'No title')}")
            return True
        else:
            print(f"  WAF/Blocked: {r.text[:150]}")
            return False
    except Exception as e:
        print(f"  Error: {e}")
        return False

if __name__ == "__main__":
    bayanat_ok = test_bayanat()
    saudi_ok = test_saudi()
    
    print("\n" + "="*50)
    print("MENA AUTO-COLLECTION STATUS:")
    print(f"  UAE Bayanat: {'✅ AVAILABLE' if bayanat_ok else '❌ BLOCKED'}")
    print(f"  Saudi Open Data: {'✅ AVAILABLE' if saudi_ok else '❌ BLOCKED'}")
