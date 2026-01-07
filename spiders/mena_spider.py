"""
MENA Spider - UAE Bayanat & Saudi Data  
======================================
Middle East waste data for home turf advantage
"""
import httpx
from typing import Generator
from .base_spider import BaseSpider

class MENASpider(BaseSpider):
    """Spider for UAE/Saudi environmental data."""
    
    name = "mena"
    source = "mena"
    
    # UAE Bayanat
    BAYANAT_API = "https://bayanat.ae/api/explore/v2.1/catalog/datasets"
    # Saudi Open Data (CKAN)
    SAUDI_API = "https://data.gov.sa/Data/en/api/3/action/package_search"

    def get_urls(self) -> Generator[str, None, None]:
        """Yield MENA data URLs (UAE + Saudi)."""
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }

        # 1. SAUDI ARABIA (Open Data)
        try:
            # Search for 'waste' datasets
            resp = self.session.get(f"{self.SAUDI_API}?q=waste", headers=headers, timeout=30.0)
            if resp.status_code == 200:
                data = resp.json()
                for pkg in data.get('result', {}).get('results', []):
                    for res in pkg.get('resources', []):
                        if res.get('format', '').lower() == 'csv':
                            yield res['url']
        except Exception as e:
            print(f"   ⚠️ Saudi API Error: {e}")

        # 2. UAE (Bayanat)
        try:
            response = self.session.get(
                f"{self.BAYANAT_API}?where=theme%3D%22Environment%22&limit=50",
                headers=headers,
                timeout=60.0
            )
            if response.status_code == 200:
                data = response.json()
                for ds in data.get("results", []):
                    ds_id = ds.get("dataset_id")
                    if ds_id:
                        yield f"{self.BAYANAT_API}/{ds_id}/exports/csv"
            else:
                print(f"   ⚠️ UAE (Bayanat) Blocked: Status {response.status_code}")
        except Exception as e:
            # Handle non-JSON response gracefully
            print(f"   ⚠️ MENA (Bayanat): API unavailable or blocking (Retrying next cycle)")
            pass
    
    def parse(self, response: httpx.Response, url: str):
        """Parse MENA CSV response."""
        if response.status_code == 200 and len(response.content) > 100:
            return self.save_raw(
                content=response.content,
                url=url,
                document_type="csv",
                metadata={"region": "UAE", "source": "Bayanat"}
            )
        return None
