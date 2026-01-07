"""
E-PRTR Spider - European Pollutant Release and Transfer Register
=================================================================
Covers 27 EU countries, 91 pollutants, 33,000+ facilities
"""
import httpx
from pathlib import Path
from typing import Generator, List
from .base_spider import BaseSpider

class EPRTRSpider(BaseSpider):
    """Spider for European E-PRTR data."""
    
    name = "eprtr"
    source = "eprtr"
    
    # E-PRTR CSV downloads
    DATA_URLS = [
        "https://www.eea.europa.eu/data-and-maps/data/member-states-reporting-art-7-under-the-european-pollutant-release-and-transfer-register-e-prtr-regulation-23/e-prtr-releases/releases.csv/at_download/file",
    ]
    
    def get_urls(self) -> Generator[str, None, None]:
        """Yield E-PRTR download URLs."""
        for url in self.DATA_URLS:
            yield url
    
    def parse(self, response: httpx.Response, url: str):
        """Parse E-PRTR CSV response."""
        if response.status_code == 200:
            return self.save_raw(
                content=response.content,
                url=url,
                document_type="csv",
                metadata={"region": "EU", "source": "E-PRTR"}
            )
        return None
