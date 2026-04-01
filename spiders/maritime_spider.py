"""
Symbio Data Engine - Maritime Spider
====================================
Ingest maritime waste, dredged sediments, and port data.
"""

from typing import Generator
import httpx
from .base_spider import BaseSpider


class MaritimeSpider(BaseSpider):
    """Crawl maritime, port, and dredging data sources."""

    name = "maritime"
    source = "maritime"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for maritime and port data sources."""
        urls = [
            "https://www.ospar.org/work-areas/coastal-protection/dredging",
            "https://www.imo.org/en/OurWork/Environment/Pages/Ballast-Water-Management.aspx",
            "https://www.portofrotterdam.com/en/port-information/facts-and-figures",
            "https://www.swcc.gov.sa/en/",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse maritime response and save raw content."""
        if response.status_code != 200:
            return

        self.save_raw(
            content=response.content,
            url=url,
            document_type="html",
            metadata={
                "source_type": "maritime",
                "year": 2024,
                "industry": "maritime",
                "byproduct_types": ["dredged_sediments", "desalination_brine", "ballast_residue"],
            }
        )
