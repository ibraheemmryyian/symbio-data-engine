"""
Symbio Data Engine - Petcoke Spider
===================================
Ingest petroleum coke and oil/gas industry data.
"""

from typing import Generator
import httpx
from .base_spider import BaseSpider


class PetcokeSpider(BaseSpider):
    """Crawl petroleum coke and oil/gas industry data sources."""

    name = "petcoke"
    source = "petcoke"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for petcoke and oil/gas data sources."""
        urls = [
            "https://www.iea.org/data-and-statistics/data-tools/",
            "https://www.opec.org/opecweb/en/publications/",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse petcoke response and save raw content."""
        if response.status_code != 200:
            return

        self.save_raw(
            content=response.content,
            url=url,
            document_type="html",
            metadata={
                "source_type": "petcoke",
                "year": 2024,
                "industry": "oil_gas",
                "byproduct_types": ["petroleum_coke"],
            }
        )
