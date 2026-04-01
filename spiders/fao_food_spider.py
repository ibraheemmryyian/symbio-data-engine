"""
Symbio Data Engine - FAO Food Spider
====================================
Ingest food waste and agricultural byproduct data from FAO.
"""

from typing import Generator
import httpx
from .base_spider import BaseSpider


class FAOFoodSpider(BaseSpider):
    """Crawl FAO sources for food waste and agricultural byproduct data."""

    name = "fao"
    source = "fao"
    allowed_domains = ["fao.org", "www.fao.org"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (SymbioFlows FAO Data Ingestion)"
        })

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for FAO data sources."""
        urls = [
            "https://www.fao.org/fileadmin/templates/food_loss_waste/docs/FLW_Database_Updated_2024.csv",
            "https://www.fao.org/documents/card/en/c/CB6016EN/",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse FAO response and save raw content."""
        if response.status_code != 200:
            self.logger.warning(f"Failed to fetch {url}: {response.status_code}")
            return

        self.save_raw(
            content=response.content,
            url=url,
            document_type="csv" if url.endswith(".csv") else "html",
            metadata={
                "source_type": "fao",
                "year": 2024,
                "industry": "food",
                "byproduct_types": ["food_waste", "whey", "spent_grain", "press_cake"],
            }
        )
