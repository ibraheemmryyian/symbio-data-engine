"""
Symbio Data Engine - Plastics Exchange Spider
==============================================
Ingest plastics recycling and scrap market data.
"""

from typing import Generator
import httpx
from .base_spider import BaseSpider


class PlasticsExchangeSpider(BaseSpider):
    """Crawl plastics industry sources for scrap and recycling data."""

    name = "plastics"
    source = "plastics_exchange"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (SymbioFlows Plastics Market Data)"
        })

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for plastics market data sources."""
        urls = [
            "https://www.plasticseurope.org/en/resources/publications",
            "https://ellenmacarthurfoundation.org/resources/reports",
            "https://www.wrap.org.uk/resources/reports",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse plastics exchange response and save raw content."""
        if response.status_code != 200:
            return

        self.save_raw(
            content=response.content,
            url=url,
            document_type="html",
            metadata={
                "source_type": "plastics_exchange",
                "year": 2024,
                "industry": "plastics",
                "byproduct_types": ["plastic_scrap", "regrind", "film_laminates"],
            }
        )
