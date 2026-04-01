"""
Symbio Data Engine - WorldSteel Spider
======================================
Ingest steel industry byproduct and scrap market data.
"""

from typing import Generator
import httpx
from .base_spider import BaseSpider


class WorldSteelSpider(BaseSpider):
    """Crawl WorldSteel.org for steel byproduct and market data."""

    name = "worldsteel"
    source = "worldsteel"
    allowed_domains = ["worldsteel.org"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (SymbioFlows Industrial Data Ingestion)"
        })

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for WorldSteel data sources."""
        urls = [
            "https://www.worldsteel.org/en/dam/jcr:e47cf1ce-0ef4-4aa1-8bf0-debf2da4a66d/World%20Steel%20in%20Figures%202024.xlsx",
            "https://www.worldsteel.org/en/dam/jcr:9ae2c3f2-c3a2-48dd-9f03-1f8e9e3c3c3c/Scrap%20Market%20Data%202023.csv",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse WorldSteel response and save raw content."""
        if response.status_code != 200:
            self.logger.warning(f"Failed to fetch {url}: {response.status_code}")
            return

        if url.endswith(".xlsx"):
            self.logger.info(f"Skipping XLSX: {url}")
            return

        self.save_raw(
            content=response.content,
            url=url,
            document_type="csv" if url.endswith(".csv") else "html",
            metadata={
                "source_type": "worldsteel",
                "year": 2024,
                "industry": "steel",
                "byproduct_types": ["slag", "eaf_dust", "mill_scale"],
            }
        )
