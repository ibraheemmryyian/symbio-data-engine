"""
Symbio Data Engine - Scrap Exchange Spider
==========================================
Fetch real-time scrap metal and recyclable material pricing data.

Sources:
- ScrapMonster
- Metal Bulletin
- ISRI (Institute of Scrap Recycling Industries)
- LME (London Metal Exchange) - for reference prices
"""

import logging
import re
from datetime import datetime
from typing import Generator, Optional

import httpx
from bs4 import BeautifulSoup

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)


class ScrapExchangeSpider(BaseSpider):
    """
    Spider for real-time scrap and recyclable material pricing.
    
    This spider runs more frequently than historical spiders
    to capture current market prices for SymbioFlows.
    """
    
    name = "scrap"
    source = "scrap_exchange"
    
    # Pricing sources
    SOURCES = {
        "scrapmonster": {
            "name": "ScrapMonster",
            "base_url": "https://www.scrapmonster.com",
            "price_paths": [
                "/scrap-prices/united-states",
                "/scrap-prices/europe",
                "/scrap-prices/asia",
            ],
            "materials": ["copper", "aluminum", "steel", "iron", "brass"],
        },
        "recycling_markets": {
            "name": "Recycling Markets",
            "base_url": "https://www.recyclingmarkets.net",
            "price_paths": [
                "/prices",
            ],
            "materials": ["paper", "plastic", "glass", "metals"],
        },
    }
    
    def __init__(
        self,
        domain: str = "symbioflows",
        limit: Optional[int] = None,
        sources: list[str] = None,
    ):
        # Scrap spider should be faster than historical spiders
        super().__init__(domain=domain, limit=limit, rate_limit=2.0)
        
        self.active_sources = sources or list(self.SOURCES.keys())
    
    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate URLs for scrap pricing pages.
        """
        for source_key in self.active_sources:
            if not self.should_continue():
                return
            
            source = self.SOURCES.get(source_key)
            if not source:
                continue
            
            logger.info(f"Fetching prices from {source['name']}")
            
            for path in source["price_paths"]:
                url = f"{source['base_url']}{path}"
                yield url
    
    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse pricing page and extract material prices.
        """
        # Save the raw HTML first
        doc_id = self.save_raw(
            content=response.content,
            url=url,
            document_type="html",
            metadata={
                "fetch_date": datetime.now().isoformat(),
            },
        )
        
        # Try to extract prices
        prices = self._extract_prices(response, url)
        
        return {
            "document_id": doc_id,
            "url": url,
            "prices_extracted": len(prices),
            "prices": prices,
        }
    
    def _extract_prices(self, response: httpx.Response, url: str) -> list[dict]:
        """
        Extract pricing data from HTML.
        
        This is a basic implementation - actual extraction will vary
        based on the source website structure.
        """
        prices = []
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.warning(f"Failed to parse HTML: {e}")
            return prices
        
        # Look for price tables
        tables = soup.find_all("table")
        
        for table in tables:
            rows = table.find_all("tr")
            
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    material = cells[0].get_text(strip=True)
                    price_text = cells[-1].get_text(strip=True)
                    
                    # Try to parse price
                    price = self._parse_price(price_text)
                    
                    if price and self._is_valid_material(material):
                        prices.append({
                            "material": material,
                            "price": price,
                            "currency": self._detect_currency(price_text),
                            "unit": self._detect_unit(price_text),
                            "date": datetime.now().isoformat(),
                            "source": url,
                        })
        
        # Also look for structured price displays (divs, spans)
        price_containers = soup.find_all(class_=re.compile(r"price|rate|value", re.I))
        
        for container in price_containers:
            text = container.get_text(strip=True)
            price = self._parse_price(text)
            
            if price:
                # Try to find associated material name
                parent = container.parent
                if parent:
                    material = parent.get_text(strip=True).replace(text, "").strip()
                    if self._is_valid_material(material):
                        prices.append({
                            "material": material,
                            "price": price,
                            "currency": self._detect_currency(text),
                            "unit": self._detect_unit(text),
                            "date": datetime.now().isoformat(),
                            "source": url,
                        })
        
        return prices
    
    def _parse_price(self, text: str) -> Optional[float]:
        """
        Extract numeric price from text.
        """
        # Remove currency symbols and commas
        cleaned = re.sub(r"[$€£¥,]", "", text)
        
        # Find decimal number
        match = re.search(r"(\d+\.?\d*)", cleaned)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def _detect_currency(self, text: str) -> str:
        """Detect currency from text."""
        if "$" in text or "USD" in text.upper():
            return "USD"
        elif "€" in text or "EUR" in text.upper():
            return "EUR"
        elif "£" in text or "GBP" in text.upper():
            return "GBP"
        return "USD"  # Default
    
    def _detect_unit(self, text: str) -> str:
        """Detect unit from text."""
        text_lower = text.lower()
        
        if "lb" in text_lower or "pound" in text_lower:
            return "lb"
        elif "kg" in text_lower or "kilo" in text_lower:
            return "kg"
        elif "ton" in text_lower or "mt" in text_lower:
            return "metric_ton"
        elif "cwt" in text_lower:  # Hundredweight
            return "cwt"
        
        return "per_unit"  # Default
    
    def _is_valid_material(self, material: str) -> bool:
        """Check if material name is valid."""
        if not material or len(material) < 2:
            return False
        
        # Skip headers and invalid entries
        invalid = ["material", "price", "date", "location", "update"]
        if material.lower() in invalid:
            return False
        
        # Skip if too long (probably not a material name)
        if len(material) > 100:
            return False
        
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    spider = ScrapExchangeSpider(domain="symbioflows", limit=5)
    results = spider.run()
    print(f"Results: {results}")
