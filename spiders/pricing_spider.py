"""
Symbio Data Engine - Pricing Spider
====================================
Multi-source pricing aggregator for industrial waste materials.

Scrapes multiple free sources, averages values for stability.
Stores raw prices then aggregates to material_valuations.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from typing import Generator, Optional

import httpx
from bs4 import BeautifulSoup

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)


@dataclass
class RawPrice:
    """Raw price extracted from a source."""
    material_name: str
    price_value: float
    price_unit: str  # 'lb', 'kg', 'ton', 'mt'
    currency: str = "USD"
    source: str = ""
    source_url: str = ""
    region: str = "us"
    price_date: Optional[date] = None
    

class PricingSpider(BaseSpider):
    """
    Multi-source pricing aggregator.
    
    Scrapes ScrapMonster, Rockaway Recycling, and other free sources.
    Averages prices across sources for stability.
    """
    
    name = "pricing"
    source = "pricing"
    
    # =========================================
    # SOURCE CONFIGURATIONS
    # =========================================
    SOURCES = {
        "scrapmonster": {
            "name": "ScrapMonster",
            "base_url": "https://www.scrapmonster.com",
            "materials": {
                # Copper
                "copper_bare_bright": "/scrap-metal-prices/copper-scrap/1-copper-bare-bright/17",
                "copper_wire_1": "/scrap-metal-prices/copper-scrap/1-copper-wire-and-tubing/18",
                "copper_wire_2": "/scrap-metal-prices/copper-scrap/2-copper-wire-and-tubing/19",
                "copper_light": "/scrap-metal-prices/copper-scrap/3-copper-light-copper/20",
                # Aluminum
                "aluminum_6063": "/scrap-metal-prices/aluminum-scrap/6063-extrusions/3",
                "aluminum_6061": "/scrap-metal-prices/aluminum-scrap/6061-extrusions/10",
                "aluminum_ubc": "/scrap-metal-prices/aluminum-scrap/ubc/11",
                "aluminum_wheels": "/scrap-metal-prices/aluminum-scrap/356-aluminum-wheels-clean/13",
                # Brass
                "brass_yellow": "/scrap-metal-prices/brassbronze/yellow-brass/26",
                "brass_red": "/scrap-metal-prices/brassbronze/red-brass/27",
                # Steel
                "steel_hms_1": "/scrap-metal-prices/steel/1-hms/44",
                "steel_busheling": "/scrap-metal-prices/steel/1-busheling/215",
                "steel_shredded": "/scrap-metal-prices/steel/shredded-auto-scrap/345",
                # Lead
                "lead_batteries": "/scrap-metal-prices/lead-scrap/scrap-auto-batteries/41",
                "lead_solid": "/scrap-metal-prices/lead-scrap/lead-solid-lead/114",
            },
            "parser": "_parse_scrapmonster",
        },
        "rockaway": {
            "name": "Rockaway Recycling",
            "base_url": "https://rockawayrecycling.com",
            "list_url": "/scrap-metal-prices/",
            "parser": "_parse_rockaway",
        },
    }
    
    # Unit conversion factors to metric tons
    UNIT_TO_TONS = {
        "lb": 0.000453592,
        "lbs": 0.000453592,
        "pound": 0.000453592,
        "kg": 0.001,
        "kilo": 0.001,
        "ton": 0.907185,  # US short ton
        "mt": 1.0,
        "metric_ton": 1.0,
        "tonne": 1.0,
        "cwt": 0.0453592,  # hundredweight
    }
    
    def __init__(
        self,
        domain: str = "symbioflows",
        limit: Optional[int] = None,
        sources: list[str] = None,
    ):
        super().__init__(domain=domain, limit=limit, rate_limit=0.5)  # Be polite
        self.active_sources = sources or list(self.SOURCES.keys())
        self.raw_prices: list[RawPrice] = []
        
        # Override session with longer timeout for slow sites
        self.session = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            },
        )
    
    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for all material price pages."""
        for source_key in self.active_sources:
            if not self.should_continue():
                return
            
            source = self.SOURCES.get(source_key)
            if not source:
                continue
            
            logger.info(f"Fetching prices from {source['name']}")
            
            if source_key == "scrapmonster":
                # Individual material pages
                for material_key, path in source["materials"].items():
                    if not self.should_continue():
                        return
                    yield (source_key, material_key, f"{source['base_url']}{path}")
            
            elif source_key == "rockaway":
                # Single listing page
                yield (source_key, "all", f"{source['base_url']}{source['list_url']}")
    
    def parse(self, response: httpx.Response, url_info: tuple) -> Optional[dict]:
        """Parse pricing page and extract material prices."""
        source_key, material_key, url = url_info
        
        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.warning(f"Failed to parse HTML: {e}")
            return None
        
        # Route to appropriate parser
        parser_name = self.SOURCES[source_key].get("parser", "_parse_generic")
        parser = getattr(self, parser_name, self._parse_generic)
        
        prices = parser(soup, url, source_key, material_key)
        
        if prices:
            self.raw_prices.extend(prices)
            logger.info(f"Extracted {len(prices)} prices from {url}")
        
        return {"prices_extracted": len(prices)}
    
    # =========================================
    # PARSERS
    # =========================================
    
    def _parse_scrapmonster(
        self, soup: BeautifulSoup, url: str, source_key: str, material_key: str
    ) -> list[RawPrice]:
        """Parse ScrapMonster individual material page."""
        prices = []
        
        # Find price container: <div class="scrapitemprice">
        price_container = soup.select_one('.scrapitemprice')
        if not price_container:
            logger.warning(f"No price container found on {url}")
            return prices
        
        # Extract price value (first text node before $)
        text = price_container.get_text(strip=True)
        
        # Parse: "4.89$US/Lb +0.07(+1.39)"
        price_match = re.search(r'([\d.]+)\s*\$', text)
        if not price_match:
            # Try alternative: just a number
            price_match = re.search(r'^([\d.]+)', text)
        
        if not price_match:
            logger.warning(f"Could not extract price from: {text}")
            return prices
        
        price_value = float(price_match.group(1))
        
        # Detect unit
        unit = "lb"  # Default for ScrapMonster US prices
        if "/kg" in text.lower():
            unit = "kg"
        elif "/mt" in text.lower() or "/tonne" in text.lower():
            unit = "mt"
        elif "/ton" in text.lower():
            unit = "ton"
        
        # Extract date if available
        price_date = None
        date_elem = soup.select_one('.ov-price-date')
        if date_elem:
            date_text = date_elem.get_text(strip=True)
            try:
                price_date = datetime.strptime(date_text, "%B %d, %Y").date()
            except ValueError:
                pass
        
        # Clean material name from key
        material_name = self._clean_material_name(material_key)
        
        prices.append(RawPrice(
            material_name=material_name,
            price_value=price_value,
            price_unit=unit,
            currency="USD",
            source="scrapmonster",
            source_url=url,
            region="us",
            price_date=price_date,
        ))
        
        return prices
    
    def _parse_rockaway(
        self, soup: BeautifulSoup, url: str, source_key: str, material_key: str
    ) -> list[RawPrice]:
        """Parse Rockaway Recycling prices page."""
        prices = []
        
        # Look for price tables
        tables = soup.find_all("table")
        
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    material = cells[0].get_text(strip=True)
                    price_text = cells[-1].get_text(strip=True)
                    
                    # Parse price
                    price_match = re.search(r'\$?([\d.]+)', price_text)
                    if price_match and self._is_valid_material(material):
                        prices.append(RawPrice(
                            material_name=material.lower().strip(),
                            price_value=float(price_match.group(1)),
                            price_unit="lb",  # Rockaway uses $/lb
                            currency="USD",
                            source="rockaway",
                            source_url=url,
                            region="us_east",
                        ))
        
        return prices
    
    def _parse_generic(
        self, soup: BeautifulSoup, url: str, source_key: str, material_key: str
    ) -> list[RawPrice]:
        """Generic parser for unknown sources."""
        return []
    
    # =========================================
    # UTILITIES
    # =========================================
    
    def _clean_material_name(self, key: str) -> str:
        """Convert material key to readable name."""
        # copper_bare_bright -> Copper Bare Bright
        return key.replace("_", " ").title()
    
    def _is_valid_material(self, material: str) -> bool:
        """Check if material name is valid."""
        if not material or len(material) < 2:
            return False
        
        # Skip headers and invalid entries
        invalid = ["material", "price", "date", "location", "update", "metal", "type"]
        if material.lower().strip() in invalid:
            return False
        
        # Skip if too long
        if len(material) > 100:
            return False
        
        return True
    
    def normalize_to_tons(self, price: float, unit: str) -> float:
        """Convert price per unit to price per metric ton."""
        factor = self.UNIT_TO_TONS.get(unit.lower(), 1.0)
        if factor == 0:
            return 0
        return price / factor
    
    def get_aggregated_prices(self) -> dict[str, dict]:
        """
        Aggregate raw prices by material.
        
        Returns dict of material_name -> {
            price_per_ton_usd: float,
            price_per_lb_usd: float,
            source_count: int,
            confidence: float,
            sources: list,
        }
        """
        from collections import defaultdict
        
        grouped = defaultdict(list)
        
        for raw in self.raw_prices:
            # Normalize to $/ton
            price_per_ton = self.normalize_to_tons(raw.price_value, raw.price_unit)
            grouped[raw.material_name.lower()].append({
                "price_per_ton": price_per_ton,
                "price_per_lb": raw.price_value if raw.price_unit == "lb" else raw.price_value * 0.000453592,
                "source": raw.source,
            })
        
        result = {}
        for material, prices in grouped.items():
            avg_ton = sum(p["price_per_ton"] for p in prices) / len(prices)
            avg_lb = sum(p["price_per_lb"] for p in prices) / len(prices)
            sources = list(set(p["source"] for p in prices))
            
            # Confidence based on source count
            confidence = min(1.0, len(prices) / 3.0)
            
            result[material] = {
                "price_per_ton_usd": round(avg_ton, 2),
                "price_per_lb_usd": round(avg_lb, 4),
                "source_count": len(prices),
                "confidence": round(confidence, 2),
                "sources": sources,
            }
        
        return result
    
    def run(self) -> dict:
        """Execute spider and return aggregated results."""
        logger.info(f"Starting {self.name} spider")
        
        # Fetch all prices
        for url_info in self.get_urls():
            if not self.should_continue():
                break
            
            source_key, material_key, url = url_info
            response = self.fetch(url)
            
            if response:
                try:
                    self.parse(response, url_info)
                except Exception as e:
                    logger.error(f"Parse error for {url}: {e}")
                    self.errors += 1
        
        self.session.close()
        
        # Aggregate prices
        aggregated = self.get_aggregated_prices()
        
        logger.info(f"Collected {len(self.raw_prices)} raw prices, aggregated to {len(aggregated)} materials")
        
        return {
            "spider": self.name,
            "raw_prices": len(self.raw_prices),
            "aggregated_materials": len(aggregated),
            "errors": self.errors,
            "prices": aggregated,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    spider = PricingSpider(sources=["scrapmonster"], limit=5)
    results = spider.run()
    
    print("\n" + "="*50)
    print("PRICING SPIDER RESULTS")
    print("="*50)
    print(f"Raw prices: {results['raw_prices']}")
    print(f"Aggregated materials: {results['aggregated_materials']}")
    print()
    
    for material, data in results["prices"].items():
        print(f"  {material}: ${data['price_per_lb_usd']}/lb (${data['price_per_ton_usd']}/ton)")
        print(f"    Sources: {data['sources']}, Confidence: {data['confidence']}")
