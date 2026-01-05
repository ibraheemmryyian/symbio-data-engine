"""
Symbio Data Engine - Spiders Module
====================================
Web crawlers for data ingestion from various sources.
"""

from .base_spider import BaseSpider
from .wayback_spider import WaybackSpider
from .gov_spider import GovSpider
from .csr_spider import CSRSpider
from .scrap_exchange_spider import ScrapExchangeSpider


# Spider registry for CLI
SPIDERS = {
    "wayback": WaybackSpider,
    "gov": GovSpider,
    "csr": CSRSpider,
    "scrap": ScrapExchangeSpider,
}


def run_spider(
    domain: str,
    source: str = "all",
    limit: int = None,
) -> dict:
    """
    Run spiders for a specific domain.
    
    Args:
        domain: Target domain (symbioflows, symbiotrust, research, all)
        source: Spider source to use (wayback, gov, csr, scrap, all)
        limit: Maximum documents to ingest
    
    Returns:
        Dict with ingestion results
    """
    results = {
        "domain": domain,
        "source": source,
        "documents": 0,
        "errors": 0,
    }
    
    spiders_to_run = []
    
    if source == "all":
        spiders_to_run = list(SPIDERS.values())
    elif source in SPIDERS:
        spiders_to_run = [SPIDERS[source]]
    else:
        raise ValueError(f"Unknown spider source: {source}")
    
    for SpiderClass in spiders_to_run:
        spider = SpiderClass(domain=domain, limit=limit)
        spider_result = spider.run()
        
        results["documents"] += spider_result.get("documents", 0)
        results["errors"] += spider_result.get("errors", 0)
    
    return results


__all__ = [
    "BaseSpider",
    "WaybackSpider",
    "GovSpider",
    "CSRSpider",
    "ScrapExchangeSpider",
    "run_spider",
    "SPIDERS",
]
