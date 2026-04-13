
import logging
import json
from typing import Generator, Optional
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)

class MENAGovSpider(BaseSpider):
    """
    Spider for MENA Government Open Data Portals.
    Targets Saudi Arabia, UAE, Qatar, and Oman.
    """
    
    name = "mena_gov"
    source = "government"
    
    PORTALS = {
        "saudi": {
            "name": "Saudi Arabia",
            "api_url": "https://catalog.data.gov.sa/data/api/3/action/package_search",
            "search_query": "waste",
            "type": "ckan"
        },
        "uae": {
            "name": "United Arab Emirates",
            "api_url": "https://bayanat.ae/api/v1/datasets",
            "search_query": "waste",
            "type": "custom"
        },
        "qatar": {
            "name": "Qatar",
            "api_url": "https://www.data.gov.qa/api/v2/catalog/datasets",
            "search_query": "waste",
            "type": "opendatasoft"
        },
        "oman": {
            "name": "Oman",
            "api_url": "https://data.gov.om/api/v1/datasets",
            "search_query": "waste",
            "type": "custom"
        }
    }
    
    def __init__(
        self,
        domain: str = "symbioflows",
        limit: Optional[int] = None,
        countries: list[str] = None,
    ):
        super().__init__(domain=domain, limit=limit)
        self.active_portals = countries or list(self.PORTALS.keys())

    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate API query URLs for each portal.
        """
        for country in self.active_portals:
            portal = self.PORTALS.get(country)
            if not portal:
                continue
            
            logger.info(f"Querying {portal['name']} Open Data Portal...")
            
            if portal["type"] == "ckan":
                yield f"{portal['api_url']}?q={portal['search_query']}&rows=100"
            elif portal["type"] == "opendatasoft":
                yield f"{portal['api_url']}?search={portal['search_query']}&limit=100"
            else:
                # Custom/Other - return base API for manual handling in parse
                yield portal["api_url"]

    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse API response and discover datasets.
        """
        # Determine which portal this is
        portal_key = None
        for key, portal in self.PORTALS.items():
            if portal["api_url"] in url:
                portal_key = key
                break
        
        if not portal_key:
            return None
            
        portal = self.PORTALS[portal_key]
        data = response.json()
        
        datasets_found = 0
        
        if portal["type"] == "ckan":
            results = data.get("result", {}).get("results", [])
            for ds in results:
                self._handle_ckan_dataset(ds, portal_key)
                datasets_found += 1
                
        elif portal["type"] == "opendatasoft":
            results = data.get("datasets", [])
            for ds in results:
                self._handle_ods_dataset(ds, portal_key)
                datasets_found += 1
        
        else:
            # Handle list-based or other custom structures
            if isinstance(data, list):
                for ds in data:
                    self._handle_custom_dataset(ds, portal_key)
                    datasets_found += 1
            elif "datasets" in data:
                 for ds in data["datasets"]:
                    self._handle_custom_dataset(ds, portal_key)
                    datasets_found += 1

        logger.info(f"Found {datasets_found} datasets for {portal['name']}")
        return {"country": portal_key, "datasets_found": datasets_found}

    def _handle_ckan_dataset(self, ds: dict, country: str):
        """Process a CKAN dataset entry."""
        title = ds.get("title", "Untitled")
        resources = ds.get("resources", [])
        
        for res in resources:
            url = res.get("url")
            fmt = res.get("format", "").lower()
            if url and fmt in ["csv", "xlsx", "xls", "json"]:
                # Download and save the resource
                self._download_resource(url, fmt, country, title)

    def _handle_ods_dataset(self, ds: dict, country: str):
        """Process an OpenDataSoft dataset entry."""
        dataset_id = ds.get("dataset", {}).get("dataset_id")
        title = ds.get("dataset", {}).get("metas", {}).get("default", {}).get("title", "Untitled")
        
        if dataset_id:
            # ODS usually allows direct CSV export via URL pattern
            export_url = f"https://www.data.gov.qa/api/v2/catalog/datasets/{dataset_id}/exports/csv"
            self._download_resource(export_url, "csv", country, title)

    def _handle_custom_dataset(self, ds: dict, country: str):
        """Handle custom/generic dataset structure."""
        # This is a guestimate - would be refined after seeing output
        url = ds.get("url") or ds.get("download_url") or ds.get("link")
        title = ds.get("title") or ds.get("name") or "Untitled"
        fmt = ds.get("format") or ds.get("file_type") or "csv"
        
        if url:
            self._download_resource(url, str(fmt).lower(), country, title)

    def _download_resource(self, url: str, fmt: str, country: str, title: str):
        """Download the actual data file."""
        if not self.should_continue():
            return
            
        logger.info(f"Downloading dataset: {title} ({fmt})")
        resp = self.fetch(url)
        if resp:
            self.save_raw(
                content=resp.content,
                url=url,
                document_type=fmt,
                metadata={
                    "country": country,
                    "title": title,
                    "year": datetime.now().year,
                    "extraction_method": "government_api"
                }
            )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spider = MENAGovSpider(limit=20)
    spider.run()
