"""
Symbio Data Engine - Government Data Spider
============================================
Fetch data from government environmental databases.

Sources:
- EPA TRI (Toxics Release Inventory) - USA
- E-PRTR (European Pollutant Release and Transfer Register) - EU
- MOIAT (Ministry of Industry and Advanced Technology) - UAE
"""

import csv
import io
import logging
from pathlib import Path
from typing import Generator, Optional

import httpx

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)


class GovSpider(BaseSpider):
    """
    Spider for government environmental databases.
    
    These sources provide bulk CSV/Excel downloads rather than web pages,
    so this spider handles structured data downloads differently.
    """
    
    name = "gov"
    source = "government"
    
    # Data source URLs and configurations
    SOURCES = {
        "epa_tri": {
            "name": "EPA Toxics Release Inventory",
            "base_url": "https://enviro.epa.gov/triexplorer/",
            "bulk_url": "https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present",
            "format": "csv",
            "years": range(1987, 2025),
        },
        # "eprtr": {
        #     "name": "European Pollutant Release and Transfer Register",
        #     "base_url": "https://prtr.eea.europa.eu/",
        #     "api_url": "https://prtr.eea.europa.eu/api/",
        #     "format": "csv",
        #     "years": range(2007, 2025),
        # },
    }
    
    def __init__(
        self,
        domain: str = "symbiotrust",
        limit: Optional[int] = None,
        sources: list[str] = None,
    ):
        super().__init__(domain=domain, limit=limit)
        
        # Which government sources to crawl
        self.active_sources = sources or list(self.SOURCES.keys())
    
    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate URLs for government data downloads.
        
        Note: Government sources often require different handling.
        Some provide bulk downloads, others have APIs.
        """
        for source_key in self.active_sources:
            if not self.should_continue():
                return
            
            source = self.SOURCES.get(source_key)
            if not source:
                continue
            
            logger.info(f"Processing source: {source['name']}")
            
            if source_key == "epa_tri":
                yield from self._get_epa_tri_urls(source)
            elif source_key == "eprtr":
                yield from self._get_eprtr_urls(source)
    
    def _get_epa_tri_urls(self, source: dict) -> Generator[str, None, None]:
        """
        Generate URLs for EPA TRI bulk data files.
        
        The TRI provides annual CSV files with facility release data.
        """
        # Updated 2026: Browser-verified EPA Envirofacts API endpoint
        # Pattern: https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{YEAR}_US/csv
        
        for year in source["years"]:
            if not self.should_continue():
                return
            
            url = f"https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{year}_US/csv"
            yield url
    
    def _get_eprtr_urls(self, source: dict) -> Generator[str, None, None]:
        """
        Generate URLs for E-PRTR data.
        
        E-PRTR provides an API and bulk download options.
        """
        # E-PRTR API endpoints
        api_base = source.get("api_url", source["base_url"])
        
        for year in source["years"]:
            if not self.should_continue():
                return
            
            # Facility releases by year
            url = f"{api_base}releases?year={year}&format=csv"
            yield url
    
    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse government data response.
        
        Handles CSV and JSON responses from government APIs.
        """
        content_type = response.headers.get("content-type", "")
        
        # Check for CSV: content-type contains csv, OR URL ends with .csv OR /csv
        if "csv" in content_type or url.endswith(".csv") or url.endswith("/csv"):
            return self._parse_csv(response, url)
        elif "json" in content_type:
            return self._parse_json(response, url)
        else:
            # Save as raw file
            return self._save_raw_file(response, url)
    
    def _parse_csv(self, response: httpx.Response, url: str) -> Optional[dict]:
        """Parse CSV response and save."""
        content = response.content
        
        # Quick validation - check it's actually CSV
        try:
            text = response.text
            reader = csv.reader(io.StringIO(text))
            headers = next(reader, None)
            row_count = sum(1 for _ in reader)
        except Exception as e:
            logger.warning(f"Invalid CSV from {url}: {e}")
            return None
        
        # Determine year from URL if possible
        year = self._extract_year_from_url(url)
        
        # Save raw CSV
        doc_id = self.save_raw(
            content=content,
            url=url,
            document_type="csv",
            metadata={
                "row_count": row_count,
                "column_count": len(headers) if headers else 0,
                "columns": headers,
                "year": year,
            },
        )
        
        return {
            "document_id": doc_id,
            "row_count": row_count,
            "year": year,
        }
    
    def _parse_json(self, response: httpx.Response, url: str) -> Optional[dict]:
        """Parse JSON API response and save."""
        content = response.content
        
        try:
            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
        except Exception as e:
            logger.warning(f"Invalid JSON from {url}: {e}")
            return None
        
        year = self._extract_year_from_url(url)
        
        doc_id = self.save_raw(
            content=content,
            url=url,
            document_type="json",
            metadata={
                "record_count": record_count,
                "year": year,
            },
        )
        
        return {
            "document_id": doc_id,
            "record_count": record_count,
            "year": year,
        }
    
    def _save_raw_file(self, response: httpx.Response, url: str) -> Optional[dict]:
        """Save unrecognized file format."""
        content = response.content
        
        # Guess extension from URL
        ext = Path(url).suffix.lstrip(".") or "bin"
        
        doc_id = self.save_raw(
            content=content,
            url=url,
            document_type=ext,
            metadata={
                "year": self._extract_year_from_url(url),
            },
        )
        
        return {"document_id": doc_id}
    
    def _extract_year_from_url(self, url: str) -> Optional[int]:
        """Try to extract year from URL."""
        import re
        
        # Look for 4-digit years (1987-2029)
        matches = re.findall(r"(19[89]\d|20[0-2]\d)", url)
        if matches:
            # Return the last match (often the data year)
            return int(matches[-1])
        
        return None


# Convenience function for direct URL fetching
def fetch_epa_tri_file(year: int, output_dir: Path = None) -> Optional[Path]:
    """
    Download a specific EPA TRI file.
    
    Args:
        year: Year to download
        output_dir: Directory to save file
    
    Returns:
        Path to downloaded file
    """
    spider = GovSpider(limit=1)
    url = f"https://www.epa.gov/system/files/other-files/{year}/tri_{year}_us.csv"
    
    response = spider.fetch(url)
    if response:
        output_dir = output_dir or config.RAW_DIR / "government"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / f"tri_{year}.csv"
        output_path.write_bytes(response.content)
        
        return output_path
    
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    spider = GovSpider(domain="symbiotrust", limit=2)
    results = spider.run()
    print(f"Results: {results}")
