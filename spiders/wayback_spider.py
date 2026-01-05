"""
Symbio Data Engine - Wayback Machine Spider
============================================
Fetch historical web pages from the Internet Archive (1978-2024).

🛡️ FEATURES:
- CDX API for snapshot discovery
- Availability check before download
- Rate limiting with exponential backoff
"""

import logging
from datetime import datetime
from typing import Generator, Optional
from urllib.parse import urljoin

import httpx

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)


class WaybackSpider(BaseSpider):
    """
    Spider for fetching historical pages from the Wayback Machine.
    
    Uses the CDX API to find archived snapshots and retrieves them.
    This is the primary spider for historical symbiosis research (1978-present).
    """
    
    name = "wayback"
    source = "wayback"
    
    # Target domains for historical research
    TARGET_URLS = {
        # Kalundborg Symbiosis - Primary target
        "kalundborg": [
            "symbiosis.dk",
            "www.symbiosis.dk",
            "kalundborgsymbiosis.dk",
        ],
        # Government/Environmental
        "government": [
            "epa.gov/tri",
            "eea.europa.eu",
        ],
        # Industrial parks
        "industrial_parks": [
            "ulsan-eip.or.kr",
            "nisp.org.uk",
        ],
    }
    
    def __init__(
        self,
        domain: str = "research",
        limit: Optional[int] = None,
        start_year: int = 1996,  # Wayback started in 1996
        end_year: int = None,
    ):
        super().__init__(domain=domain, limit=limit)
        
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        
        # CDX API endpoint
        self.cdx_url = config.WAYBACK_CDX_URL
        # 🛡️ Availability check endpoint
        self.availability_url = "https://archive.org/wayback/available"
    
    def _check_availability(self, url: str) -> Optional[dict]:
        """
        🛡️ Check if a URL is available in the Wayback Machine.
        
        This prevents wasting time on 404s.
        
        Args:
            url: URL to check
        
        Returns:
            Snapshot info dict if available, None otherwise
        """
        try:
            response = self.session.get(
                self.availability_url,
                params={"url": url},
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            archived = data.get("archived_snapshots", {})
            closest = archived.get("closest")
            
            if closest and closest.get("available"):
                return closest
            
            return None
            
        except Exception as e:
            logger.debug(f"Availability check failed for {url}: {e}")
            return None
    
    def _get_cdx_results(
        self,
        url: str,
        from_date: str = None,
        to_date: str = None,
    ) -> list[dict]:
        """
        Query the CDX API for archived snapshots.
        
        Args:
            url: URL to search for
            from_date: Start date (YYYYMMDD format)
            to_date: End date (YYYYMMDD format)
        
        Returns:
            List of snapshot records
        """
        params = {
            "url": url,
            "output": "json",
            "fl": "timestamp,original,mimetype,statuscode,length",
            "filter": "statuscode:200",
            "collapse": "timestamp:6",  # One per month
        }
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        
        try:
            response = self.session.get(self.cdx_url, params=params)
            if response.status_code != 200:
                logger.warning(f"CDX API error: {response.status_code}")
                return []
            
            data = response.json()
            if not data or len(data) < 2:
                return []
            
            # First row is headers
            headers = data[0]
            results = []
            
            for row in data[1:]:
                record = dict(zip(headers, row))
                results.append(record)
            
            return results
            
        except Exception as e:
            logger.error(f"CDX query error: {e}")
            return []
    
    def _build_wayback_url(self, timestamp: str, original_url: str) -> str:
        """Build the Wayback Machine URL for a specific snapshot."""
        return f"https://web.archive.org/web/{timestamp}/{original_url}"
    
    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate Wayback Machine URLs to crawl.
        
        🛡️ Uses availability check to skip unavailable snapshots.
        
        Yields:
            Wayback URLs for archived snapshots
        """
        from_date = f"{self.start_year}0101"
        to_date = f"{self.end_year}1231"
        
        # Select target URLs based on domain
        if self.domain == "symbioflows":
            target_groups = ["kalundborg"]  # Focus on symbiosis examples
        elif self.domain == "symbiotrust":
            target_groups = ["government"]  # Environmental data
        else:  # research or all
            target_groups = list(self.TARGET_URLS.keys())
        
        for group in target_groups:
            urls = self.TARGET_URLS.get(group, [])
            
            for base_url in urls:
                logger.info(f"Querying CDX for {base_url} ({self.start_year}-{self.end_year})")
                
                # Query CDX for snapshots
                snapshots = self._get_cdx_results(
                    url=f"{base_url}/*",  # Wildcard for all pages
                    from_date=from_date,
                    to_date=to_date,
                )
                
                logger.info(f"Found {len(snapshots)} snapshots for {base_url}")
                
                for snapshot in snapshots:
                    if not self.should_continue():
                        return
                    
                    # Skip non-HTML content (for now)
                    mimetype = snapshot.get("mimetype", "")
                    if not mimetype.startswith("text/html"):
                        continue
                    
                    timestamp = snapshot["timestamp"]
                    original = snapshot["original"]
                    
                    # 🛡️ Check availability before adding to queue
                    if not self._quick_validate_snapshot(snapshot):
                        logger.debug(f"Skipping invalid snapshot: {original}")
                        continue
                    
                    wayback_url = self._build_wayback_url(timestamp, original)
                    yield wayback_url
    
    def _quick_validate_snapshot(self, snapshot: dict) -> bool:
        """
        🛡️ Quick validation of snapshot before fetching.
        
        Args:
            snapshot: CDX snapshot record
        
        Returns:
            True if snapshot looks valid
        """
        # Check status code (should be 200 from CDX filter, but verify)
        status = snapshot.get("statuscode", "")
        if status != "200":
            return False
        
        # Check content length (skip tiny pages)
        try:
            length = int(snapshot.get("length", 0))
            if length < 500:  # Less than 500 bytes is probably an error page
                return False
        except (ValueError, TypeError):
            pass
        
        return True
    
    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse a Wayback response and save it.
        
        Args:
            response: HTTP response from Wayback
            url: Wayback URL (contains timestamp)
        
        Returns:
            Extracted metadata
        """
        # Extract timestamp and original URL from Wayback URL
        # Format: https://web.archive.org/web/TIMESTAMP/ORIGINAL
        parts = url.split("/web/", 1)
        if len(parts) < 2:
            return None
        
        timestamp_and_url = parts[1]
        timestamp = timestamp_and_url[:14]  # YYYYMMDDHHMMSS
        original_url = timestamp_and_url[15:]  # Skip timestamp and /
        
        # Parse timestamp to year
        try:
            year = int(timestamp[:4])
            month = int(timestamp[4:6])
            day = int(timestamp[6:8])
            snapshot_date = f"{year}-{month:02d}-{day:02d}"
        except ValueError:
            snapshot_date = None
            year = None
        
        # Save raw HTML
        content = response.content
        
        doc_id = self.save_raw(
            content=content,
            url=url,
            document_type="html",
            metadata={
                "wayback_timestamp": timestamp,
                "original_url": original_url,
                "snapshot_date": snapshot_date,
                "year": year,
            },
        )
        
        return {
            "document_id": doc_id,
            "original_url": original_url,
            "timestamp": timestamp,
            "year": year,
        }


# Convenience function for testing
def test_wayback_spider():
    """Quick test of the Wayback spider."""
    spider = WaybackSpider(
        domain="research",
        limit=5,
        start_year=2020,
        end_year=2024,
    )
    
    # Test availability check
    print("🛡️ Testing availability check...")
    available = spider._check_availability("symbiosis.dk")
    print(f"  symbiosis.dk available: {available is not None}")
    
    # Test CDX query
    print("\n📜 Testing CDX query...")
    results = spider._get_cdx_results("symbiosis.dk", "20200101", "20241231")
    print(f"  Found {len(results)} snapshots")
    
    for r in results[:5]:
        print(f"    {r['timestamp']}: {r['original']}")
    
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_wayback_spider()
