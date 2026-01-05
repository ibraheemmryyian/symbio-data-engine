"""
Symbio Data Engine - Corporate Sustainability Report Spider
===========================================================
Discover and download corporate sustainability/ESG reports.

Primary targets:
- Borouge (borouge.com) - Petrochemical company
- ADNOC (adnoc.ae) - Oil & gas major
- Other major industrial companies
"""

import logging
import re
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)


class CSRSpider(BaseSpider):
    """
    Spider for corporate sustainability reports.
    
    CSR reports are typically PDFs published annually.
    This spider discovers report pages and downloads PDFs.
    """
    
    name = "csr"
    source = "csr_reports"
    
    # Target companies and their sustainability pages
    TARGETS = {
        "borouge": {
            "name": "Borouge",
            "base_url": "https://www.borouge.com",
            "sustainability_paths": [
                "/sustainability",
                "/en/sustainability",
                "/sustainability/reports",
                "/investors/reports",
            ],
            "report_patterns": [
                r"sustainability.*report",
                r"esg.*report",
                r"annual.*report",
                r"environmental.*report",
            ],
        },
        "adnoc": {
            "name": "ADNOC",
            "base_url": "https://www.adnoc.ae",
            "sustainability_paths": [
                "/en/sustainability",
                "/sustainability",
                "/en/about/sustainability",
            ],
            "report_patterns": [
                r"sustainability.*report",
                r"esg.*report",
                r"annual.*review",
            ],
        },
        "sabic": {
            "name": "SABIC",
            "base_url": "https://www.sabic.com",
            "sustainability_paths": [
                "/en/sustainability",
                "/sustainability/reports",
            ],
            "report_patterns": [
                r"sustainability.*report",
                r"integrated.*report",
            ],
        },
    }
    
    def __init__(
        self,
        domain: str = "symbiotrust",
        limit: Optional[int] = None,
        companies: list[str] = None,
    ):
        super().__init__(domain=domain, limit=limit)
        
        # Which companies to target
        self.active_companies = companies or list(self.TARGETS.keys())
        
        # Track discovered PDFs to avoid duplicates
        self.discovered_pdfs = set()
    
    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate starting URLs for CSR discovery.
        """
        for company_key in self.active_companies:
            if not self.should_continue():
                return
            
            company = self.TARGETS.get(company_key)
            if not company:
                continue
            
            logger.info(f"Discovering CSR reports for {company['name']}")
            
            for path in company["sustainability_paths"]:
                url = urljoin(company["base_url"], path)
                yield url
    
    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse a sustainability page to find PDF links.
        """
        content_type = response.headers.get("content-type", "")
        
        # If it's a PDF, save it directly
        if "pdf" in content_type or url.lower().endswith(".pdf"):
            return self._save_pdf(response, url)
        
        # Otherwise, parse HTML to find PDF links
        return self._parse_html_for_pdfs(response, url)
    
    def _parse_html_for_pdfs(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse HTML page to discover PDF report links.
        """
        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.warning(f"Failed to parse HTML from {url}: {e}")
            return None
        
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        # Find the company config for pattern matching
        company_config = None
        for config in self.TARGETS.values():
            if config["base_url"] in url:
                company_config = config
                break
        
        patterns = company_config["report_patterns"] if company_config else [r"report"]
        
        pdfs_found = []
        
        # Find all links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            link_text = link.get_text(strip=True).lower()
            
            # Check if it's a PDF
            if not (href.lower().endswith(".pdf") or "pdf" in href.lower()):
                continue
            
            # Build absolute URL
            if href.startswith("/"):
                pdf_url = urljoin(base_url, href)
            elif href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(url, href)
            
            # Check if it matches our patterns
            is_report = any(
                re.search(pattern, link_text, re.IGNORECASE) or
                re.search(pattern, href, re.IGNORECASE)
                for pattern in patterns
            )
            
            if is_report and pdf_url not in self.discovered_pdfs:
                self.discovered_pdfs.add(pdf_url)
                pdfs_found.append({
                    "url": pdf_url,
                    "title": link_text or href.split("/")[-1],
                })
        
        logger.info(f"Found {len(pdfs_found)} PDF reports on {url}")
        
        # Download discovered PDFs
        for pdf_info in pdfs_found:
            if not self.should_continue():
                break
            
            pdf_response = self.fetch(pdf_info["url"])
            if pdf_response:
                self._save_pdf(
                    pdf_response,
                    pdf_info["url"],
                    title=pdf_info["title"],
                )
        
        return {
            "page_url": url,
            "pdfs_found": len(pdfs_found),
        }
    
    def _save_pdf(
        self,
        response: httpx.Response,
        url: str,
        title: str = None,
    ) -> Optional[dict]:
        """
        Save a PDF report.
        """
        content = response.content
        
        # Validate it's actually a PDF
        if not content.startswith(b"%PDF"):
            logger.warning(f"Not a valid PDF: {url}")
            return None
        
        # Try to extract year from URL or filename
        year = self._extract_year(url)
        
        # Determine company from URL
        company = "unknown"
        for key, config in self.TARGETS.items():
            if config["base_url"] in url:
                company = key
                break
        
        doc_id = self.save_raw(
            content=content,
            url=url,
            document_type="pdf",
            metadata={
                "company": company,
                "title": title,
                "year": year,
                "size_bytes": len(content),
            },
        )
        
        return {
            "document_id": doc_id,
            "company": company,
            "year": year,
        }
    
    def _extract_year(self, url: str) -> Optional[int]:
        """Extract year from URL."""
        matches = re.findall(r"20[0-2]\d", url)
        if matches:
            return int(matches[-1])
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    spider = CSRSpider(domain="symbiotrust", limit=5)
    results = spider.run()
    print(f"Results: {results}")
