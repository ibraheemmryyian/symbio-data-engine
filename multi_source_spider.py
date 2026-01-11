"""
MULTI-SOURCE CSR MEGA SPIDER
=============================
Hits multiple report databases to maximize PDF collection.

Sources:
1. ResponsibilityReports.com - Largest free CSR report database
2. Corporate Register - 100,000+ reports indexed
3. Sustainability Reports - Curated collection
4. Direct company investor relations pages
5. Google Scholar for academic/industry studies

Target: 20,000+ PDFs
"""
import asyncio
import logging
import random
import re
import json
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Set
from urllib.parse import urljoin, quote_plus

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

# Industries to search for
INDUSTRIES = [
    "mining", "oil gas", "petroleum", "chemical", "steel", "metals", "cement",
    "construction", "automotive", "manufacturing", "food beverage", "agriculture",
    "pharmaceutical", "healthcare", "energy", "utilities", "power", "renewable",
    "transport", "logistics", "shipping", "aviation", "retail", "consumer goods",
    "technology", "electronics", "telecommunications", "banking", "financial",
    "real estate", "hospitality", "textiles", "apparel", "pulp paper", "packaging",
    "waste management", "recycling", "water", "aluminum", "copper", "gold",
]

# Regions to search
REGIONS = [
    "middle east", "UAE", "Saudi Arabia", "Qatar", "Kuwait", "Bahrain", "Oman",
    "Egypt", "Morocco", "South Africa", "Nigeria", "Kenya",
    "China", "Japan", "Korea", "India", "Australia", "Singapore", "Malaysia", "Thailand",
    "Germany", "UK", "France", "Netherlands", "Switzerland", "Sweden", "Norway",
    "USA", "Canada", "Mexico", "Brazil", "Chile", "Argentina", "Colombia",
]

# Years to search
YEARS = list(range(2015, 2025))


class MultiSourceCSRSpider:
    """
    Multi-source spider that hits:
    1. ResponsibilityReports.com
    2. CorporateRegister.com
    3. Sustainability-reports.com
    4. Direct Google search for PDFs
    """
    
    def __init__(self, output_dir: str = "data/raw/csr_reports", max_concurrent: int = 10):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent
        self.downloaded_urls: Set[str] = set()
        self.stats = {
            "sources_checked": 0,
            "pdfs_found": 0,
            "pdfs_downloaded": 0,
            "total_bytes": 0,
            "errors": 0,
        }
        self.semaphore = None
        
        # Load existing URLs to avoid re-downloading
        self._load_existing()
    
    def _load_existing(self):
        """Load URLs of already downloaded PDFs."""
        url_cache = self.output_dir / "_downloaded_urls.json"
        if url_cache.exists():
            with open(url_cache) as f:
                self.downloaded_urls = set(json.load(f))
        logger.info(f"Loaded {len(self.downloaded_urls)} existing URLs")
    
    def _save_urls(self):
        """Save downloaded URLs."""
        url_cache = self.output_dir / "_downloaded_urls.json"
        with open(url_cache, "w") as f:
            json.dump(list(self.downloaded_urls), f)
    
    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    
    async def run(self):
        """Run all source spiders."""
        start = datetime.now()
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        logger.info("="*70)
        logger.info("MULTI-SOURCE CSR MEGA SPIDER")
        logger.info("="*70)
        logger.info(f"Output: {self.output_dir}")
        logger.info(f"Concurrency: {self.max_concurrent}")
        logger.info(f"Industries: {len(INDUSTRIES)}")
        logger.info(f"Regions: {len(REGIONS)}")
        logger.info("="*70)
        
        async with httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=30)
        ) as client:
            # Source 1: ResponsibilityReports.com
            await self._spider_responsibility_reports(client)
            
            # Source 2: Corporate Register
            await self._spider_corporate_register(client)
            
            # Source 3: Sustainability Reports
            await self._spider_sustainability_reports(client)
            
            # Source 4: Known company report archives
            await self._spider_company_archives(client)
            
            # Source 5: Industry association reports
            await self._spider_industry_associations(client)
        
        self._save_urls()
        elapsed = (datetime.now() - start).total_seconds() / 60
        
        logger.info("\n" + "="*70)
        logger.info("MULTI-SOURCE SPIDER COMPLETE")
        logger.info("="*70)
        logger.info(f"Sources checked: {self.stats['sources_checked']}")
        logger.info(f"PDFs found: {self.stats['pdfs_found']}")
        logger.info(f"PDFs downloaded: {self.stats['pdfs_downloaded']}")
        logger.info(f"Data: {self.stats['total_bytes']/1024/1024:.1f} MB")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Time: {elapsed:.1f} minutes")
        
        return self.stats
    
    async def _spider_responsibility_reports(self, client: httpx.AsyncClient):
        """Spider ResponsibilityReports.com - huge free database."""
        logger.info("\n" + "="*50)
        logger.info("SOURCE 1: ResponsibilityReports.com")
        logger.info("="*50)
        
        base_url = "https://www.responsibilityreports.com"
        
        # Search by industry
        for industry in INDUSTRIES:
            search_url = f"{base_url}/Companies?search={quote_plus(industry)}"
            await self._process_report_page(client, search_url, f"RR-{industry}")
            await asyncio.sleep(1)  # Be nice
        
        # Search by region
        for region in REGIONS:
            search_url = f"{base_url}/Companies?search={quote_plus(region)}"
            await self._process_report_page(client, search_url, f"RR-{region}")
            await asyncio.sleep(1)
        
        self.stats["sources_checked"] += 1
    
    async def _spider_corporate_register(self, client: httpx.AsyncClient):
        """Spider CorporateRegister.com - 100K+ reports."""
        logger.info("\n" + "="*50)
        logger.info("SOURCE 2: CorporateRegister.com")
        logger.info("="*50)
        
        base_url = "https://www.corporateregister.com"
        
        # Search reports section
        for year in YEARS:
            for industry in INDUSTRIES[:20]:  # Top 20 industries
                search_url = f"{base_url}/search/?q={quote_plus(industry)}+{year}+sustainability+report"
                await self._process_report_page(client, search_url, f"CR-{industry}-{year}")
                await asyncio.sleep(1.5)
        
        self.stats["sources_checked"] += 1
    
    async def _spider_sustainability_reports(self, client: httpx.AsyncClient):
        """Spider sustainability-reports.com."""
        logger.info("\n" + "="*50)
        logger.info("SOURCE 3: Sustainability-Reports.com")
        logger.info("="*50)
        
        base_url = "https://www.sustainability-reports.com"
        
        # Browse by sector
        sectors = [
            "energy", "mining", "chemicals", "manufacturing", "construction",
            "food-beverage", "healthcare", "financial", "technology", "transport"
        ]
        
        for sector in sectors:
            sector_url = f"{base_url}/sector/{sector}"
            await self._process_report_page(client, sector_url, f"SR-{sector}")
            await asyncio.sleep(1)
        
        self.stats["sources_checked"] += 1
    
    async def _spider_company_archives(self, client: httpx.AsyncClient):
        """Spider known company investor relations archives."""
        logger.info("\n" + "="*50)
        logger.info("SOURCE 4: Company IR Archives")
        logger.info("="*50)
        
        # Direct report archive URLs for major companies
        company_archives = [
            # MENA
            ("https://www.aramco.com/en/sustainability/sustainability-report", "Aramco"),
            ("https://www.adnoc.ae/en/sustainability/sustainability-reports", "ADNOC"),
            ("https://www.sabic.com/en/sustainability/sustainability-reports", "SABIC"),
            ("https://www.qatarenergy.qa/en/sustainability/reports", "QatarEnergy"),
            ("https://www.ega.ae/en/sustainability/reports", "EGA"),
            ("https://www.dpworld.com/sustainability/sustainability-reports", "DPWorld"),
            # Majors
            ("https://www.bp.com/en/global/corporate/sustainability/sustainability-reporting", "BP"),
            ("https://www.shell.com/sustainability/sustainability-reporting-and-performance-data", "Shell"),
            ("https://corporate.exxonmobil.com/sustainability/sustainability-report", "ExxonMobil"),
            ("https://www.chevron.com/sustainability/sustainability-report", "Chevron"),
            ("https://totalenergies.com/sustainability/reports-and-indicators", "TotalEnergies"),
            ("https://www.basf.com/global/en/who-we-are/sustainability/report", "BASF"),
            ("https://www.bhp.com/sustainability/reports-and-data", "BHP"),
            ("https://www.riotinto.com/sustainability/sustainability-reporting", "RioTinto"),
            ("https://www.vale.com/sustainability/reports", "Vale"),
            ("https://www.glencore.com/sustainability/reports", "Glencore"),
            ("https://www.angloamerican.com/sustainability/reporting", "AngloAmerican"),
            ("https://www.newmont.com/sustainability/sustainability-reporting", "Newmont"),
            ("https://www.nipponsteel.com/en/csr/report", "NipponSteel"),
            ("https://www.posco.co.kr/homepage/docs/eng6/jsp/sustainability/report", "POSCO"),
            ("https://www.tatasteel.com/sustainability/sustainability-report", "TataSteel"),
            ("https://global.toyota/en/sustainability/report", "Toyota"),
            ("https://www.samsung.com/global/sustainability/report", "Samsung"),
            ("https://www.siemens.com/global/en/company/sustainability/reporting", "Siemens"),
            ("https://www.nestle.com/sustainability/reports", "Nestle"),
            ("https://www.unilever.com/planet-and-society/reports", "Unilever"),
            ("https://us.pg.com/environmental-sustainability/reports", "PG"),
            ("https://www.coca-colacompany.com/sustainability/reports", "CocaCola"),
        ]
        
        for url, company in company_archives:
            await self._process_report_page(client, url, f"Archive-{company}")
            await asyncio.sleep(0.5)
        
        self.stats["sources_checked"] += 1
    
    async def _spider_industry_associations(self, client: httpx.AsyncClient):
        """Spider industry association reports."""
        logger.info("\n" + "="*50)
        logger.info("SOURCE 5: Industry Associations")
        logger.info("="*50)
        
        associations = [
            ("https://www.worldsteel.org/steel-topics/sustainability", "WorldSteel"),
            ("https://www.icmm.com/en-gb/research/sustainability", "ICMM-Mining"),
            ("https://www.iea.org/reports", "IEA-Energy"),
            ("https://www.wri.org/research", "WRI"),
            ("https://www.unep.org/resources/reports", "UNEP"),
            ("https://www.cir-strategy.com/research", "CIR"),
            ("https://www.globalreporting.org/standards", "GRI"),
            ("https://www.cdp.net/en/research", "CDP"),
            ("https://www.wbcsd.org/Programs/Circular-Economy/Resources", "WBCSD"),
            ("https://www.irena.org/publications", "IRENA-Renewable"),
        ]
        
        for url, name in associations:
            await self._process_report_page(client, url, f"Assoc-{name}")
            await asyncio.sleep(1)
        
        self.stats["sources_checked"] += 1
    
    async def _process_report_page(self, client: httpx.AsyncClient, url: str, source: str):
        """Process a page and extract PDF links."""
        try:
            response = await client.get(url, headers=self._get_headers())
            if response.status_code != 200:
                return
            
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", href=True)
            
            pdf_keywords = ["sustainability", "csr", "esg", "annual", "environment",
                           "report", "impact", "responsibility", "climate", "carbon",
                           "waste", "emissions", "environmental"]
            
            pdf_links = []
            for link in links:
                href = link.get("href", "")
                text = link.get_text(strip=True).lower()
                
                # Check if it's a PDF
                is_pdf = href.lower().endswith(".pdf") or "/pdf" in href.lower()
                has_keyword = any(kw in href.lower() or kw in text for kw in pdf_keywords)
                
                if is_pdf and has_keyword:
                    # Make absolute URL
                    if href.startswith("/"):
                        href = urljoin(url, href)
                    elif not href.startswith("http"):
                        href = urljoin(url, href)
                    
                    if href not in self.downloaded_urls:
                        pdf_links.append((href, text[:100]))
            
            if pdf_links:
                logger.info(f"[{source}] Found {len(pdf_links)} PDFs")
                self.stats["pdfs_found"] += len(pdf_links)
                
                # Download PDFs
                tasks = [self._download_pdf(client, pdf_url, title, source) 
                        for pdf_url, title in pdf_links[:50]]  # Limit per page
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            self.stats["errors"] += 1
            logger.debug(f"Error processing {url}: {e}")
    
    async def _download_pdf(self, client: httpx.AsyncClient, url: str, title: str, source: str):
        """Download a PDF file."""
        async with self.semaphore:
            if url in self.downloaded_urls:
                return
            
            try:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                response = await client.get(url, headers=self._get_headers())
                
                if response.status_code == 200 and len(response.content) > 50000:  # Min 50KB
                    # Generate filename
                    safe_source = re.sub(r'[^\w]', '_', source)
                    safe_title = re.sub(r'[^\w]', '_', title[:40]) if title else "report"
                    
                    # Extract year from URL or title
                    year = "unknown"
                    for y in YEARS:
                        if str(y) in url or str(y) in title:
                            year = str(y)
                            break
                    
                    filename = f"{safe_source}_{year}_{safe_title}_{hash(url) % 10000}.pdf"
                    filepath = self.output_dir / filename
                    
                    if not filepath.exists():
                        filepath.write_bytes(response.content)
                        self.stats["pdfs_downloaded"] += 1
                        self.stats["total_bytes"] += len(response.content)
                        self.downloaded_urls.add(url)
                        logger.debug(f"Downloaded: {filename}")
                        
            except Exception as e:
                self.stats["errors"] += 1


async def main():
    spider = MultiSourceCSRSpider(max_concurrent=15)
    await spider.run()


if __name__ == "__main__":
    asyncio.run(main())
