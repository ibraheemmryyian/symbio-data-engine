"""
EIA SPIDER - PROCESS KNOWLEDGE EXTRACTION
=========================================
Target: Environmental Impact Assessments (EIAs)
Goal: Extract manufacturing process details and Process Flow Diagrams (PFDs).
Sources: IFC, EIB, and search queries for "EIA" + Company Name.
"""
import asyncio
import httpx
from pathlib import Path
import logging
import re
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EiaSpider:
    def __init__(self, output_dir='data/raw/eia'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_urls = set()
        
        # Targets: Major EIA Repositories
        self.seed_urls = [
            "https://disclosures.ifc.org/", # IFC Project Database
            "https://www.eib.org/en/projects/pipelines/index.htm", # EIB Pipeline
            "https://www.adb.org/projects", # Asian Development Bank
            "https://www.ebrd.com/work-with-us/project-finance/project-summary-documents.html", # EBRD
        ]
        
        self.keywords = [
            "environmental impact assessment", "EIA", "ESIA", 
            "process flow diagram", "manufacturing process", "waste management plan"
        ]

    async def fetch(self, client, url):
        try:
            resp = await client.get(url, timeout=30.0, follow_redirects=True)
            return resp
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    async def process_page(self, client, url):
        if url in self.seen_urls: return
        self.seen_urls.add(url)
        
        logger.info(f"Scanning: {url}")
        resp = await self.fetch(client, url)
        if not resp: return

        # Extract PDF links
        links = re.findall(r'href=["\'](.*?\.pdf)["\']', resp.text, re.IGNORECASE)
        
        for link in links:
            full_url = urljoin(url, link)
            await self.download_pdf(client, full_url)

    async def download_pdf(self, client, url):
        if url in self.seen_urls: return
        self.seen_urls.add(url)
        
        filename = url.split('/')[-1]
        filepath = self.output_dir / filename
        
        if filepath.exists():
            logger.info(f"Skipping existing: {filename}")
            return

        logger.info(f"Found PDF: {filename}")
        resp = await self.fetch(client, url)
        
        if resp and resp.status_code == 200:
            with open(filepath, 'wb') as f:
                f.write(resp.content)
            logger.info(f"Downloaded: {filepath}")

    async def run(self):
        logger.info("Starting EIA Spider...")
        async with httpx.AsyncClient(headers={'User-Agent': 'SymbioDataEngine/1.0'}) as client:
            tasks = [self.process_page(client, url) for url in self.seed_urls]
            await asyncio.gather(*tasks)
        logger.info("EIA Spider run complete.")

if __name__ == '__main__':
    spider = EiaSpider()
    asyncio.run(spider.run())
