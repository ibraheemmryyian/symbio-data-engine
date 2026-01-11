"""
SMART PLAYWRIGHT CSR SPIDER
============================
Uses headless browser ONLY when fast HTTP requests fail.

Decision flow:
1. Try fast httpx request (100ms timeout)
2. If blocked/JS-rendered → fall back to Playwright
3. Use Playwright with stealth to bypass blocks

Target: High-yield sites that block regular requests
"""
import asyncio
import logging
import random
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Set, List, Tuple
from urllib.parse import urljoin

# Try imports
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("Playwright not installed. Run: pip install playwright && playwright install chromium")

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# High-value targets that typically block regular requests
HIGH_VALUE_TARGETS = [
    # Report databases (need JS)
    ("https://www.responsibilityreports.com/Companies?search=mining", "RR-Mining"),
    ("https://www.responsibilityreports.com/Companies?search=oil+gas", "RR-OilGas"),
    ("https://www.responsibilityreports.com/Companies?search=steel", "RR-Steel"),
    ("https://www.responsibilityreports.com/Companies?search=chemical", "RR-Chemical"),
    ("https://www.responsibilityreports.com/Companies?search=cement", "RR-Cement"),
    ("https://www.responsibilityreports.com/Companies?search=manufacturing", "RR-Manufacturing"),
    ("https://www.responsibilityreports.com/Companies?search=energy", "RR-Energy"),
    ("https://www.responsibilityreports.com/Companies?search=utilities", "RR-Utilities"),
    ("https://www.responsibilityreports.com/Companies?search=automotive", "RR-Auto"),
    ("https://www.responsibilityreports.com/Companies?search=food", "RR-Food"),
    ("https://www.responsibilityreports.com/Companies?search=healthcare", "RR-Healthcare"),
    ("https://www.responsibilityreports.com/Companies?search=retail", "RR-Retail"),
    ("https://www.responsibilityreports.com/Companies?search=technology", "RR-Tech"),
    ("https://www.responsibilityreports.com/Companies?search=UAE", "RR-UAE"),
    ("https://www.responsibilityreports.com/Companies?search=Saudi", "RR-Saudi"),
    ("https://www.responsibilityreports.com/Companies?search=Qatar", "RR-Qatar"),
    
    # Major company report pages (often JS-heavy)
    ("https://www.aramco.com/en/sustainability/sustainability-report", "Aramco"),
    ("https://www.sabic.com/en/sustainability/reports", "SABIC"),
    ("https://www.adnoc.ae/en/sustainability", "ADNOC"),
    ("https://www.qatarenergy.qa/en/sustainability", "QatarEnergy"),
    ("https://www.bp.com/en/global/corporate/sustainability/sustainability-reporting", "BP"),
    ("https://www.shell.com/sustainability/sustainability-reporting-and-performance-data", "Shell"),
    ("https://corporate.exxonmobil.com/sustainability", "ExxonMobil"),
    ("https://www.chevron.com/sustainability", "Chevron"),
    ("https://totalenergies.com/sustainability/reports-and-indicators", "TotalEnergies"),
    ("https://www.bhp.com/sustainability/reports-and-data", "BHP"),
    ("https://www.riotinto.com/sustainability/sustainability-reporting", "RioTinto"),
    ("https://www.vale.com/sustainability", "Vale"),
    ("https://www.glencore.com/sustainability/reports", "Glencore"),
    ("https://www.basf.com/global/en/who-we-are/sustainability/we-report", "BASF"),
    ("https://www.nestle.com/sustainability/reports", "Nestle"),
    ("https://www.unilever.com/planet-and-society/sustainability-reporting-centre", "Unilever"),
    
    # Industry associations (usually have direct PDFs)
    ("https://www.worldsteel.org/steel-topics/sustainability/sustainability-indicators", "WorldSteel"),
    ("https://www.icmm.com/en-gb/our-principles/sustainability", "ICMM"),
    ("https://www.iea.org/reports", "IEA"),
    ("https://www.irena.org/publications", "IRENA"),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


class SmartPlaywrightSpider:
    """
    Smart spider that:
    1. Tries fast HTTP first
    2. Falls back to Playwright for blocked/JS sites
    3. Uses stealth settings to avoid detection
    """
    
    def __init__(self, output_dir: str = "data/raw/csr_reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.downloaded_urls: Set[str] = set()
        self.stats = {
            "http_attempts": 0,
            "http_success": 0,
            "playwright_attempts": 0,
            "playwright_success": 0,
            "pdfs_found": 0,
            "pdfs_downloaded": 0,
            "total_bytes": 0,
        }
        self._load_existing()
    
    def _load_existing(self):
        cache = self.output_dir / "_smart_spider_urls.json"
        if cache.exists():
            with open(cache) as f:
                self.downloaded_urls = set(json.load(f))
        logger.info(f"Loaded {len(self.downloaded_urls)} existing URLs")
    
    def _save_urls(self):
        cache = self.output_dir / "_smart_spider_urls.json"
        with open(cache, "w") as f:
            json.dump(list(self.downloaded_urls), f)
    
    async def run(self):
        """Run the smart spider."""
        start = datetime.now()
        
        logger.info("="*70)
        logger.info("SMART PLAYWRIGHT CSR SPIDER")
        logger.info("="*70)
        logger.info(f"Targets: {len(HIGH_VALUE_TARGETS)}")
        logger.info(f"Playwright available: {PLAYWRIGHT_AVAILABLE}")
        logger.info("="*70)
        
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright not installed! Run: pip install playwright && playwright install chromium")
            return self.stats
        
        # Process targets
        async with async_playwright() as p:
            # Launch browser with stealth settings
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--no-sandbox',
                ]
            )
            
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
            )
            
            # Process each target
            for url, name in HIGH_VALUE_TARGETS:
                await self._process_target(context, url, name)
                await asyncio.sleep(random.uniform(2, 4))  # Be nice
            
            await browser.close()
        
        self._save_urls()
        elapsed = (datetime.now() - start).total_seconds() / 60
        
        logger.info("\n" + "="*70)
        logger.info("SMART SPIDER COMPLETE")
        logger.info("="*70)
        logger.info(f"HTTP attempts: {self.stats['http_attempts']} (success: {self.stats['http_success']})")
        logger.info(f"Playwright attempts: {self.stats['playwright_attempts']} (success: {self.stats['playwright_success']})")
        logger.info(f"PDFs found: {self.stats['pdfs_found']}")
        logger.info(f"PDFs downloaded: {self.stats['pdfs_downloaded']}")
        logger.info(f"Data: {self.stats['total_bytes']/1024/1024:.1f} MB")
        logger.info(f"Time: {elapsed:.1f} minutes")
        
        return self.stats
    
    async def _try_http_first(self, url: str) -> Tuple[bool, str]:
        """Try fast HTTP request first."""
        self.stats["http_attempts"] += 1
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, headers={"User-Agent": random.choice(USER_AGENTS)})
                
                if response.status_code == 200:
                    # Check if it's real content or JS placeholder
                    text = response.text
                    if len(text) > 5000 and "pdf" in text.lower():
                        self.stats["http_success"] += 1
                        return True, text
        except Exception:
            pass
        
        return False, ""
    
    async def _process_target(self, context, url: str, name: str):
        """Process a target URL - try HTTP first, then Playwright."""
        logger.info(f"[{name}] Processing: {url}")
        
        # Try HTTP first (fast path)
        success, html = await self._try_http_first(url)
        
        if not success:
            # Fall back to Playwright
            self.stats["playwright_attempts"] += 1
            logger.info(f"[{name}] HTTP failed, using Playwright")
            
            try:
                page = await context.new_page()
                
                # Navigate with wait for network idle
                await page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Wait for dynamic content
                await asyncio.sleep(2)
                
                # Scroll to trigger lazy loading
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)
                
                html = await page.content()
                await page.close()
                
                self.stats["playwright_success"] += 1
                
            except Exception as e:
                logger.debug(f"[{name}] Playwright error: {e}")
                return
        
        # Parse HTML and find PDFs
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", href=True)
        
        pdf_keywords = ["sustainability", "csr", "esg", "annual", "environment",
                       "report", "impact", "responsibility", "climate", "carbon"]
        
        pdf_links = []
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            
            is_pdf = href.lower().endswith(".pdf") or "/pdf" in href.lower()
            has_keyword = any(kw in href.lower() or kw in text for kw in pdf_keywords)
            
            if is_pdf or (has_keyword and "download" in text.lower()):
                if href.startswith("/"):
                    href = urljoin(url, href)
                elif not href.startswith("http"):
                    href = urljoin(url, href)
                
                if href not in self.downloaded_urls:
                    pdf_links.append((href, text[:80]))
        
        if pdf_links:
            logger.info(f"[{name}] Found {len(pdf_links)} PDF links")
            self.stats["pdfs_found"] += len(pdf_links)
            
            # Download PDFs
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                for pdf_url, title in pdf_links[:20]:  # Limit per page
                    await self._download_pdf(client, pdf_url, title, name)
    
    async def _download_pdf(self, client: httpx.AsyncClient, url: str, title: str, source: str):
        """Download a PDF file."""
        if url in self.downloaded_urls:
            return
        
        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            response = await client.get(url, headers={"User-Agent": random.choice(USER_AGENTS)})
            
            if response.status_code == 200 and len(response.content) > 50000:
                # Check if it's actually a PDF
                if response.content[:4] == b'%PDF':
                    safe_source = re.sub(r'[^\w]', '_', source)
                    safe_title = re.sub(r'[^\w]', '_', title[:40]) if title else "report"
                    filename = f"Smart_{safe_source}_{safe_title}_{hash(url) % 10000}.pdf"
                    filepath = self.output_dir / filename
                    
                    if not filepath.exists():
                        filepath.write_bytes(response.content)
                        self.stats["pdfs_downloaded"] += 1
                        self.stats["total_bytes"] += len(response.content)
                        self.downloaded_urls.add(url)
                        logger.info(f"Downloaded: {filename}")
                        
        except Exception as e:
            logger.debug(f"Download error: {e}")


async def main():
    spider = SmartPlaywrightSpider()
    await spider.run()


if __name__ == "__main__":
    asyncio.run(main())
