
import logging
import re
import asyncio
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

import config
from .base_spider import BaseSpider

logger = logging.getLogger(__name__)

class MENACSRSpider(BaseSpider):
    """
    Spider for MENA Corporate Sustainability Reports.
    Discovery + Download of PDFs from regional giants.
    """
    
    name = "mena_csr"
    source = "csr_reports"
    
    # Comprehensive Target List for MENA Expansion
    TARGET_COMPANIES = {
        "aramco": {"name": "Saudi Aramco", "url": "https://www.aramco.com/en/sustainability"},
        "sabic": {"name": "SABIC", "url": "https://www.sabic.com/en/sustainability"},
        "maaden": {"name": "Ma'aden", "url": "https://www.maaden.com.sa/en/sustainability/reports"},
        "almarai": {"name": "Almarai", "url": "https://www.almarai.com/en/sustainability"},
        "adnoc": {"name": "ADNOC", "url": "https://www.adnoc.ae/en/sustainability"},
        "borouge": {"name": "Borouge", "url": "https://www.borouge.com/en/sustainability"},
        "dpworld": {"name": "DP World", "url": "https://www.dpworld.com/sustainability"},
        "masdar": {"name": "Masdar", "url": "https://masdar.ae/en/sustainability"},
        "oq": {"name": "OQ Oman", "url": "https://oq.com/en/sustainability"},
        "qatarenergy": {"name": "QatarEnergy", "url": "https://www.qatarenergy.qa/en/Sustainability"},
        "alba": {"name": "Alba Bahrain", "url": "https://www.albasmelter.com/sustainability"},
        "equate": {"name": "EQUATE Kuwait", "url": "https://www.equate.com/sustainability"},
        "potash": {"name": "Arab Potash Jordan", "url": "https://www.arabpotash.com/sustainability"},
        "dubal": {"name": "EGA UAE", "url": "https://www.ega.ae/en/sustainability"},
        "nadec": {"name": "Nadec Saudi", "url": "https://www.nadec.com.sa/sustainability/"},
        "savola": {"name": "Savola Group", "url": "https://www.savola.com/en/sustainability"},
        "fertile": {"name": "Fertiglobe", "url": "https://fertiglobe.com/sustainability/"},
    }
    
    REPORT_PATTERNS = [
        r"sustainability.*report",
        r"esg.*report",
        r"annual.*report",
        r"environmental.*report",
        r"impact.*report",
        r"non-financial.*report"
    ]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.discovered_pdfs = set()

    def get_urls(self) -> Generator[str, None, None]:
        for company in self.TARGET_COMPANIES.values():
            yield company["url"]

    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Standard HTTP parse. If we fail to find PDFs, we might fallback to Playwright.
        """
        return self._parse_html_for_pdfs(response, url)

    def _parse_html_for_pdfs(self, response: httpx.Response, url: str) -> Optional[dict]:
        soup = BeautifulSoup(response.text, "html.parser")
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        
        pdfs_found = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()
            
            if ".pdf" in href.lower():
                # Build Absolute URL
                pdf_url = urljoin(url, href)
                
                # Filter for Reports
                is_report = any(re.search(p, text) or re.search(p, href.lower()) for p in self.REPORT_PATTERNS)
                
                if is_report and pdf_url not in self.discovered_pdfs:
                    self.discovered_pdfs.add(pdf_url)
                    pdfs_found.append({"url": pdf_url, "title": text})

        logger.info(f"   ✅ Found {len(pdfs_found)} reports on {url}")
        
        for pdf in pdfs_found:
            self._download_pdf(pdf["url"], pdf["title"])
            
        return {"pdfs": len(pdfs_found)}

    def _download_pdf(self, url: str, title: str):
        if not self.should_continue(): return
        
        resp = self.fetch(url)
        if resp and resp.content.startswith(b"%PDF"):
            self.save_raw(
                content=resp.content,
                url=url,
                document_type="pdf",
                metadata={"title": title, "source": "mena_csr"}
            )

    async def run_playwright_discovery(self):
        """
        Optional: Use Playwright for those tricky 'Lazy Load' or 'WAF' sites.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for name, company in self.TARGET_COMPANIES.items():
                logger.info(f"🌐 Playwright Discovery for {company['name']}...")
                try:
                    await page.goto(company["url"], wait_until="networkidle", timeout=60000)
                    # Pull all PDF links
                    links = await page.eval_on_selector_all("a[href*='.pdf']", "elements => elements.map(e => ({href: e.href, text: e.innerText}))")
                    for link in links:
                        pdf_url = link["href"]
                        text = link["text"].lower()
                        is_report = any(re.search(p, text) or re.search(p, pdf_url.lower()) for p in self.REPORT_PATTERNS)
                        if is_report and pdf_url not in self.discovered_pdfs:
                            self.discovered_pdfs.add(pdf_url)
                            logger.info(f"      ✨ Found PDF via Playwright: {pdf_url}")
                            # Download (can use httpx here for efficiency)
                            self._download_pdf(pdf_url, text)
                except Exception as e:
                    logger.warning(f"      ⚠️ Playwright failed for {name}: {e}")
                    
            await browser.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spider = MENACSRSpider(limit=30)
    # Standard run
    spider.run()
    # Playwright run (optional)
    # asyncio.run(spider.run_playwright_discovery())
