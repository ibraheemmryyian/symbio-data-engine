"""
GLOBAL CSR SPIDER - ROBUST VERSION
==================================
Target: 750+ Companies
Goal: Harvest PDFs using direct API requests (context.request) to bypass browser rendering issues.
"""
import asyncio
import logging
import re
import random
from pathlib import Path

# Import the massive company list
try:
    from company_list_expanded import COMPANIES
except ImportError:
    print("CRITICAL: company_list_expanded.py not found using dummy list.")
    COMPANIES = [("Example Corp", "https://example.com")]

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("spider_csr.log", mode='w'), # Overwrite old log
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GlobalCsrSpider:
    def __init__(self, output_dir='data/raw/csr_reports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_urls = set()
        
        self.priority_keywords = [
            r"sustainability", r"annual report", r"esg report", 
            r"integrated report", r"non-financial", r"environment", 
            r"climate", r"2024", r"2023", r"2025"
        ]
        
    async def download_file(self, context, url, company_name):
        if url in self.seen_urls: return
        self.seen_urls.add(url)
        
        try:
            # Clean filename
            clean_name = re.sub(r'[^a-zA-Z0-9]', '_', company_name)
            filename = url.split('/')[-1].split('?')[0]
            if len(filename) > 100: filename = filename[-100:]
            if not filename.lower().endswith('.pdf'): filename += ".pdf"
            
            save_path = self.output_dir / f"{clean_name}_{filename}"
            
            if save_path.exists():
                # logger.info(f"Skipping existing: {save_path.name}")
                return

            # DIRECT FETCH via Playwright API Context (Valid cookies, but no browser rendering)
            response = await context.request.get(url, timeout=60000)
            
            if response.status == 200:
                body = await response.body()
                # Check magic bytes for PDF to avoid saving HTML as .pdf
                if body.startswith(b'%PDF'):
                    with open(save_path, 'wb') as f:
                        f.write(body)
                    logger.info(f"SUCCESS: Downloaded {save_path.name} ({len(body)/1024/1024:.2f} MB)")
                else:
                    logger.warning(f"Ignored non-PDF content from {url}")
            else:
                logger.warning(f"Download failed {url}: Status {response.status}")
            
        except Exception as e:
            logger.error(f"Download error for {url}: {e}")

    async def process_company(self, context, name, start_url):
        logger.info(f"Scanning {name} -> {start_url}")
        page = None
        try:
            # Create a page just to find the links
            page = await context.new_page()
            
            # Block heavy stuff
            await page.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "media", "font"] 
                else route.continue_())
                
            try:
                await page.goto(start_url, timeout=30000, wait_until="domcontentloaded")
            except Exception:
                # Even if it times out, we might have loaded HTML
                pass

            # Extract links
            links = await page.eval_on_selector_all("a", """
                elements => elements.map(e => ({
                    href: e.href,
                    text: e.innerText,
                    title: e.title
                }))
            """)
            
            await page.close()
            page = None # explicitly free
            
            candidates = []
            for link in links:
                href = link.get('href')
                text = (link.get('text') or "") + " " + (link.get('title') or "")
                
                if not href or href.startswith('javascript'): continue
                
                is_pdf = href.lower().endswith('.pdf')
                text_match = any(re.search(k, text, re.IGNORECASE) for k in self.priority_keywords)
                
                if is_pdf and text_match:
                    candidates.append(href)
            
            unique = list(set(candidates))[:3]
            
            if unique:
                logger.info(f"Found {len(unique)} reports for {name}")
                for pdf_url in unique:
                    await self.download_file(context, pdf_url, name)
            
        except Exception as e:
            logger.error(f"Failed to process {name}: {e}")
            if page: await page.close()

    async def run(self):
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            print("Install playwright first")
            return

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) SymbioBot/2.0"
            )
            
            # 10 Concurrent workers - fast scrape
            sem = asyncio.Semaphore(10) 
            
            async def safe_process(company):
                async with sem:
                    await self.process_company(context, company[0], company[1])

            random.shuffle(COMPANIES)
            tasks = [safe_process(c) for c in COMPANIES]
            
            logger.info(f"Starting FAST crawl for {len(tasks)} companies...")
            await asyncio.gather(*tasks)
            
            await browser.close()
            logger.info("Crawl Complete.")

if __name__ == "__main__":
    spider = GlobalCsrSpider()
    asyncio.run(spider.run())
