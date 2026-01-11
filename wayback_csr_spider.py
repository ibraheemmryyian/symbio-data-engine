"""
WAYBACK MACHINE CSR SPIDER
==========================
Downloads historical CSR/sustainability reports from the Internet Archive.
Can access reports from companies that have changed URLs or removed old reports.

Target: 5,000+ historical PDFs
"""
import asyncio
import logging
import random
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Set
from urllib.parse import quote

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Companies to search in Wayback
COMPANIES_WAYBACK = [
    # MENA - Focus
    ("aramco.com", "Aramco"),
    ("adnoc.ae", "ADNOC"),
    ("sabic.com", "SABIC"),
    ("qatarenergy.qa", "QatarEnergy"),
    ("qp.com.qa", "QatarPetroleum"),
    ("ega.ae", "EGA"),
    ("dpworld.com", "DPWorld"),
    ("masdar.ae", "Masdar"),
    ("mubadala.com", "Mubadala"),
    ("taqa.com", "TAQA"),
    ("maaden.com.sa", "Maaden"),
    ("acwapower.com", "ACWAPower"),
    ("se.com.sa", "SaudiElectricity"),
    ("kpc.com.kw", "KPC"),
    ("oq.com", "OQOman"),
    ("pdo.co.om", "PDO"),
    ("bapco.net", "BAPCO"),
    ("albasmelter.com", "Alba"),
    ("ocpgroup.ma", "OCP"),
    
    # Oil & Gas Majors
    ("bp.com", "BP"),
    ("shell.com", "Shell"),
    ("exxonmobil.com", "ExxonMobil"),
    ("chevron.com", "Chevron"),
    ("totalenergies.com", "TotalEnergies"),
    ("conocophillips.com", "ConocoPhillips"),
    ("equinor.com", "Equinor"),
    ("eni.com", "Eni"),
    ("repsol.com", "Repsol"),
    ("petrobras.com.br", "Petrobras"),
    ("pemex.com", "Pemex"),
    ("petronas.com", "Petronas"),
    ("sinopec.com", "Sinopec"),
    ("cnpc.com.cn", "CNPC"),
    
    # Mining Majors
    ("bhp.com", "BHP"),
    ("riotinto.com", "RioTinto"),
    ("vale.com", "Vale"),
    ("glencore.com", "Glencore"),
    ("angloamerican.com", "AngloAmerican"),
    ("newmont.com", "Newmont"),
    ("barrick.com", "Barrick"),
    ("freeportmcmoran.com", "Freeport"),
    ("fcx.com", "FCX"),
    ("codelco.com", "Codelco"),
    ("goldfields.com", "GoldFields"),
    ("anglogoldashanti.com", "AngloGold"),
    
    # Steel & Metals
    ("nipponsteel.com", "NipponSteel"),
    ("posco.co.kr", "POSCO"),
    ("tatasteel.com", "TataSteel"),
    ("jfe-steel.co.jp", "JFE"),
    ("arcelormittal.com", "ArcelorMittal"),
    ("nucor.com", "Nucor"),
    ("ussteel.com", "USSteel"),
    ("thyssenkrupp.com", "ThyssenKrupp"),
    ("baosteel.com", "Baosteel"),
    ("hyundai-steel.com", "HyundaiSteel"),
    
    # Chemicals
    ("basf.com", "BASF"),
    ("dow.com", "Dow"),
    ("dupont.com", "DuPont"),
    ("lyondellbasell.com", "LyondellBasell"),
    ("sabic.com", "SABIC"),
    ("covestro.com", "Covestro"),
    ("evonik.com", "Evonik"),
    ("lanxess.com", "Lanxess"),
    ("huntsman.com", "Huntsman"),
    ("eastman.com", "Eastman"),
    
    # Cement & Construction
    ("holcim.com", "Holcim"),
    ("heidelbergmaterials.com", "Heidelberg"),
    ("cemex.com", "CEMEX"),
    ("martinmarietta.com", "MartinMarietta"),
    ("vulcanmaterials.com", "Vulcan"),
    ("conch.cn", "AnhuiConch"),
    ("ultratech.cement.com", "UltraTech"),
    
    # Automotive
    ("toyota.com", "Toyota"),
    ("honda.com", "Honda"),
    ("volkswagen.com", "VW"),
    ("bmw.com", "BMW"),
    ("mercedes-benz.com", "Mercedes"),
    ("ford.com", "Ford"),
    ("gm.com", "GM"),
    ("hyundai.com", "Hyundai"),
    ("tesla.com", "Tesla"),
    
    # Consumer Goods
    ("nestle.com", "Nestle"),
    ("unilever.com", "Unilever"),
    ("pg.com", "PG"),
    ("coca-colacompany.com", "CocaCola"),
    ("pepsico.com", "PepsiCo"),
    ("danone.com", "Danone"),
    ("loreal.com", "LOreal"),
    
    # Waste & Environment
    ("wm.com", "WasteManagement"),
    ("republicservices.com", "Republic"),
    ("veolia.com", "Veolia"),
    ("suez.com", "Suez"),
    ("cleanharbors.com", "CleanHarbors"),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]


class WaybackCSRSpider:
    """
    Spider that searches Wayback Machine for historical CSR/sustainability PDFs.
    """
    
    def __init__(self, output_dir: str = "data/raw/csr_reports", max_concurrent: int = 5):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent
        self.downloaded_urls: Set[str] = set()
        self.stats = {
            "companies_searched": 0,
            "snapshots_found": 0,
            "pdfs_downloaded": 0,
            "total_bytes": 0,
            "errors": 0,
        }
        self.semaphore = None
        self._load_existing()
    
    def _load_existing(self):
        cache = self.output_dir / "_wayback_urls.json"
        if cache.exists():
            with open(cache) as f:
                self.downloaded_urls = set(json.load(f))
    
    def _save_urls(self):
        cache = self.output_dir / "_wayback_urls.json"
        with open(cache, "w") as f:
            json.dump(list(self.downloaded_urls), f)
    
    async def run(self):
        """Run the Wayback spider."""
        start = datetime.now()
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        logger.info("="*70)
        logger.info("WAYBACK MACHINE CSR SPIDER")
        logger.info("="*70)
        logger.info(f"Companies to search: {len(COMPANIES_WAYBACK)}")
        logger.info(f"Output: {self.output_dir}")
        logger.info("="*70)
        
        async with httpx.AsyncClient(
            timeout=120.0,  # Wayback is slow
            follow_redirects=True,
            limits=httpx.Limits(max_connections=10)
        ) as client:
            # Process companies in batches
            batch_size = 10
            for i in range(0, len(COMPANIES_WAYBACK), batch_size):
                batch = COMPANIES_WAYBACK[i:i+batch_size]
                tasks = [
                    self._search_company(client, domain, name)
                    for domain, name in batch
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                logger.info(f"Progress: {min(i+batch_size, len(COMPANIES_WAYBACK))}/{len(COMPANIES_WAYBACK)} companies")
        
        self._save_urls()
        elapsed = (datetime.now() - start).total_seconds() / 60
        
        logger.info("\n" + "="*70)
        logger.info("WAYBACK SPIDER COMPLETE")
        logger.info("="*70)
        logger.info(f"Companies searched: {self.stats['companies_searched']}")
        logger.info(f"Snapshots found: {self.stats['snapshots_found']}")
        logger.info(f"PDFs downloaded: {self.stats['pdfs_downloaded']}")
        logger.info(f"Data: {self.stats['total_bytes']/1024/1024:.1f} MB")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Time: {elapsed:.1f} minutes")
        
        return self.stats
    
    async def _search_company(self, client: httpx.AsyncClient, domain: str, company_name: str):
        """Search Wayback Machine for a company's CSR PDFs."""
        async with self.semaphore:
            try:
                # Use CDX API to search for PDFs
                cdx_url = (
                    f"https://web.archive.org/cdx/search/cdx"
                    f"?url={domain}/*sustainability*.pdf"
                    f"&output=json"
                    f"&limit=100"
                    f"&filter=mimetype:application/pdf"
                )
                
                response = await client.get(cdx_url)
                if response.status_code != 200:
                    # Try alternative search patterns
                    cdx_url = (
                        f"https://web.archive.org/cdx/search/cdx"
                        f"?url={domain}/*csr*.pdf"
                        f"&output=json"
                        f"&limit=50"
                    )
                    response = await client.get(cdx_url)
                
                if response.status_code != 200:
                    return
                
                try:
                    results = response.json()
                except:
                    return
                
                if not results or len(results) < 2:  # First row is header
                    return
                
                self.stats["companies_searched"] += 1
                snapshots = results[1:]  # Skip header
                self.stats["snapshots_found"] += len(snapshots)
                
                logger.info(f"[{company_name}] Found {len(snapshots)} PDF snapshots")
                
                # Download unique PDFs (dedupe by URL)
                seen_urls = set()
                for row in snapshots[:30]:  # Limit per company
                    if len(row) >= 3:
                        timestamp = row[1]
                        original_url = row[2]
                        
                        if original_url in seen_urls:
                            continue
                        seen_urls.add(original_url)
                        
                        wayback_url = f"https://web.archive.org/web/{timestamp}id_/{original_url}"
                        await self._download_pdf(client, wayback_url, company_name, timestamp)
                
            except Exception as e:
                self.stats["errors"] += 1
                logger.debug(f"Error searching {company_name}: {e}")
    
    async def _download_pdf(self, client: httpx.AsyncClient, url: str, company: str, timestamp: str):
        """Download a PDF from Wayback."""
        if url in self.downloaded_urls:
            return
        
        try:
            await asyncio.sleep(random.uniform(1, 3))  # Be nice to archive.org
            
            response = await client.get(url, headers={"User-Agent": random.choice(USER_AGENTS)})
            
            if response.status_code == 200 and len(response.content) > 50000:
                # Generate filename
                year = timestamp[:4] if len(timestamp) >= 4 else "unknown"
                safe_company = re.sub(r'[^\w]', '_', company)
                filename = f"Wayback_{safe_company}_{year}_{hash(url) % 10000}.pdf"
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
    spider = WaybackCSRSpider(max_concurrent=5)
    await spider.run()


if __name__ == "__main__":
    asyncio.run(main())
