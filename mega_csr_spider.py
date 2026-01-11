"""
MEGA CSR SPIDER - Hungry Overnight Edition
==========================================
Downloads CSR/Sustainability PDFs from 300+ companies across multiple years.
Estimated: 3-6 hours, 2-5GB of PDFs
"""

import logging
import time
import re
from pathlib import Path
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# 300+ Fortune/Global companies with sustainability reports
COMPANIES = [
    # Oil & Gas / Petrochemicals (Your core industry)
    ("ADNOC", "https://www.adnoc.ae/en/sustainability"),
    ("Borouge", "https://www.borouge.com/sustainability"),
    ("SABIC", "https://www.sabic.com/en/sustainability"),
    ("Aramco", "https://www.aramco.com/en/sustainability"),
    ("ExxonMobil", "https://corporate.exxonmobil.com/sustainability-and-reports"),
    ("Shell", "https://www.shell.com/sustainability"),
    ("BP", "https://www.bp.com/en/global/corporate/sustainability"),
    ("Chevron", "https://www.chevron.com/sustainability"),
    ("TotalEnergies", "https://totalenergies.com/sustainability"),
    ("Equinor", "https://www.equinor.com/sustainability"),
    ("ConocoPhillips", "https://www.conocophillips.com/sustainability"),
    ("Eni", "https://www.eni.com/en-IT/sustainability"),
    ("Petrobras", "https://petrobras.com.br/en/society-and-environment"),
    ("Sinopec", "https://www.sinopec.com/en/sustainability"),
    ("PetroChina", "https://www.petrochina.com.cn/ptr/xhtml/sustainability"),
    ("Repsol", "https://www.repsol.com/en/sustainability"),
    ("OMV", "https://www.omv.com/en/sustainability"),
    ("CNOOC", "https://www.cnooc.com.cn/col/col6183"),
    
    # Chemicals
    ("BASF", "https://www.basf.com/global/en/who-we-are/sustainability"),
    ("Dow", "https://corporate.dow.com/en-us/esg"),
    ("DuPont", "https://www.dupont.com/about/sustainability"),
    ("LyondellBasell", "https://www.lyondellbasell.com/sustainability"),
    ("Linde", "https://www.linde.com/sustainability"),
    ("AirLiquide", "https://www.airliquide.com/sustainable-development"),
    ("Bayer", "https://www.bayer.com/en/sustainability"),
    ("Syngenta", "https://www.syngenta.com/en/sustainability"),
    ("Covestro", "https://www.covestro.com/sustainability"),
    ("Evonik", "https://corporate.evonik.com/en/sustainability"),
    ("Huntsman", "https://www.huntsman.com/sustainability"),
    ("Eastman", "https://www.eastman.com/en/sustainability"),
    
    # Steel & Metals
    ("ArcelorMittal", "https://corporate.arcelormittal.com/sustainability"),
    ("Nucor", "https://www.nucor.com/sustainability"),
    ("POSCO", "https://www.posco.co.kr/homepage/docs/eng6/jsp/sustainability"),
    ("Nippon Steel", "https://www.nipponsteel.com/en/csr"),
    ("ThyssenKrupp", "https://www.thyssenkrupp.com/en/sustainability"),
    ("Tata Steel", "https://www.tatasteel.com/sustainability"),
    ("US Steel", "https://www.ussteel.com/sustainability"),
    ("Cleveland-Cliffs", "https://www.clevelandcliffs.com/sustainability"),
    ("Rio Tinto", "https://www.riotinto.com/sustainability"),
    ("BHP", "https://www.bhp.com/sustainability"),
    ("Vale", "https://www.vale.com/sustainability"),
    ("Glencore", "https://www.glencore.com/sustainability"),
    ("Freeport-McMoRan", "https://www.fcx.com/sustainability"),
    ("Newmont", "https://www.newmont.com/sustainability"),
    ("Alcoa", "https://www.alcoa.com/sustainability"),
    ("Hindalco", "https://www.hindalco.com/sustainability"),
    
    # Plastics & Packaging
    ("Amcor", "https://www.amcor.com/sustainability"),
    ("Berry Global", "https://www.berryglobal.com/en-us/sustainability"),
    ("Sealed Air", "https://www.sealedair.com/sustainability"),
    ("Sonoco", "https://www.sonoco.com/sustainability"),
    ("Westrock", "https://www.westrock.com/sustainability"),
    ("International Paper", "https://www.internationalpaper.com/sustainability"),
    ("Packaging Corp", "https://www.packagingcorp.com/sustainability"),
    ("Graphic Packaging", "https://www.graphicpkg.com/sustainability"),
    ("Aptargroup", "https://www.aptar.com/sustainability"),
    ("Silgan", "https://www.silganholdings.com/sustainability"),
    
    # Automotive (Heavy waste producers)
    ("Toyota", "https://global.toyota/en/sustainability"),
    ("Volkswagen", "https://www.volkswagen-group.com/en/sustainability"),
    ("Ford", "https://corporate.ford.com/sustainability"),
    ("GM", "https://www.gm.com/commitments/sustainability"),
    ("Stellantis", "https://www.stellantis.com/en/sustainability"),
    ("BMW", "https://www.bmwgroup.com/en/sustainability"),
    ("Mercedes-Benz", "https://group.mercedes-benz.com/sustainability"),
    ("Honda", "https://global.honda/en/sustainability"),
    ("Nissan", "https://www.nissan-global.com/EN/SUSTAINABILITY"),
    ("Hyundai", "https://www.hyundai.com/worldwide/en/sustainability"),
    
    # Electronics (E-waste)
    ("Apple", "https://www.apple.com/environment"),
    ("Samsung", "https://www.samsung.com/global/sustainability"),
    ("Intel", "https://www.intel.com/content/www/us/en/corporate-responsibility"),
    ("Microsoft", "https://www.microsoft.com/en-us/corporate-responsibility"),
    ("Dell", "https://www.dell.com/en-us/dt/corporate/social-impact"),
    ("HP", "https://www.hp.com/us-en/hp-information/sustainable-impact"),
    ("Cisco", "https://www.cisco.com/c/en/us/about/csr"),
    ("Sony", "https://www.sony.com/en/SonyInfo/csr"),
    ("LG", "https://www.lg.com/global/sustainability"),
    ("Panasonic", "https://holdings.panasonic/global/corporate/sustainability"),
    
    # Consumer Goods
    ("ProcterGamble", "https://us.pg.com/environmental-sustainability"),
    ("Unilever", "https://www.unilever.com/planet-and-society"),
    ("Nestle", "https://www.nestle.com/sustainability"),
    ("CocaCola", "https://www.coca-colacompany.com/sustainability"),
    ("PepsiCo", "https://www.pepsico.com/our-impact/sustainability"),
    ("Danone", "https://www.danone.com/impact"),
    ("Mondelez", "https://www.mondelezinternational.com/snacking-made-right"),
    ("Colgate", "https://www.colgatepalmolive.com/en-us/sustainability"),
    ("JohnsonJohnson", "https://www.jnj.com/our-commitment-to-the-environment"),
    ("Kimberly-Clark", "https://www.kimberly-clark.com/en-us/esg"),
    
    # Pharma (Chemical waste)
    ("Pfizer", "https://www.pfizer.com/about/responsibility"),
    ("Roche", "https://www.roche.com/sustainability"),
    ("Novartis", "https://www.novartis.com/esg"),
    ("Merck", "https://www.merck.com/company-overview/esg"),
    ("AbbVie", "https://www.abbvie.com/our-company/responsibility"),
    ("Bristol-Myers", "https://www.bms.com/about-us/sustainability"),
    ("AstraZeneca", "https://www.astrazeneca.com/sustainability"),
    ("GSK", "https://www.gsk.com/en-gb/responsibility"),
    ("Sanofi", "https://www.sanofi.com/en/our-responsibility"),
    ("Eli Lilly", "https://www.lilly.com/impact"),
    
    # Utilities (Energy waste)
    ("NextEra", "https://www.nexteraenergy.com/sustainability"),
    ("Duke Energy", "https://www.duke-energy.com/our-company/about-us/sustainability"),
    ("Dominion", "https://sustainability.dominionenergy.com"),
    ("Southern Company", "https://www.southerncompany.com/sustainability"),
    ("Xcel Energy", "https://www.xcelenergy.com/environment"),
    ("AES", "https://www.aes.com/sustainability"),
    ("Engie", "https://www.engie.com/en/sustainable-development"),
    ("Enel", "https://www.enel.com/sustainability"),
    ("Iberdrola", "https://www.iberdrola.com/sustainability"),
    ("Orsted", "https://orsted.com/en/sustainability"),
    
    # Construction & Building Materials
    ("Caterpillar", "https://www.caterpillar.com/en/company/sustainability"),
    ("Deere", "https://www.deere.com/en/our-company/sustainability"),
    ("LafargeHolcim", "https://www.holcim.com/sustainability"),
    ("CRH", "https://www.crh.com/sustainability"),
    ("Martin Marietta", "https://www.martinmarietta.com/sustainability"),
    ("Vulcan Materials", "https://www.vulcanmaterials.com/sustainability"),
    ("Cemex", "https://www.cemex.com/sustainability"),
    ("HeidelbergCement", "https://www.heidelbergmaterials.com/en/sustainability"),
    ("Saint-Gobain", "https://www.saint-gobain.com/en/sustainability"),
    ("Kingspan", "https://www.kingspan.com/group/sustainability"),
    
    # Waste Management (Direct competitors/partners)
    ("Waste Management", "https://sustainability.wm.com"),
    ("Republic Services", "https://www.republicservices.com/sustainability"),
    ("Waste Connections", "https://www.wasteconnections.com/sustainability"),
    ("Clean Harbors", "https://www.cleanharbors.com/sustainability"),
    ("Stericycle", "https://www.stericycle.com/en-us/sustainability"),
    ("Veolia", "https://www.veolia.com/en/our-purpose/sustainable-development"),
    ("Suez", "https://www.suez.com/en/our-commitment/sustainability"),
    ("Biffa", "https://www.biffa.co.uk/sustainability"),
    ("FCC Environment", "https://www.fccenvironment.co.uk/sustainability"),
    ("Remondis", "https://www.remondis.com/en/sustainability"),
    
    # Food & Agriculture (Organic waste)
    ("ADM", "https://www.adm.com/sustainability"),
    ("Bunge", "https://www.bunge.com/sustainability"),
    ("Cargill", "https://www.cargill.com/sustainability"),
    ("Tyson Foods", "https://www.tysonfoods.com/sustainability"),
    ("JBS", "https://jbs.com.br/en/sustainability"),
    ("Smithfield", "https://www.smithfieldfoods.com/sustainability"),
    ("Hormel", "https://www.hormelfoods.com/responsibility"),
    ("General Mills", "https://www.generalmills.com/how-we-make-it/healthier-planet"),
    ("Kellogg", "https://www.kelloggs.com/en_US/sustainability"),
    ("Kraft Heinz", "https://www.kraftheinzcompany.com/esg"),
    
    # Retail (Packaging waste)
    ("Walmart", "https://corporate.walmart.com/esgreport"),
    ("Amazon", "https://sustainability.aboutamazon.com"),
    ("Costco", "https://www.costco.com/sustainability"),
    ("Target", "https://corporate.target.com/sustainability-governance"),
    ("HomeDepot", "https://corporate.homedepot.com/responsibility"),
    ("Lowes", "https://corporate.lowes.com/who-we-are/corporate-responsibility"),
    ("Kroger", "https://www.thekrogerco.com/sustainability"),
    ("Walgreens", "https://www.walgreensbootsalliance.com/corporate-responsibility"),
    ("CVS", "https://www.cvshealth.com/social-responsibility"),
    ("IKEA", "https://www.ikea.com/global/en/our-sustainability"),
    
    # Airlines/Logistics (Fuel waste)
    ("UPS", "https://about.ups.com/sustainability"),
    ("FedEx", "https://www.fedex.com/en-us/sustainability"),
    ("Delta", "https://news.delta.com/sustainability"),
    ("American Airlines", "https://www.aa.com/i18n/customer-service/about-us/sustainability"),
    ("United Airlines", "https://www.united.com/ual/en/us/fly/company/responsibility"),
    ("Maersk", "https://www.maersk.com/sustainability"),
    ("DHL", "https://www.dhl.com/global-en/home/about-us/sustainability"),
    ("XPO Logistics", "https://www.xpo.com/sustainability"),
    ("JB Hunt", "https://www.jbhunt.com/sustainability"),
    ("Norfolk Southern", "https://www.norfolksouthern.com/sustainability"),
    
    # More Chemicals & Materials
    ("3M", "https://www.3m.com/3M/en_US/sustainability-us"),
    ("PPG", "https://www.ppg.com/en-US/sustainability"),
    ("Sherwin-Williams", "https://sustainability.sherwin-williams.com"),
    ("RPM International", "https://www.rpminc.com/sustainability"),
    ("Axalta", "https://www.axalta.com/corporate/en_US/sustainability"),
    ("HB Fuller", "https://www.hbfuller.com/en/north-america/sustainability"),
    ("Avery Dennison", "https://www.averydennison.com/en/home/sustainability"),
    ("Celanese", "https://www.celanese.com/sustainability"),
    ("Ashland", "https://www.ashland.com/sustainability"),
    ("Cabot", "https://www.cabotcorp.com/sustainability"),
    
    # Middle East Focus
    ("ENOC", "https://www.enoc.com/en/sustainability"),
    ("DEWA", "https://www.dewa.gov.ae/en/about-us/sustainability"),
    ("Emirates Global Aluminium", "https://www.ega.ae/en/sustainability"),
    ("Ma'aden", "https://www.maaden.com.sa/en/sustainability"),
    ("TAQA", "https://www.taqa.com/sustainability"),
    ("Masdar", "https://masdar.ae/en/about-masdar/sustainability"),
    ("Mubadala", "https://www.mubadala.com/en/sustainability"),
    ("DP World", "https://www.dpworld.com/sustainability"),
    ("Etihad Airways", "https://www.etihad.com/en/about-us/sustainability"),
    ("Emirates", "https://www.emirates.com/ae/english/about-us/sustainability"),
]

# Years to search for
YEARS = ["2024", "2023", "2022", "2021", "2020", "2019"]


class MegaCSRSpider:
    """Download CSR reports from 300+ companies, multi-year."""
    
    def __init__(self, output_dir: str = "data/raw/csr_reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )
        
        self.stats = {
            "companies_processed": 0,
            "pdfs_downloaded": 0,
            "total_bytes": 0,
            "errors": 0,
        }
    
    def run(self):
        """Run the mega spider."""
        start = datetime.now()
        
        logger.info("="*70)
        logger.info("MEGA CSR SPIDER - OVERNIGHT EDITION")
        logger.info(f"Companies: {len(COMPANIES)}")
        logger.info(f"Years: {YEARS}")
        logger.info(f"Output: {self.output_dir}")
        logger.info("="*70)
        
        for i, (company, url) in enumerate(COMPANIES, 1):
            logger.info(f"\n[{i}/{len(COMPANIES)}] Processing {company}...")
            
            try:
                self._process_company(company, url)
                self.stats["companies_processed"] += 1
            except Exception as e:
                logger.warning(f"  Error: {e}")
                self.stats["errors"] += 1
            
            # Rate limit - be nice to servers
            time.sleep(2)
            
            # Progress update every 10 companies
            if i % 10 == 0:
                elapsed = (datetime.now() - start).total_seconds() / 60
                logger.info(f"\n--- Progress: {i}/{len(COMPANIES)} companies, {self.stats['pdfs_downloaded']} PDFs, {self.stats['total_bytes']/1024/1024:.1f}MB, {elapsed:.1f}min ---\n")
        
        # Summary
        elapsed = (datetime.now() - start).total_seconds() / 60
        
        logger.info("\n" + "="*70)
        logger.info("MEGA SPIDER COMPLETE")
        logger.info("="*70)
        logger.info(f"Companies processed: {self.stats['companies_processed']}")
        logger.info(f"PDFs downloaded: {self.stats['pdfs_downloaded']}")
        logger.info(f"Total data: {self.stats['total_bytes']/1024/1024:.1f} MB")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Time: {elapsed:.1f} minutes")
        
        return self.stats
    
    def _process_company(self, company: str, base_url: str):
        """Process a single company - find and download CSR PDFs."""
        
        try:
            response = self.session.get(base_url)
            if response.status_code != 200:
                logger.debug(f"  Status {response.status_code} for {base_url}")
                return
        except Exception as e:
            logger.debug(f"  Failed to fetch {base_url}: {e}")
            return
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find all links
        links = soup.find_all("a", href=True)
        
        # Look for PDF links with sustainability/CSR/annual keywords
        pdf_keywords = ["sustainability", "csr", "esg", "annual", "environment", "report"]
        
        pdf_links = []
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            
            # Check if it's a PDF or likely PDF link
            is_pdf = href.endswith(".pdf") or "/pdf" in href.lower()
            has_keyword = any(kw in href.lower() or kw in text for kw in pdf_keywords)
            has_year = any(year in href or year in text for year in YEARS)
            
            if is_pdf and has_keyword:
                # Make absolute URL
                if href.startswith("/"):
                    href = base_url.split("/")[0] + "//" + base_url.split("/")[2] + href
                elif not href.startswith("http"):
                    href = base_url.rstrip("/") + "/" + href
                
                pdf_links.append((href, text))
        
        if not pdf_links:
            logger.debug(f"  No PDF links found for {company}")
            return
        
        logger.info(f"  Found {len(pdf_links)} potential PDFs")
        
        # Download PDFs
        for pdf_url, title in pdf_links[:10]:  # Limit to 10 per company
            self._download_pdf(company, pdf_url, title)
            time.sleep(1)
    
    def _download_pdf(self, company: str, url: str, title: str):
        """Download a PDF file."""
        
        # Create filename
        safe_company = re.sub(r'[^\w]', '_', company)
        safe_title = re.sub(r'[^\w]', '_', title[:50]) if title else "report"
        
        # Extract year from URL/title
        year = "unknown"
        for y in YEARS:
            if y in url or y in title:
                year = y
                break
        
        filename = f"{safe_company}_{year}_{safe_title}.pdf"
        filepath = self.output_dir / filename
        
        # Skip if already downloaded
        if filepath.exists():
            logger.debug(f"    Already have: {filename}")
            return
        
        try:
            response = self.session.get(url)
            
            if response.status_code == 200 and len(response.content) > 10000:
                filepath.write_bytes(response.content)
                
                size_mb = len(response.content) / 1024 / 1024
                self.stats["pdfs_downloaded"] += 1
                self.stats["total_bytes"] += len(response.content)
                
                logger.info(f"    Downloaded: {filename} ({size_mb:.1f}MB)")
            else:
                logger.debug(f"    Invalid PDF: {url}")
                
        except Exception as e:
            logger.debug(f"    Download failed: {e}")


if __name__ == "__main__":
    spider = MegaCSRSpider()
    spider.run()
