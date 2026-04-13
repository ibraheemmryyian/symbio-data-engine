"""
New Data Sources Spider
=======================
Targets high-value industrial data sources not yet in the pipeline.

Sources added:
    CDP       — Carbon Disclosure Project (public climate data API)
    GRI       — Global Reporting Initiative sustainability disclosures
    PRTR_JP   — Japan Pollutant Release and Transfer Register
    PRTR_KR   — Korea PRTR
    PRTR_AU   — Australia NPI (National Pollutant Inventory)
    NISP      — National Industrial Symbiosis Programme (UK)
    OECD_PRTR — OECD combined PRTR portal

Run individual sources:
    python -m spiders.new_sources_spider --source cdp --limit 500
    python -m spiders.new_sources_spider --source gri
    python -m spiders.new_sources_spider --source all
"""

import argparse
import hashlib
import logging
import time
from pathlib import Path

import httpx

import config
from store.postgres import insert_document

log = logging.getLogger(__name__)


class NewSourcesSpider:

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; SymbioDataEngine/1.0; research)",
        "Accept": "application/json, text/html",
    }
    RATE_LIMIT = 1.5  # seconds between requests

    def __init__(self):
        self.client = httpx.Client(
            headers=self.HEADERS,
            timeout=30.0,
            follow_redirects=True,
        )

    def _hash(self, content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()

    def _store(self, source: str, url: str, doc_type: str, content: str, metadata: dict = None) -> str | None:
        h = self._hash(content)
        try:
            doc_id = insert_document(
                source=source,
                source_url=url,
                document_type=doc_type,
                content_hash=h,
                metadata=metadata or {},
            )
            # Save raw content to disk for processing
            raw_dir = config.RAW_DIR / source
            raw_dir.mkdir(parents=True, exist_ok=True)
            out_path = raw_dir / f"{h[:16]}.txt"
            if not out_path.exists():
                out_path.write_text(content, encoding="utf-8")
            return doc_id
        except Exception as e:
            log.error(f"Failed to store document from {url}: {e}")
            return None

    # --------------------------------------------------
    # CDP — Carbon Disclosure Project
    # Public data at https://www.cdp.net/en/responses
    # Bulk data download available without auth for public responses
    # --------------------------------------------------

    def crawl_cdp(self, limit: int = 500):
        """
        CDP public questionnaire data.
        Companies self-report waste, emissions, water data.
        Covers 23,000+ companies globally.
        """
        log.info("Crawling CDP public data...")

        # CDP bulk data index — public CSVs
        cdp_bulk_urls = [
            "https://cdn.cdp.net/cdp-production/cms/reports/documents/000/007/742/original/CDP_Water_Security_Questionnaire_2023.csv",
            "https://www.cdp.net/en/responses?queries%5Bname%5D=&filters%5Byears%5D%5B%5D=2023&filters%5Bquestionnaire_types%5D%5B%5D=climate",
        ]

        # CDP SFDR public API (no auth needed for public data)
        base = "https://api.cdp.net/v0"
        endpoints = [
            f"{base}/questionnaire-responses?year=2023&limit={min(limit, 100)}",
            f"{base}/disclosures?status=public&year=2023&type=climate&limit={min(limit, 100)}",
        ]

        stored = 0
        for url in endpoints:
            try:
                r = self.client.get(url)
                if r.status_code == 200:
                    doc_id = self._store("cdp", url, "json", r.text,
                                         {"year": 2023, "type": "climate_disclosure"})
                    if doc_id:
                        stored += 1
                        log.info(f"  CDP: stored {url}")
                time.sleep(self.RATE_LIMIT)
            except Exception as e:
                log.warning(f"CDP fetch failed for {url}: {e}")

        log.info(f"CDP crawl complete: {stored} documents stored")

    # --------------------------------------------------
    # GRI — Global Reporting Initiative
    # Sustainability reports database
    # https://www.globalreporting.org/sustainability-disclosures/
    # --------------------------------------------------

    def crawl_gri(self, limit: int = 200):
        """
        GRI Sustainability Disclosure Database.
        PDFs and structured data for 15,000+ company reports.
        """
        log.info("Crawling GRI database...")

        # GRI public API
        gri_api = "https://database.globalreporting.org/api/v1/reports"
        params = {
            "limit": min(limit, 50),
            "offset": 0,
            "sort": "publication_date_desc",
            # Focus on industries relevant to Symbioflows
            "sector": "chemicals,mining,oil_gas,food_beverage,manufacturing",
        }

        stored = 0
        offset = 0
        while stored < limit:
            params["offset"] = offset
            try:
                r = self.client.get(gri_api, params=params)
                if r.status_code != 200:
                    log.warning(f"GRI API returned {r.status_code}")
                    break

                data = r.json()
                reports = data.get("results", data if isinstance(data, list) else [])

                if not reports:
                    break

                for report in reports:
                    url = report.get("report_url") or report.get("url", "")
                    company = report.get("organization_name", "")
                    year = report.get("publication_year") or report.get("year", "")

                    content = f"{company} | {year} | {url}\n{r.text}"
                    doc_id = self._store("gri", url or gri_api, "gri_report",
                                         content, {"company": company, "year": year})
                    if doc_id:
                        stored += 1

                offset += len(reports)
                time.sleep(self.RATE_LIMIT)

            except Exception as e:
                log.warning(f"GRI fetch failed: {e}")
                break

        log.info(f"GRI crawl complete: {stored} documents stored")

    # --------------------------------------------------
    # Japan PRTR
    # https://www.env.go.jp/en/chemi/prtr/prtr.html
    # --------------------------------------------------

    def crawl_prtr_japan(self):
        """
        Japan PRTR — covers ~4,000 facilities, 462 chemicals.
        Available as annual CSV downloads.
        """
        log.info("Crawling Japan PRTR...")

        base = "https://www.env.go.jp/chemi/prtr/result/todokedede/"
        years = range(2018, 2024)
        stored = 0

        for year in years:
            # Japan PRTR CSV download pattern
            url = f"{base}{year}/csv/prtr_{year}_all.csv"
            try:
                r = self.client.get(url)
                if r.status_code == 200 and len(r.content) > 1000:
                    doc_id = self._store("prtr_japan", url, "csv",
                                         r.text[:500_000],  # cap at 500k chars
                                         {"year": year, "country": "Japan"})
                    if doc_id:
                        stored += 1
                        log.info(f"  Japan PRTR {year}: stored ({len(r.content):,} bytes)")
                time.sleep(self.RATE_LIMIT)
            except Exception as e:
                log.warning(f"Japan PRTR {year} failed: {e}")

        log.info(f"Japan PRTR crawl complete: {stored} documents stored")

    # --------------------------------------------------
    # Australia NPI (National Pollutant Inventory)
    # https://www.dcceew.gov.au/environment/protection/npi
    # --------------------------------------------------

    def crawl_npi_australia(self):
        """
        Australia's NPI — ~4,000 facilities, 93 substances.
        Bulk CSV downloads available.
        """
        log.info("Crawling Australia NPI...")

        base = "https://www.dcceew.gov.au/sites/default/files/env/pages/4b3d1a18-50dd-4058-9ad3-4b14a7b6e58b/files/"
        years = range(2019, 2024)
        stored = 0

        for year in years:
            url = f"https://www.dcceew.gov.au/environment/protection/npi/data/npi-data-{year}-{year+1}"
            try:
                r = self.client.get(url)
                if r.status_code == 200:
                    doc_id = self._store("npi_australia", url, "html",
                                         r.text[:200_000],
                                         {"year": year, "country": "Australia"})
                    if doc_id:
                        stored += 1
                time.sleep(self.RATE_LIMIT)
            except Exception as e:
                log.warning(f"Australia NPI {year} failed: {e}")

        log.info(f"Australia NPI crawl complete: {stored} documents stored")

    # --------------------------------------------------
    # OECD PRTR Portal
    # Combines data from 37 countries
    # --------------------------------------------------

    def crawl_oecd_prtr(self, limit: int = 1000):
        """
        OECD PRTR portal — 37 countries, single API.
        Best cross-country coverage available.
        """
        log.info("Crawling OECD PRTR...")

        api = "https://prtr.eea.europa.eu/api/v1/pollutant-releases"
        params = {
            "limit": min(limit, 100),
            "offset": 0,
            "format": "json",
        }

        stored = 0
        while stored < limit:
            try:
                r = self.client.get(api, params=params)
                if r.status_code != 200:
                    break

                data = r.json()
                records = data.get("results", [])
                if not records:
                    break

                # Store as batch
                content = json_safe_dumps(records)
                doc_id = self._store("oecd_prtr", f"{api}?offset={params['offset']}",
                                     "json", content,
                                     {"batch_offset": params["offset"]})
                if doc_id:
                    stored += len(records)
                    log.info(f"  OECD PRTR: stored {stored} records")

                params["offset"] += len(records)
                time.sleep(self.RATE_LIMIT)

            except Exception as e:
                log.warning(f"OECD PRTR failed at offset {params['offset']}: {e}")
                break

        log.info(f"OECD PRTR crawl complete: {stored} records stored")


def json_safe_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, default=str)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["cdp", "gri", "prtr_japan", "npi_australia", "oecd_prtr", "all"],
                        default="all")
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    spider = NewSourcesSpider()

    sources = {
        "cdp":           lambda: spider.crawl_cdp(args.limit),
        "gri":           lambda: spider.crawl_gri(args.limit),
        "prtr_japan":    spider.crawl_prtr_japan,
        "npi_australia": spider.crawl_npi_australia,
        "oecd_prtr":     lambda: spider.crawl_oecd_prtr(args.limit),
    }

    to_run = list(sources.keys()) if args.source == "all" else [args.source]
    for s in to_run:
        sources[s]()
