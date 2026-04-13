"""
Symbio Data Engine - Maritime Spider
====================================
Aggressive scraping of maritime datasets: dredging, ship recycling, port cargo data.
Extracts PDFs, CSVs, and downloadable reports from port authorities and EU agencies.
"""

from typing import Generator
import httpx
import re
from .base_spider import BaseSpider


class MaritimeSpider(BaseSpider):
    """Crawl maritime, port, and dredging data sources."""

    name = "maritime"
    source = "maritime"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_urls(self) -> Generator[str, None, None]:
        """Generate URLs for maritime datasets: dredging, ship recycling, port cargo."""
        urls = [
            # EU Dredging Data - OSPAR/HELCOM reports with actual dredge volumes
            "https://www.ospar.org/documents?v=6827",
            "https://www.helcom.fi/action-areas/maritime-activities/dredging/",
            "https://www.helcom.fi/press-releases/",

            # EU Port Authority Data - Eurostat maritime transport
            "https://ec.europa.eu/eurostat/statistics-explained/index.php/Maritime_transport_statistics",
            "https://ec.europa.eu/eurostat/databrowser/view/MAR_GO_AM/default/table",

            # Rotterdam Port Authority - Cargo statistics CSVs
            "https://www.portofrotterdam.com/en/port-information/facts-and-figures/key-figures",
            "https://www.portofrotterdam.com/en/news-and-press/news",

            # Hamburg Port Authority - Traffic data and annual reports
            "https://www.hafen-hamburg.de/en/statistics",
            "https://www.hafen-hamburg.de/en/press",

            # Antwerp Port - Cargo & handling data
            "https://www.portofantwerp.com/en/news-press",
            "https://www.portofantwerp.com/en/statistics",

            # Basel Convention - Ship recycling facilities database
            "http://www.basel.int/Countries/ListofPartiestotheBaselConvention/tabid/4499/Default.aspx",

            # Hong Kong International Convention - Ship Recycling Facilities List
            "https://wwwcdn.imo.org/localresources/en/OurWork/Environment/Documents/Hong%20Kong%20Convention/HKC%20LIST%20OF%20AUTHORIZED%20FACILITIES.pdf",

            # International Maritime Organization - Monthly and annual statistics
            "https://www.imo.org/en/OurWork/Statistics/Pages/Default.aspx",
            "https://www.imo.org/en/OurWork/Statistics/Pages/What-are-the-statistics-pages-.aspx",

            # European Commission - Maritime Spatial Planning data
            "https://www.emodnet.eu/",
            "https://www.emodnet.eu/data",

            # CEDA (Central Dredging Association) - Dredging research & reports
            "https://www.dredging.org/library/references",
            "https://www.dredging.org/research",

            # PIANC (Ports & Inland Navigation) - Port development reports
            "https://www.pianc.org/documents",
            "https://www.pianc.org/publications",

            # World Bank - Maritime transport data
            "https://data.worldbank.org/indicator/IS.SHP.GOOD.TU",

            # UN Conference on Trade & Development - Shipping review
            "https://unctad.org/webflyer/review-maritime-transport",
            "https://unctad.org/publications",

            # Danish Straits data - Dredging volumes by country
            "https://www.dma.dk/",

            # German Federal Maritime & Hydrographic Agency (BSH)
            "https://www.bsh.de/EN/",

            # Netherlands PBL Environmental Assessment Agency - Port & dredging data
            "https://www.pbl.nl/en",

            # European Environment Agency - Waste & maritime data
            "https://www.eea.europa.eu/themes/waste",
            "https://www.eea.europa.eu/data-and-maps",

            # Ship breaking statistics - Global Maritime Forum & others
            "https://www.gcaptain.com/ship-breaking-statistics/",
            "https://www.maritime-executive.com/article/ship-breaking",

            # Data.gov portals - US EPA maritime & port data
            "https://catalog.data.gov/dataset?q=maritime+port+cargo",
            "https://catalog.data.gov/dataset?q=dredging+sediment",
        ]
        for url in urls:
            yield url

    def parse(self, response: httpx.Response, url: str) -> None:
        """Parse maritime response and extract downloadable datasets (PDFs, CSVs, Excel)."""
        if response.status_code != 200:
            return

        # Categorize by source
        source_type = self._categorize_source(url)
        byproduct_types = self._get_byproduct_types(source_type)

        # Save the main page
        self.save_raw(
            content=response.content,
            url=url,
            document_type="html",
            metadata={
                "source_type": source_type,
                "year": 2024,
                "industry": "maritime",
                "byproduct_types": byproduct_types,
            }
        )

        # Extract downloadable files (PDFs, CSVs, Excel) from the response
        self._extract_and_fetch_datasets(response, url, source_type, byproduct_types)

    def _categorize_source(self, url: str) -> str:
        """Determine source type from URL."""
        url_lower = url.lower()

        if "recycling" in url_lower or "ship-recycling" in url_lower or "breaking" in url_lower:
            return "ship_recycling"
        elif "dredg" in url_lower or "ospar" in url_lower or "helcom" in url_lower:
            return "dredging"
        elif "rotterdam" in url_lower:
            return "port_rotterdam"
        elif "hamburg" in url_lower or "hafen" in url_lower:
            return "port_hamburg"
        elif "antwerp" in url_lower:
            return "port_antwerp"
        elif "eurostat" in url_lower or "ec.europa.eu" in url_lower:
            return "eu_maritime_statistics"
        elif "unctad" in url_lower or "worldbank" in url_lower:
            return "international_maritime_data"
        elif "salvage" in url_lower or "breaking" in url_lower:
            return "marine_salvage"
        else:
            return "maritime_other"

    def _get_byproduct_types(self, source_type: str) -> list:
        """Get byproduct types for source."""
        types_map = {
            "ship_recycling": ["ship_scrap", "steel_waste", "hazardous_materials", "copper_wire"],
            "dredging": ["dredged_sediments", "contaminated_sediment", "sand", "silt"],
            "port_rotterdam": ["port_waste", "general_cargo_residue", "ballast_water", "dredged_material"],
            "port_hamburg": ["port_waste", "general_cargo_residue", "ballast_water", "dredged_material"],
            "port_antwerp": ["port_waste", "general_cargo_residue", "ballast_water", "dredged_material"],
            "eu_maritime_statistics": ["cargo_handling", "vessel_traffic", "port_activity"],
            "international_maritime_data": ["shipping_data", "port_statistics", "maritime_transport"],
            "marine_salvage": ["ship_scrap", "marine_salvage_materials", "wreck_materials"],
        }
        return types_map.get(source_type, ["dredged_sediments", "ballast_residue", "ship_scrap"])

    def _extract_and_fetch_datasets(self, response: httpx.Response, page_url: str, source_type: str, byproduct_types: list) -> None:
        """Extract links to PDFs, CSVs, and datasets from the page and fetch them."""
        try:
            html_text = response.text
        except:
            return

        # Patterns for downloadable files
        patterns = [
            r'href=(["\']?)([^"\'>\s]+\.pdf)\1',
            r'href=(["\']?)([^"\'>\s]+\.csv)\1',
            r'href=(["\']?)([^"\'>\s]+\.xlsx?)\1',
            r'href=(["\']?)([^"\'>\s]+\.zip)\1',
            r'src=(["\']?)([^"\'>\s]+\.xlsx?)\1',
            r'download["\']?\s*[:=]\s*["\']?([^"\'>\s]+\.(pdf|csv|xlsx?))',
        ]

        found_urls = set()
        for pattern in patterns:
            matches = re.finditer(pattern, html_text, re.IGNORECASE)
            for match in matches:
                file_url = match.group(2) if len(match.groups()) > 1 else match.group(1)
                if file_url:
                    # Resolve relative URLs
                    if file_url.startswith('/'):
                        from urllib.parse import urlparse
                        parsed = urlparse(page_url)
                        file_url = f"{parsed.scheme}://{parsed.netloc}{file_url}"
                    elif not file_url.startswith('http'):
                        file_url = f"{page_url.rstrip('/')}/{file_url}"

                    found_urls.add(file_url)

        # Fetch each dataset found
        for file_url in list(found_urls)[:10]:  # Limit to 10 per page to avoid spam
            self._fetch_dataset(file_url, source_type, byproduct_types)

    def _fetch_dataset(self, file_url: str, source_type: str, byproduct_types: list) -> None:
        """Fetch a single dataset file and save it."""
        try:
            response = self.fetch(file_url)
            if response:
                # Determine file type
                content_type = response.headers.get('content-type', '').lower()
                file_ext = file_url.split('.')[-1].lower()

                if file_ext in ['pdf', 'csv', 'xlsx', 'xls', 'zip']:
                    document_type = file_ext
                elif 'pdf' in content_type:
                    document_type = 'pdf'
                elif 'csv' in content_type or 'spreadsheet' in content_type:
                    document_type = 'csv'
                else:
                    document_type = 'binary'

                # Save the file
                self.save_raw(
                    content=response.content,
                    url=file_url,
                    document_type=document_type,
                    metadata={
                        "source_type": source_type,
                        "parent_url": file_url.split('/')[2],  # Domain
                        "year": 2024,
                        "industry": "maritime",
                        "byproduct_types": byproduct_types,
                        "file_type": file_ext,
                    }
                )
        except Exception as e:
            self.logger.warning(f"Failed to fetch dataset {file_url}: {e}")
