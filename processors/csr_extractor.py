"""
ENHANCED CSR Data Extractor v3 - Proper Category Separation
============================================================
Extracts and properly categorizes:
- FINANCIAL: costs, revenue, savings, investments (USD/EUR)
- ENERGY: consumption, savings (MWh, GJ, kWh)
- EMISSIONS: CO2, GHG, scope 1/2/3 (tonnes, tCO2e)
- WASTE: volumes, recycling (tonnes, kg)
- CARBON_CREDITS: offsets, credits, prices, trading (NEW)
"""
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

from processors.pdf_processor import PDFProcessor

logger = logging.getLogger(__name__)


@dataclass
class WasteData:
    material: str
    quantity_tons: float
    waste_type: str = "unknown"  # recycled, disposed, generated
    year: Optional[int] = None
    source_company: str = ""
    context: str = ""


@dataclass
class EmissionData:
    emission_type: str  # CO2, GHG, CH4, NOx
    value: float
    unit: str  # tonnes, tCO2e, kt, Mt
    year: Optional[int] = None
    scope: str = ""  # Scope 1, 2, 3
    source_company: str = ""
    context: str = ""


@dataclass
class FinancialData:
    category: str  # disposal_cost, recycling_revenue, savings, investment
    value: float
    currency: str = "USD"
    year: Optional[int] = None
    source_company: str = ""
    context: str = ""


@dataclass
class EnergyData:
    """Energy consumption/savings data - separate from financial."""
    category: str  # consumption, savings, renewable, reduction
    value: float
    unit: str  # MWh, GWh, GJ, TJ, kWh
    year: Optional[int] = None
    source_company: str = ""
    context: str = ""


@dataclass
class CarbonCreditData:
    """Carbon credits, offsets, and trading data."""
    category: str  # credits_purchased, offsets, carbon_spend, carbon_price, trading_revenue
    value: float
    unit: str  # credits, tonnes, USD, EUR, USD/tonne
    year: Optional[int] = None
    source_company: str = ""
    context: str = ""


class CSRExtractor:
    """Enhanced CSR data extractor with proper category separation."""
    
    # Units that should NOT trigger financial multipliers
    ENERGY_UNITS = ['mwh', 'gwh', 'twh', 'kwh', 'gj', 'tj', 'mj', 'btu']
    MASS_UNITS = ['mt', 'tonnes', 'tons', 'kg', 'kilotons', 'kt']
    
    def __init__(self):
        self.pdf_processor = PDFProcessor()
        
        # WASTE patterns
        self.waste_patterns = [
            r"(recycled?|disposed?|generated?|diverted?|recovered?)\s+([\d,]+(?:\.\d+)?)\s*(tonnes?|tons?|mt|kg)\s+(?:of\s+)?([a-zA-Z\s]+)",
            r"([a-zA-Z]+\s*waste)\s*[:=]\s*([\d,]+(?:\.\d+)?)\s*(tonnes?|tons?|mt)",
            r"([\d,]+(?:\.\d+)?)\s*(tonnes?|tons?|mt)\s+(?:of\s+)?([a-zA-Z\s]+)\s+(recycled?|disposed?|generated?)",
            r"total\s+waste\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(tonnes?|tons?|mt)",
            r"waste\s+(?:to\s+)?landfill\s*[\|:]\s*([\d,]+(?:\.\d+)?)",
            r"hazardous\s+waste\s*[\|:]\s*([\d,]+(?:\.\d+)?)",
            r"(plastic|paper|metal|glass|organic)\s+waste\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(tonnes?|tons?)?",
        ]
        
        # EMISSIONS patterns (CO2, GHG, scope)
        self.emission_patterns = [
            r"(CO2|CO₂|carbon\s*dioxide|GHG|greenhouse\s*gas)\s*(?:emissions?)?\s*[:=]?\s*([\d,.]+)\s*(million)?\s*(tonnes?|tons?|mt|tCO2e?)",
            r"(Scope\s*[123])\s*(?:emissions?)?\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(tonnes?|tCO2e?|mt|kt)",
            r"reduced?\s+(?:emissions?|CO2|carbon)\s+(?:by\s+)?([\d,.]+)\s*(%|percent|tonnes?)",
            r"carbon\s+footprint\s*(?:of|:)?\s*([\d,.]+)\s*(million)?\s*(tonnes?|tons?|tCO2)?",
            r"(methane|CH4|NOx|SOx)\s*(?:emissions?)?\s*[:=]?\s*([\d,.]+)\s*(tonnes?|mt|kg)",
        ]
        
        # ENERGY patterns (MWh, GJ - NOT financial)
        self.energy_patterns = [
            r"(?:energy|electricity)\s+(?:consumption|usage)\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(MWh|GWh|TWh|GJ|TJ)",
            r"(?:saved?|reduced?)\s+([\d,]+(?:\.\d+)?)\s*(MWh|GWh|kWh|GJ)\s+(?:of\s+)?(?:energy|electricity)?",
            r"([\d,]+(?:\.\d+)?)\s*(MWh|GWh|TWh|GJ|TJ)\s+(?:of\s+)?(?:renewable|clean|green)\s+energy",
            r"renewable\s+energy\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(%|percent|MWh|GWh)?",
            r"solar\s+(?:capacity|generation)\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(MW|GW|MWh)?",
        ]
        
        # FINANCIAL patterns (MUST have $ or currency, NOT energy units)
        self.financial_patterns = [
            # Explicit currency markers
            r"\$\s*([\d,.]+)\s*(million|billion|M|B)?\s+(?:in\s+)?(?:savings?|saved)",
            r"\$\s*([\d,.]+)\s*(million|billion|M|B)?\s+(?:in\s+)?(?:revenue|income)",
            r"\$\s*([\d,.]+)\s*(million|billion|M|B)?\s+(?:in\s+)?(?:investment|invested)",
            r"\$\s*([\d,.]+)\s*(million|billion|M|B)?\s+(?:in\s+)?(?:cost|expense)",
            r"(?:cost|expense)\s+(?:savings?|reduction)\s*[:=]?\s*\$\s*([\d,.]+)\s*(million|M)?",
            r"(?:recycling|environmental)\s+revenue\s*[:=]?\s*\$?\s*([\d,.]+)\s*(million|M)?",
            r"(?:USD|EUR|€)\s*([\d,.]+)\s*(million|billion|M|B)?",
            # Per-unit costs (always financial)
            r"\$\s*([\d,.]+)\s*(?:per|/)\s*(?:ton|tonne|mt)",
        ]
        
        # CARBON CREDIT patterns
        self.carbon_credit_patterns = [
            # Credits purchased
            r"(?:purchased?|bought|acquired?)\s+([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:carbon)?\s*(?:credits?|offsets?|allowances?)",
            r"([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:carbon)?\s*(?:credits?|offsets?)\s+(?:purchased?|bought|acquired?)",
            # Offsets in tonnes
            r"offset\s+([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:tonnes?|tons?|tCO2e?)\s+(?:of\s+)?(?:CO2|carbon|emissions?)?",
            r"([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:tonnes?|tCO2e?)\s+(?:of\s+)?(?:carbon|CO2)\s+offset",
            # Carbon spend
            r"\$\s*([\d,.]+)\s*(million|billion|M|B)?\s+(?:on|for|spent\s+on)?\s*(?:carbon)?\s*(?:credits?|offsets?)",
            r"(?:carbon|offset)\s+(?:spend|spending|expenditure|cost)\s*[:=]?\s*\$?\s*([\d,.]+)\s*(million|M)?",
            r"(?:invested?|spent?)\s+\$?\s*([\d,.]+)\s*(million|M)?\s+(?:in|on)\s+(?:carbon)?\s*(?:credits?|offsets?)",
            # Carbon price
            r"(?:carbon|CO2)\s+price\s*(?:of|[:=])?\s*(?:\$|€|EUR|USD)?\s*([\d,.]+)\s*(?:per|/)\s*(?:tonne?|ton|tCO2e?)",
            r"(?:\$|€)\s*([\d,.]+)\s*(?:per|/)\s*(?:tonne?|ton)?\s*(?:of\s+)?(?:carbon|CO2)",
            # ETS/EUA allowances
            r"(?:EUA|ETS|EU\s*allowances?)\s*[:=]?\s*([\d,]+(?:\.\d+)?)\s*(?:million)?",
            r"(?:retired?|surrendered?)\s+([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:EUA|allowances?)",
            # Trading revenue
            r"(?:carbon|emissions?)\s+trading\s+(?:revenue|income)\s*[:=]?\s*\$?\s*([\d,.]+)\s*(million|M)?",
            r"(?:sold|selling)\s+([\d,]+(?:\.\d+)?)\s*(?:million)?\s*(?:carbon)?\s*(?:credits?|allowances?)",
            # VCS/Gold Standard/Verra
            r"(?:VCS|Verra|Gold\s*Standard)\s+(?:credits?|offsets?)\s*[:=]?\s*([\d,]+(?:\.\d+)?)",
            # Net zero/carbon neutral claims with numbers
            r"(?:carbon\s*neutral|net\s*zero)\s+(?:by|target|goal)\s+(20\d{2})",
        ]
    
    def extract_from_pdf(self, pdf_path: str | Path, company: str = "unknown") -> dict:
        """Extract all data categories from a CSR PDF."""
        pdf_path = Path(pdf_path)
        
        empty_result = {"waste_data": [], "emissions": [], "financials": [], "energy": [], "carbon_credits": []}
        
        if not pdf_path.exists():
            return empty_result
        
        text = self.pdf_processor.extract_text(pdf_path)
        
        if not text:
            return empty_result
        
        year = self._extract_year(pdf_path.name, text)
        
        waste_data = self._extract_waste(text, company, year)
        emissions = self._extract_emissions(text, company, year)
        energy = self._extract_energy(text, company, year)
        financials = self._extract_financials(text, company, year)
        carbon_credits = self._extract_carbon_credits(text, company, year)
        
        return {
            "waste_data": waste_data,
            "emissions": emissions,
            "financials": financials,
            "energy": energy,
            "carbon_credits": carbon_credits,
            "text_length": len(text),
            "year": year,
        }
    
    def _extract_waste(self, text: str, company: str, year: int) -> List[WasteData]:
        """Extract waste data."""
        results = []
        seen = set()
        
        for pattern in self.waste_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = match.groups()
                    
                    quantity = 0
                    material = "waste"
                    waste_type = "unknown"
                    
                    for i, g in enumerate(groups):
                        if g and re.match(r'^[\d,]+(?:\.\d+)?$', str(g)):
                            quantity = self._parse_number(g)
                            break
                    
                    for g in groups:
                        if not g:
                            continue
                        gl = g.lower()
                        if gl in ['recycled', 'disposed', 'generated', 'diverted', 'recovered']:
                            waste_type = gl
                        elif gl not in ['tonnes', 'tons', 'mt', 'kg'] and not re.match(r'^[\d,\.]+$', g):
                            material = g.strip()[:50]
                    
                    if quantity > 0:
                        key = f"{material}_{quantity}_{waste_type}"
                        if key not in seen:
                            seen.add(key)
                            results.append(WasteData(
                                material=material,
                                quantity_tons=quantity,
                                waste_type=waste_type,
                                year=year,
                                source_company=company,
                                context=match.group(0)[:100],
                            ))
            except Exception:
                continue
        
        return results
    
    def _extract_emissions(self, text: str, company: str, year: int) -> List[EmissionData]:
        """Extract emission data (CO2, GHG, scope)."""
        results = []
        seen = set()
        
        for pattern in self.emission_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = match.groups()
                    
                    value = 0
                    emission_type = "CO2"
                    unit = "tonnes"
                    scope = ""
                    
                    for i, g in enumerate(groups):
                        if g and re.match(r'^[\d,]+(?:\.\d+)?$', str(g)):
                            value = self._parse_number(g)
                            # Check for million multiplier
                            for j in range(max(0, i-1), min(len(groups), i+2)):
                                mult = str(groups[j]).lower() if groups[j] else ""
                                if mult == 'million':
                                    value *= 1_000_000
                            break
                    
                    for g in groups:
                        if not g:
                            continue
                        gl = g.lower()
                        if 'scope' in gl:
                            scope = g
                        elif gl in ['co2', 'co₂', 'carbon', 'ghg', 'methane', 'ch4', 'nox', 'sox']:
                            emission_type = g.upper()
                        elif gl in ['tonnes', 'tons', 'mt', 'tco2e', 'tco2', 'kt', '%']:
                            unit = g
                    
                    if value > 0:
                        key = f"{emission_type}_{value}_{scope}"
                        if key not in seen:
                            seen.add(key)
                            results.append(EmissionData(
                                emission_type=emission_type,
                                value=value,
                                unit=unit,
                                scope=scope,
                                year=year,
                                source_company=company,
                                context=match.group(0)[:100],
                            ))
            except Exception:
                continue
        
        return results
    
    def _extract_energy(self, text: str, company: str, year: int) -> List[EnergyData]:
        """Extract energy data (MWh, GJ) - separate from financial."""
        results = []
        seen = set()
        
        for pattern in self.energy_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = match.groups()
                    full_match = match.group(0).lower()
                    
                    value = 0
                    unit = "MWh"
                    
                    for g in groups:
                        if g and re.match(r'^[\d,]+(?:\.\d+)?$', str(g)):
                            value = self._parse_number(g)
                        elif g and g.lower() in ['mwh', 'gwh', 'twh', 'kwh', 'gj', 'tj', 'mw', 'gw']:
                            unit = g.upper()
                    
                    # Determine category
                    if 'saved' in full_match or 'reduced' in full_match or 'reduction' in full_match:
                        category = "savings"
                    elif 'renewable' in full_match or 'solar' in full_match or 'wind' in full_match:
                        category = "renewable"
                    elif 'consumption' in full_match or 'usage' in full_match:
                        category = "consumption"
                    else:
                        category = "other"
                    
                    if value > 0:
                        key = f"{category}_{value}_{unit}"
                        if key not in seen:
                            seen.add(key)
                            results.append(EnergyData(
                                category=category,
                                value=value,
                                unit=unit,
                                year=year,
                                source_company=company,
                                context=match.group(0)[:100],
                            ))
            except Exception:
                continue
        
        return results
    
    def _extract_financials(self, text: str, company: str, year: int) -> List[FinancialData]:
        """Extract financial data ONLY - excludes energy/mass units."""
        results = []
        seen = set()
        
        for pattern in self.financial_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = match.groups()
                    full_match = match.group(0).lower()
                    
                    # CRITICAL: Skip if this contains energy/mass units
                    if any(unit in full_match for unit in self.ENERGY_UNITS + self.MASS_UNITS):
                        continue
                    
                    value = 0
                    currency = "USD"
                    
                    for g in groups:
                        if g and re.match(r'^[\d,]+(?:\.\d+)?$', str(g)):
                            value = self._parse_number(g)
                        elif g and g.lower() in ['million', 'm']:
                            value *= 1_000_000
                        elif g and g.lower() in ['billion', 'b']:
                            value *= 1_000_000_000
                    
                    # Currency detection
                    if 'eur' in full_match or '€' in full_match:
                        currency = "EUR"
                    
                    # Category detection
                    if 'cost' in full_match or 'expense' in full_match:
                        category = "disposal_cost"
                    elif 'revenue' in full_match or 'income' in full_match:
                        category = "recycling_revenue"
                    elif 'saving' in full_match or 'saved' in full_match:
                        category = "savings"
                    elif 'invest' in full_match:
                        category = "investment"
                    else:
                        category = "environmental_spend"
                    
                    # Sanity check: reasonable financial bounds ($1K - $100B)
                    if 1000 <= value <= 100_000_000_000:
                        key = f"{category}_{value}"
                        if key not in seen:
                            seen.add(key)
                            results.append(FinancialData(
                                category=category,
                                value=value,
                                currency=currency,
                                year=year,
                                source_company=company,
                                context=match.group(0)[:100],
                            ))
            except Exception:
                continue
        
        return results
    
    def _extract_carbon_credits(self, text: str, company: str, year: int) -> List[CarbonCreditData]:
        """Extract carbon credit, offset, and trading data."""
        results = []
        seen = set()
        
        for pattern in self.carbon_credit_patterns:
            try:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = match.groups()
                    full_match = match.group(0).lower()
                    
                    value = 0
                    unit = "credits"
                    
                    # Parse value
                    for g in groups:
                        if g and re.match(r'^[\d,]+(?:\.\d+)?$', str(g)):
                            value = self._parse_number(g)
                        elif g and g.lower() == 'million':
                            value *= 1_000_000
                        elif g and g.lower() in ['billion', 'b']:
                            value *= 1_000_000_000
                        elif g and g.lower() in ['m']:
                            # Be careful - M could be million or part of a unit
                            if 'mwh' not in full_match and 'mt' not in full_match:
                                value *= 1_000_000
                    
                    # Determine category and unit
                    if 'price' in full_match or 'per' in full_match:
                        category = "carbon_price"
                        unit = "USD/tonne"
                        if '€' in full_match or 'eur' in full_match:
                            unit = "EUR/tonne"
                    elif 'purchased' in full_match or 'bought' in full_match or 'acquired' in full_match:
                        category = "credits_purchased"
                        unit = "credits"
                    elif 'offset' in full_match:
                        if 'tonne' in full_match or 'tco2' in full_match:
                            category = "offsets"
                            unit = "tCO2e"
                        else:
                            category = "offsets"
                            unit = "credits"
                    elif 'spend' in full_match or 'cost' in full_match or 'spent' in full_match or 'invested' in full_match:
                        category = "carbon_spend"
                        unit = "USD"
                        if '€' in full_match or 'eur' in full_match:
                            unit = "EUR"
                    elif 'revenue' in full_match or 'income' in full_match or 'sold' in full_match or 'selling' in full_match:
                        category = "trading_revenue"
                        unit = "USD"
                    elif 'eua' in full_match or 'ets' in full_match or 'allowance' in full_match:
                        category = "eu_allowances"
                        unit = "EUA"
                    elif 'vcs' in full_match or 'verra' in full_match or 'gold standard' in full_match:
                        category = "voluntary_credits"
                        unit = "credits"
                    elif 'net zero' in full_match or 'carbon neutral' in full_match:
                        category = "net_zero_target"
                        unit = "year"
                    else:
                        category = "carbon_credits"
                        unit = "credits"
                    
                    if value > 0:
                        key = f"{category}_{value}_{unit}"
                        if key not in seen:
                            seen.add(key)
                            results.append(CarbonCreditData(
                                category=category,
                                value=value,
                                unit=unit,
                                year=year,
                                source_company=company,
                                context=match.group(0)[:100],
                            ))
            except Exception:
                continue
        
        return results
    
    def _parse_number(self, s: str) -> float:
        if not s:
            return 0.0
        clean = str(s).replace(",", "").strip()
        try:
            return float(clean)
        except ValueError:
            return 0.0
    
    def _extract_year(self, filename: str, text: str) -> Optional[int]:
        year_match = re.search(r"20[0-2]\d", filename)
        if year_match:
            return int(year_match.group())
        year_matches = re.findall(r"(20[0-2]\d)", text[:2000])
        if year_matches:
            return int(year_matches[-1])
        return None


def extract_csr_data(pdf_path: str | Path, company: str = "unknown") -> dict:
    extractor = CSRExtractor()
    return extractor.extract_from_pdf(pdf_path, company)
