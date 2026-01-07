"""
Symbio Data Engine - Government Data Processor
==============================================
Parses structured CSV data from government sources (EPA TRI, E-PRTR).
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Generator, Optional

# Increase CSV field limit for large government files
csv.field_size_limit(10 * 1024 * 1024)

from .models import WasteListingExtraction, ExtractionResult

logger = logging.getLogger(__name__)

class GovProcessor:
    """Processor for structured government CSV data."""

    def __init__(self):
        # Mapping of source columns to our schema
        self.column_maps = {
            "epa_tri": {
                # Updated 2026: Actual EPA TRI Basic Download headers (with numeric prefixes)
                "company": ["4. FACILITY NAME", "FACILITY NAME", "FACILITY_NAME"],
                "material": ["37. CHEMICAL", "CHEMICAL", "CHEMICAL_NAME"],
                "unit": ["50. UNIT OF MEASURE", "UNIT OF MEASURE", "UNIT"],
                "year": ["1. YEAR", "YEAR", "REPORTING_YEAR"],
                "recycled": ["94. OFF-SITE RECYCLED TOTAL", "115. 8.4 - RECYCLING ON SITE", "116. 8.5 - RECYCLING OFF SIT"],
                "energy": ["97. OFF-SITE ENERGY RECOVERY T", "113. 8.2 - ENERGY RECOVER ON", "114. 8.3 - ENERGY RECOVER OF"],
                "treated": ["104. OFF-SITE TREATED TOTAL", "117. 8.6 - TREATMENT ON SITE", "118. 8.7 - TREATMENT OFF SITE"],
                "released": ["107. TOTAL RELEASES", "65. ON-SITE RELEASE TOTAL", "88. OFF-SITE RELEASE TOTAL"],
            },
            "eprtr": {
                # COMPREHENSIVE EU E-PRTR MAPPING - handles all 16 file types
                "company": ["FacilityName", "Facility Name", "Facility", "facilityName", "EPRTR_SectorName"],
                "material": [
                    # Pollutant columns (Air/Water Releases)
                    "Pollutant", "PollutantName", "Pollutant Name",
                    # Waste columns (Waste Transfers)
                    "wasteClassification", "wasteTreatment"
                ],
                "released": [
                    # Release/Transfer quantity columns
                    "Releases", "releases", "transfers", "Transfers",
                    "wasteTransfers", "TotalQuantity", "quantity", 
                    "TotalRelease", "ReleaseQuantity", "totalWasteQuantity"
                ], 
                "unit": ["UnitCode", "Unit", "UnitOfMeasure"],
                "year": ["ReportingYear", "Year", "ReferenceYear", "reportingYear"],
                "location": ["CountryCode", "CountryName", "Country", "MemberStateName", "NutRegionName", "countryName", "city"]
            },
            "generic": {
                "company": ["Company", "Facility", "Organization", "Entity", "Source", "Generator"],
                "material": ["Material", "Waste Type", "Waste", "Pollutant", "Item", "Description"],
                "released": ["Amount", "Quantity", "Weight", "Volume", "Total", "Tons", "Kg"],
                "unit": ["Unit", "UOM", "Measure"],
                "year": ["Year", "Date", "Period"],
                "location": ["Country", "Emirate", "City", "Location", "Region"]
            }
        }

    def process_csv(self, file_path: Path, source_type: str = "epa_tri") -> List[ExtractionResult]:
        """Parse CSV file and populate ExtractionResult list."""
        file_path = Path(file_path)
        if not file_path.exists():
            return [ExtractionResult.failure("gov", "File not found")]

        results = []
        rows_processed = 0
        
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                # Sniff header
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = None # Fallback to default
                    
                reader = csv.DictReader(f, dialect=dialect)
                
                # Identify column mapping based on headers
                headers = reader.fieldnames or []
                mapping = self._identify_mapping(headers)
                
                if not mapping:
                    return [ExtractionResult.failure("gov", "Unknown CSV Schema: " + ",".join(headers[:5]))]

                for row in reader:
                    wastes = self._extract_row(row, mapping, source_type=source_type)
                    for waste in wastes:
                        results.append(ExtractionResult.success(
                            record_type="waste_listing",
                            record=waste
                        ))
                    
                    rows_processed += 1
                    if rows_processed >= 10000: # Safety limit
                        break
                        
        except Exception as e:
            logger.error(f"Error processing CSV {file_path}: {e}")
            return [ExtractionResult.failure("gov", str(e))]

        return results

    def _identify_mapping(self, headers: List[str]) -> Dict[str, str]:
        """Find which known schema matches the headers."""
        upper_headers = {h.upper(): h for h in headers}
        
        for source, map_def in self.column_maps.items():
            current_map = {}
            matches = 0
            for field, aliases in map_def.items():
                for alias in aliases:
                    if alias.upper() in upper_headers:
                        current_map[field] = upper_headers[alias.upper()]
                        matches += 1
                        break
            
            # If we matched company, material, and quantity, good enough
            if matches >= 3:
                return current_map
        
        return {}

    def _extract_row(self, row: Dict[str, Any], mapping: Dict[str, str], source_type: str = "epa_tri") -> List[WasteListingExtraction]:
        """Convert CSV row to list of WasteListing objects (one per fate)."""
        wastes = []
        try:
            company = row.get(mapping.get("company", ""), "").strip()
            material = row.get(mapping.get("material", ""), "").strip()
            
            # Default unit handling
            default_unit = "lbs"
            if "eprtr" in source_type.lower():
                default_unit = "tonnes" # EU defaults to metric
                
            unit = row.get(mapping.get("unit", ""), default_unit).strip()
            year = row.get(mapping.get("year", ""), "")
            
            if not (company and material):
                return []

            # Define fate categories to check
            fates = [
                ("recycled", "Recycled"),
                ("energy", "Energy Recovery"),
                ("treated", "Treated"),
                ("released", "Disposal/Released")
            ]

            for key, fate_label in fates:
                # Find if this fate exists in the mapping (i.e. header was found)
                if key not in mapping:
                    continue
                
                # Get value
                quantity_str = row.get(mapping[key], "0").strip()
                
                # Parse quantity
                try:
                    msg = quantity_str.replace(",", "")
                    if "<" in msg: msg = msg.replace("<", "").strip()
                    if "." in msg and msg.replace(".", "").isdigit() == False: continue # Skip junk
                    quantity = float(msg)
                except ValueError:
                    continue
                
                if quantity <= 0:
                    continue

                # Convert to Tons
                if unit.lower() in ["pounds", "lbs", "lb"]:
                    quantity_tons = quantity * 0.000453592
                elif unit.lower() in ["kg", "kilograms"]:
                    quantity_tons = quantity * 0.001
                elif unit.lower() in ["grams", "g"]:
                    quantity_tons = quantity * 1e-6
                elif unit.lower() in ["tonnes", "tons", "t"]:
                    quantity_tons = quantity
                else:
                    quantity_tons = quantity

                citation = f"{company} {fate_label} {quantity} {unit} of {material} in {year}"
                
                # Determine location
                loc = "Unknown"
                if "location" in mapping:
                    loc = row.get(mapping["location"], "Unknown")
                elif "USA" in mapping.values() or "epa" in str(mapping):
                    loc = "USA"
                elif "eprtr" in str(mapping).lower():
                    loc = "Europe"

                wastes.append(WasteListingExtraction(
                    material=material,
                    quantity_tons=quantity_tons,
                    treatment_method=fate_label,
                    source_company=company,
                    source_location=loc.strip(),
                    year=int(year) if str(year).isdigit() else 2024,
                    extraction_confidence=1.0,
                    source_quote=citation[:500]
                ))

            return wastes
        except Exception as e:
            return []
