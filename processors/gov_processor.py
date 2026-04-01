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
                # FIXED: Prioritize specific pollutant names over generic waste codes
                "company": ["FacilityName", "facilityName", "Facility Name", "Facility", "EPRTR_SectorName"],
                "material": [
                    # PRIORITY 1: Specific pollutant names (from Air/Water releases)
                    "PollutantName", "Pollutant", "pollutantName", "Pollutant Name",
                    # PRIORITY 2: Waste description (if specific)
                    "wasteType", "WasteTypeName", "wasteName",
                    # NOTE: wasteClassification REMOVED - only gives hw/nonhw
                ],
                "released": [
                    # Release/Transfer quantity columns
                    "Releases", "releases", "transfers", "Transfers",
                    "wasteTransfers", "TotalQuantity", "quantity", 
                    "TotalRelease", "ReleaseQuantity", "totalWasteQuantity", "TotalWasteQuantity"
                ], 
                "unit": ["UnitCode", "Unit", "UnitOfMeasure", "unitCode"],
                "year": ["ReportingYear", "Year", "ReferenceYear", "reportingYear"],
                "location": ["CountryCode", "CountryName", "Country", "MemberStateName", "NutRegionName", "countryName", "facilityCity", "city"],
                "country": ["countryName", "CountryName", "Country"],
                "industry": ["EPRTR_SectorName", "sectorName", "activityName", "EPRTRSectorCode"]
            },
            "generic": {
                "company": ["Company", "Facility", "Organization", "Entity", "Source", "Generator"],
                "material": ["Material", "Waste Type", "Waste", "Pollutant", "Item", "Description"],
                "released": ["Amount", "Quantity", "Weight", "Volume", "Total", "Tons", "Kg"],
                "unit": ["Unit", "UOM", "Measure"],
                "year": ["Year", "Date", "Period"],
                "location": ["Country", "Emirate", "City", "Location", "Region"]
            },
            "worldsteel": {
                "company": ["Company", "Steel Producer", "Mill", "Facility"],
                "material": ["Waste Type", "By-product", "Material", "Slag Type", "Scale"],
                "quantity": ["Quantity", "Amount", "Volume", "Tonnes", "Tons"],
                "unit": ["Unit", "Measure", "UOM"],
                "year": ["Year", "ReportingYear"],
                "location": ["Country", "Region", "Location"]
            },
            "fao": {
                "company": ["Country", "Region", "Producer"],
                "material": ["Crop Residue", "Waste Type", "Biomass", "Agricultural Waste"],
                "quantity": ["Quantity", "Amount", "Production"],
                "unit": ["Unit", "Measure", "Tonnes"],
                "year": ["Year", "Period"],
                "location": ["Country", "Region"]
            },
            "plastics": {
                "company": ["Producer", "Company", "Manufacturer"],
                "material": ["Plastic Type", "Polymer", "Waste Stream", "Material"],
                "quantity": ["Quantity", "Amount", "Tonnes", "Tons"],
                "unit": ["Unit", "Measure"],
                "year": ["Year", "ReportingYear"],
                "location": ["Country", "Region"]
            },
            "petcoke": {
                "company": ["Refinery", "Producer", "Company"],
                "material": ["Petcoke Grade", "Fuel Type", "Carbon Content"],
                "quantity": ["Production", "Amount", "Tonnes"],
                "unit": ["Unit", "Measure"],
                "year": ["Year"],
                "location": ["Country", "Region"]
            },
            "maritime": {
                "company": ["Shipping Line", "Port Authority", "Vessel Owner"],
                "material": ["Bunker Fuel", "Waste Type", "Spillage"],
                "quantity": ["Quantity", "Volume", "Tonnes"],
                "unit": ["Unit", "Measure"],
                "year": ["Year"],
                "location": ["Port", "Country", "Region"]
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
            # Handle potential NaN/None from pandas gracefully
            raw_company = row.get(mapping.get("company", ""), "")
            company = str(raw_company).strip() if raw_company and str(raw_company).lower() != 'nan' else ""
            
            raw_material = row.get(mapping.get("material", ""), "")
            material = str(raw_material).strip() if raw_material and str(raw_material).lower() != 'nan' else ""
            
            # Default unit handling
            default_unit = "lbs"
            if "eprtr" in source_type.lower():
                default_unit = "tonnes" # EU defaults to metric
                
            raw_unit = row.get(mapping.get("unit", ""), default_unit)
            unit = str(raw_unit).strip() if raw_unit and str(raw_unit).lower() != 'nan' else default_unit
            
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
                loc = row.get(mapping.get("location", ""), "Unknown").strip()
                industry = row.get(mapping.get("industry", ""), "Industrial").strip()
                country = row.get(mapping.get("country", ""), "Unknown").strip()

                # Determine verification_method based on source type (DQ-001 REQUIRED)
                if "eprtr" in source_type.lower():
                    verification_method = "E-PRTR Official (v15)"
                elif "epa" in source_type.lower() or "tri" in source_type.lower():
                    verification_method = "EPA TRI Official"
                else:
                    verification_method = "Government Portal"

                wastes.append(WasteListingExtraction(
                    material=material,
                    quantity_tons=quantity_tons,
                    treatment_method=fate_label,
                    source_company=company,
                    source_location=loc,
                    source_industry=industry,
                    source_country=country,
                    year=int(year) if str(year).isdigit() else 2024,
                    extraction_confidence=1.0,
                    source_quote=citation[:500],
                    verification_method=verification_method  # DQ-001: REQUIRED
                ))


            return wastes
        except Exception as e:
            return []

    def process_csv_industrial(self, file_path: Path, source_type: str = "worldsteel") -> List[ExtractionResult]:
        """Parse industrial CSV file (worldsteel, fao, plastics, petcoke, maritime) and populate ExtractionResult list."""
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
                    dialect = None  # Fallback to default

                reader = csv.DictReader(f, dialect=dialect)

                # Identify column mapping based on headers
                headers = reader.fieldnames or []
                mapping = self._identify_mapping(headers)

                if not mapping:
                    return [ExtractionResult.failure("gov", "Unknown CSV Schema: " + ",".join(headers[:5]))]

                for row in reader:
                    wastes = self._extract_row_industrial(row, mapping, source_type=source_type)
                    for waste in wastes:
                        results.append(ExtractionResult.success(
                            record_type="waste_listing",
                            record=waste
                        ))

                    rows_processed += 1
                    if rows_processed >= 10000:  # Safety limit
                        break

        except Exception as e:
            logger.error(f"Error processing industrial CSV {file_path}: {e}")
            return [ExtractionResult.failure("gov", str(e))]

        return results

    def _extract_row_industrial(self, row: Dict[str, Any], mapping: Dict[str, str], source_type: str = "worldsteel") -> List[WasteListingExtraction]:
        """Convert industrial CSV row to list of WasteListing objects."""
        wastes = []
        try:
            # Handle potential NaN/None from pandas gracefully
            raw_company = row.get(mapping.get("company", ""), "")
            company = str(raw_company).strip() if raw_company and str(raw_company).lower() != 'nan' else ""

            raw_material = row.get(mapping.get("material", ""), "")
            material = str(raw_material).strip() if raw_material and str(raw_material).lower() != 'nan' else ""

            # Default unit handling
            default_unit = "tonnes"
            raw_unit = row.get(mapping.get("unit", ""), default_unit)
            unit = str(raw_unit).strip() if raw_unit and str(raw_unit).lower() != 'nan' else default_unit

            year = row.get(mapping.get("year", ""), "")

            if not (company and material):
                return []

            # Get quantity
            quantity_str = row.get(mapping.get("quantity", ""), "0").strip()

            # Parse quantity
            try:
                msg = quantity_str.replace(",", "")
                if "<" in msg: msg = msg.replace("<", "").strip()
                if "." in msg and msg.replace(".", "").isdigit() == False: return []  # Skip junk
                quantity = float(msg)
            except ValueError:
                return []

            if quantity <= 0:
                return []

            # Convert to Tons
            if unit.lower() in ["pounds", "lbs", "lb"]:
                quantity_tons = quantity * 0.000453592
            elif unit.lower() in ["kg", "kilograms"]:
                quantity_tons = quantity * 0.001
            elif unit.lower() in ["grams", "g"]:
                quantity_tons = quantity * 1e-6
            elif unit.lower() in ["tonnes", "tons", "t", "mt"]:
                quantity_tons = quantity
            else:
                quantity_tons = quantity

            citation = f"{company} produced/managed {quantity} {unit} of {material} in {year}"

            # Determine location
            loc = row.get(mapping.get("location", ""), "Unknown").strip()
            country = row.get(mapping.get("location", ""), "Unknown").strip()

            # Determine verification_method based on source type (DQ-001 REQUIRED)
            verification_methods = {
                "worldsteel": "World Steel Association Official",
                "fao": "FAO Statistics Official",
                "plastics": "Plastics Industry Report",
                "petcoke": "Energy Institute Official",
                "maritime": "IMO Shipping Registry"
            }
            verification_method = verification_methods.get(source_type, "Industrial Registry")

            wastes.append(WasteListingExtraction(
                material=material,
                quantity_tons=quantity_tons,
                treatment_method="Industrial By-Product",
                source_company=company,
                source_location=loc,
                source_industry=source_type.capitalize(),
                source_country=country,
                year=int(year) if str(year).isdigit() else 2024,
                extraction_confidence=1.0,
                source_quote=citation[:500],
                verification_method=verification_method  # DQ-001: REQUIRED
            ))

            return wastes
        except Exception as e:
            return []
