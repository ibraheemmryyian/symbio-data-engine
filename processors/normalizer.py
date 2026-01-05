"""
Symbio Data Engine - Data Normalizer
====================================
Second stage of the processing pipeline.

Operations:
1. Unit conversion (tons, kg, lbs → metric tons)
2. Entity resolution (fuzzy company name matching)
3. Date parsing and normalization
4. Currency conversion
5. Material category mapping
"""

import logging
import re
from datetime import datetime
from typing import Optional

from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


# Unit conversion factors to metric tons
UNIT_CONVERSIONS = {
    # Mass units
    "metric_ton": 1.0,
    "metric_tons": 1.0,
    "tonne": 1.0,
    "tonnes": 1.0,
    "t": 1.0,
    "mt": 1.0,
    "ton": 0.907185,  # US short ton
    "tons": 0.907185,
    "short_ton": 0.907185,
    "long_ton": 1.01605,
    "kg": 0.001,
    "kilogram": 0.001,
    "kilograms": 0.001,
    "lb": 0.000453592,
    "lbs": 0.000453592,
    "pound": 0.000453592,
    "pounds": 0.000453592,
    "cwt": 0.0453592,  # Hundredweight (US)
}

# Material category mapping
MATERIAL_CATEGORIES = {
    "metals": [
        "steel", "iron", "copper", "aluminum", "aluminium", "brass",
        "zinc", "nickel", "lead", "tin", "scrap metal", "ferrous",
        "non-ferrous", "alloy", "stainless",
    ],
    "plastics": [
        "plastic", "polymer", "pet", "hdpe", "ldpe", "pvc", "pp",
        "polypropylene", "polyethylene", "polystyrene", "abs",
    ],
    "organics": [
        "organic", "biomass", "food waste", "compost", "agricultural",
        "wood", "paper", "cardboard", "cellulose",
    ],
    "chemicals": [
        "chemical", "solvent", "acid", "alkali", "caustic",
        "sulfur", "sulphur", "chlorine", "ammonia",
    ],
    "energy": [
        "steam", "heat", "energy", "power", "electricity", "gas",
        "fuel", "oil", "coal",
    ],
    "water": [
        "water", "wastewater", "effluent", "sewage", "coolant",
    ],
    "construction": [
        "concrete", "cement", "aggregate", "sand", "gravel",
        "gypsum", "slag", "fly ash", "ash",
    ],
}


class Normalizer:
    """
    Data normalization processor.
    
    Standardizes units, resolves entities, and maps categories.
    """
    
    def __init__(self, company_list: list[str] = None):
        """
        Initialize normalizer.
        
        Args:
            company_list: List of known company names for matching
        """
        self.known_companies = company_list or []
    
    def normalize(self, text: str) -> str:
        """
        Apply all normalizations to text.
        
        Finds quantity-unit pairs and normalizes them to metric tons.
        This modifies the text in-place for downstream processing.
        
        Returns:
            Text with normalized values
        """
        if not text:
            return text
        
        # Find and normalize quantities
        quantities = self.extract_quantities(text)
        
        for q in reversed(quantities):  # Reverse to preserve positions
            original = f"{q['original_value']:,.2f} {q['original_unit']}"
            normalized = f"{q['metric_tons']:,.4f} metric_tons"
            text = text.replace(original, normalized, 1)
        
        return text
    
    def normalize_quantity(
        self,
        value: float,
        unit: str,
        target_unit: str = "metric_ton",
    ) -> tuple[float, str]:
        """
        Convert quantity to target unit.
        
        Args:
            value: Numeric value
            unit: Current unit
            target_unit: Target unit (default: metric_ton)
        
        Returns:
            Tuple of (converted_value, target_unit)
        """
        unit_lower = unit.lower().strip().replace(" ", "_")
        
        if unit_lower not in UNIT_CONVERSIONS:
            logger.warning(f"Unknown unit: {unit}")
            return value, unit
        
        # Convert to metric tons first
        mt_value = value * UNIT_CONVERSIONS[unit_lower]
        
        if target_unit == "metric_ton":
            return round(mt_value, 4), "metric_ton"
        
        # Convert from metric tons to target
        if target_unit in UNIT_CONVERSIONS:
            factor = UNIT_CONVERSIONS[target_unit]
            return round(mt_value / factor, 4), target_unit
        
        return round(mt_value, 4), "metric_ton"
    
    def categorize_material(self, material: str) -> Optional[str]:
        """
        Map material name to category.
        
        Args:
            material: Material name
        
        Returns:
            Category name or None
        """
        material_lower = material.lower()
        
        for category, keywords in MATERIAL_CATEGORIES.items():
            for keyword in keywords:
                if keyword in material_lower:
                    return category
        
        return None
    
    def resolve_company(
        self,
        name: str,
        threshold: int = 85,
    ) -> tuple[str, float]:
        """
        Match company name to known companies using fuzzy matching.
        
        Args:
            name: Company name to resolve
            threshold: Minimum match score (0-100)
        
        Returns:
            Tuple of (matched_name, confidence_score)
        """
        if not self.known_companies:
            return name, 1.0
        
        # Use rapidfuzz for matching
        result = process.extractOne(
            name,
            self.known_companies,
            scorer=fuzz.token_sort_ratio,
        )
        
        if result and result[1] >= threshold:
            return result[0], result[1] / 100.0
        
        return name, 0.0
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse various date formats.
        
        Args:
            date_str: Date string to parse
        
        Returns:
            datetime object or None
        """
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%B %d, %Y",
            "%d %B %Y",
            "%Y",
        ]
        
        date_str = date_str.strip()
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try to extract year
        year_match = re.search(r"(19|20)\d{2}", date_str)
        if year_match:
            try:
                return datetime(int(year_match.group()), 1, 1)
            except ValueError:
                pass
        
        return None
    
    def extract_quantities(self, text: str) -> list[dict]:
        """
        Extract quantity-unit pairs from text.
        
        Args:
            text: Text to search
        
        Returns:
            List of dicts with value, unit, metric_tons
        """
        # Pattern: number followed by unit
        pattern = r"([\d,]+\.?\d*)\s*(metric\s*tons?|tonnes?|tons?|kg|kilograms?|lbs?|pounds?|mt|t)\b"
        
        quantities = []
        
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value_str = match.group(1).replace(",", "")
            unit = match.group(2).lower()
            
            try:
                value = float(value_str)
                mt_value, _ = self.normalize_quantity(value, unit)
                
                quantities.append({
                    "original_value": value,
                    "original_unit": unit,
                    "metric_tons": mt_value,
                })
            except ValueError:
                continue
        
        return quantities


# Convenience functions
def normalize_units(value: float, unit: str) -> float:
    """Convert value to metric tons."""
    normalizer = Normalizer()
    result, _ = normalizer.normalize_quantity(value, unit)
    return result


def resolve_company(name: str, known_companies: list[str]) -> str:
    """Resolve company name to canonical form."""
    normalizer = Normalizer(company_list=known_companies)
    resolved, _ = normalizer.resolve_company(name)
    return resolved


if __name__ == "__main__":
    # Quick test
    normalizer = Normalizer()
    
    # Test unit conversion
    print("Unit conversions:")
    for value, unit in [(100, "kg"), (1, "ton"), (1000, "lbs"), (5, "tonnes")]:
        result, target = normalizer.normalize_quantity(value, unit)
        print(f"  {value} {unit} = {result} {target}")
    
    # Test material categorization
    print("\nMaterial categories:")
    for material in ["Copper scrap", "HDPE plastic", "Fly ash", "Steam"]:
        category = normalizer.categorize_material(material)
        print(f"  {material} → {category}")
    
    # Test quantity extraction
    text = "The facility processed 5,000 tonnes of steel and 250 kg of copper."
    quantities = normalizer.extract_quantities(text)
    print(f"\nExtracted quantities from: '{text}'")
    for q in quantities:
        print(f"  {q}")
