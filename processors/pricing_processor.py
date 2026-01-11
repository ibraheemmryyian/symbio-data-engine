"""
Symbio Data Engine - Pricing Processor
=======================================
Store and aggregate pricing data from spiders.
"""

import logging
from datetime import datetime
from typing import Optional

from store.postgres import execute_query, get_pool

logger = logging.getLogger(__name__)


def insert_raw_price(
    material_name: str,
    price_value: float,
    price_unit: str,
    source: str,
    source_url: str = None,
    currency: str = "USD",
    region: str = "us",
    price_date: Optional[str] = None,
) -> Optional[int]:
    """Insert a raw price into material_prices_raw table."""
    try:
        result = execute_query(
            """
            INSERT INTO material_prices_raw 
                (material_name, material_category, price_value, price_unit, 
                 currency, source, source_url, region, price_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                material_name.lower().strip(),
                _categorize_material(material_name),
                price_value,
                price_unit,
                currency,
                source,
                source_url,
                region,
                price_date,
            ),
        )
        return result[0]["id"] if result else None
    except Exception as e:
        logger.error(f"Failed to insert raw price: {e}")
        return None


def upsert_valuation(
    material_type_id: str,
    material_name: str,
    price_per_ton_usd: float,
    price_per_lb_usd: float = None,
    source_count: int = 1,
    confidence_score: float = 0.5,
    material_category: str = None,
) -> Optional[int]:
    """Upsert aggregated valuation into material_valuations table."""
    try:
        result = execute_query(
            """
            INSERT INTO material_valuations 
                (material_type_id, material_name, material_category,
                 price_per_ton_usd, price_per_lb_usd, source_count, 
                 confidence_score, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (material_type_id) 
            DO UPDATE SET
                price_per_ton_usd = EXCLUDED.price_per_ton_usd,
                price_per_lb_usd = EXCLUDED.price_per_lb_usd,
                source_count = EXCLUDED.source_count,
                confidence_score = EXCLUDED.confidence_score,
                last_updated = NOW()
            RETURNING id
            """,
            (
                material_type_id,
                material_name.lower().strip(),
                material_category or _categorize_material(material_name),
                price_per_ton_usd,
                price_per_lb_usd,
                source_count,
                confidence_score,
            ),
        )
        return result[0]["id"] if result else None
    except Exception as e:
        logger.error(f"Failed to upsert valuation: {e}")
        return None


def create_material_mapping(
    waste_material: str,
    material_type_id: str,
    match_confidence: float = 1.0,
) -> Optional[int]:
    """Create mapping from waste_listings material to valuation material."""
    try:
        result = execute_query(
            """
            INSERT INTO material_type_mapping 
                (waste_material, material_type_id, match_confidence)
            VALUES (%s, %s, %s)
            ON CONFLICT (waste_material) DO UPDATE SET
                material_type_id = EXCLUDED.material_type_id,
                match_confidence = EXCLUDED.match_confidence
            RETURNING id
            """,
            (waste_material.lower().strip(), material_type_id, match_confidence),
        )
        return result[0]["id"] if result else None
    except Exception as e:
        logger.error(f"Failed to create mapping: {e}")
        return None


def _categorize_material(name: str) -> str:
    """Categorize material based on name."""
    name_lower = name.lower()
    
    if any(w in name_lower for w in ["copper", "cu"]):
        return "metals"
    elif any(w in name_lower for w in ["aluminum", "aluminium", "al"]):
        return "metals"
    elif any(w in name_lower for w in ["steel", "iron", "hms"]):
        return "metals"
    elif any(w in name_lower for w in ["brass", "bronze"]):
        return "metals"
    elif any(w in name_lower for w in ["lead", "pb"]):
        return "metals"
    elif any(w in name_lower for w in ["zinc", "zn"]):
        return "metals"
    elif any(w in name_lower for w in ["plastic", "hdpe", "ldpe", "pet", "pvc", "pp"]):
        return "plastics"
    elif any(w in name_lower for w in ["paper", "cardboard", "occ"]):
        return "paper"
    else:
        return "other"


def generate_material_type_id(material_name: str) -> str:
    """Generate a material type ID from name."""
    # copper bare bright -> CU-BAREBRGHT
    name = material_name.upper().replace(" ", "")
    
    # Common abbreviations
    prefixes = {
        "COPPER": "CU",
        "ALUMINUM": "AL",
        "STEEL": "ST",
        "BRASS": "BR",
        "LEAD": "PB",
        "ZINC": "ZN",
    }
    
    for full, abbr in prefixes.items():
        if name.startswith(full):
            suffix = name[len(full):][:8]  # Take up to 8 chars after prefix
            return f"{abbr}-{suffix}" if suffix else abbr
    
    # Fallback: first 12 chars
    return name[:12]


def store_spider_results(results: dict) -> dict:
    """Store spider results into database."""
    stored = {"raw": 0, "valuations": 0}
    
    for material_name, data in results.get("prices", {}).items():
        # Generate type ID
        type_id = generate_material_type_id(material_name)
        
        # Store valuation
        val_id = upsert_valuation(
            material_type_id=type_id,
            material_name=material_name,
            price_per_ton_usd=data["price_per_ton_usd"],
            price_per_lb_usd=data["price_per_lb_usd"],
            source_count=data["source_count"],
            confidence_score=data["confidence"],
        )
        
        if val_id:
            stored["valuations"] += 1
            logger.info(f"Stored valuation: {material_name} = ${data['price_per_ton_usd']}/ton")
    
    return stored
