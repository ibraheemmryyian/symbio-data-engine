import json
import os
import sys
from typing import List, Dict, Optional

# Ensure project root is on sys.path when script is run directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT

# Explicit symbiosis pairs: (source_category, source_material, receiver_category, receiver_material, compat_score)
EXPLICIT_PAIRS = [
    # STEEL INDUSTRY
    ("steel", "Steel Slag", "cement", "Cement", 0.95),
    ("steel", "Steel Slag", "construction", "Concrete", 0.95),
    ("steel", "EAF Dust", "chemical", "Zinc Recovery", 0.85),
    ("steel", "Mill Scale", "construction", "Road Base", 0.90),

    # FOOD INDUSTRY
    ("food", "Food Waste Pulp", "biogas", "Biogas Feedstock", 0.95),
    ("food", "Food Waste Pulp", "agriculture", "Animal Feed", 0.90),
    ("food", "Whey (Dairy)", "food", "Protein Products", 0.95),
    ("food", "Spent Grain (Brewery)", "agriculture", "Animal Feed", 0.95),

    # PLASTICS INDUSTRY
    ("plastics", "HDPE Scrap", "plastics", "Resin Production", 0.95),
    ("plastics", "HDPE Scrap", "construction", "Pipes/Fittings", 0.85),
    ("plastics", "PET Scrap", "textile", "Fiber Production", 0.90),

    # OIL & GAS
    ("oil_gas", "Petroleum Coke", "power", "Power Generation", 0.90),
    ("oil_gas", "Petroleum Coke", "cement", "Cement Kiln Fuel", 0.85),
    ("oil_gas", "Refinery Sludge", "construction", "Asphalt", 0.70),

    # MARITIME
    ("maritime", "Dredged Sediment", "construction", "Aggregate", 0.85),
    ("maritime", "Dredged Sediment", "cement", "Cement Feedstock", 0.75),
    ("maritime", "Desalination Brine", "chemical", "Salt Recovery", 0.95),
]

def calculate_geographic_score(country_1: str, country_2: str) -> float:
    """
    Simple geographic score based on country match.
    0.9 = same country (best for logistics)
    0.7 = same region
    0.2 = different continents
    """
    if country_1 == country_2:
        return 0.9

    regions = {
        "EU": {"DE", "NL", "FR", "IT", "BE", "PL", "SE", "AT", "ES", "CZ", "UK", "CH"},
        "GCC": {"SA", "AE", "QA", "KW", "BH", "OM"},
        "APAC": {"CN", "JP", "IN", "AU", "KR", "SG", "MY"},
    }

    for region, countries in regions.items():
        if country_1 in countries and country_2 in countries:
            return 0.7

    return 0.2

def generate_matches() -> List[Dict]:
    """Generate symbiotic matches using explicit pairing."""
    matches = []

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()

        # Pull source materials
        cursor.execute("""
            SELECT DISTINCT
                material,
                AVG(quantity_tons) as avg_qty,
                array_agg(DISTINCT source_country ORDER BY source_country) as countries
            FROM waste_listings
            WHERE material IS NOT NULL AND material != ''
            GROUP BY material
            ORDER BY avg_qty DESC
            LIMIT 500
        """)

        source_materials = {}
        for row in cursor.fetchall():
            source_materials[row[0]] = {
                "avg_qty": row[1] or 0,
                "countries": list(row[2] or []),
            }

        # Generate matches from explicit pairs
        for src_cat, src_mat, recv_cat, recv_mat, compat_score in EXPLICIT_PAIRS:
            if src_mat not in source_materials:
                continue

            src_data = source_materials[src_mat]
            if not src_data["countries"]:
                continue

            src_country = src_data["countries"][0]
            geo_score = calculate_geographic_score(src_country, src_country)
            volume_factor = min(1.0, (src_data["avg_qty"] / 1_000_000))

            combined_score = (compat_score * 0.5) + (geo_score * 0.3) + (volume_factor * 0.2)
            combined_score = min(1.0, max(0.0, combined_score))

            match = {
                "material_source": src_mat,
                "material_receiver": recv_mat,
                "source_category": src_cat,
                "receiver_category": recv_cat,
                "compatibility_score": round(compat_score, 2),
                "geographic_score": round(geo_score, 2),
                "combined_score": round(combined_score, 2),
                "avg_volume_tons": int(src_data["avg_qty"]),
                "symbiosis_type": f"{src_cat}-to-{recv_cat}",
            }
            matches.append(match)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error generating matches: {e}")
        raise

    return matches

def export_to_jsonl(matches: List[Dict], output_file: str = "exports/improved_symbiotic_matches.jsonl"):
    """Export matches to JSONL format."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for match in matches:
            f.write(json.dumps(match) + '\n')
    print(f"Exported {len(matches)} matches to {output_file}")

if __name__ == "__main__":
    print("Generating improved symbiotic matches...")
    matches = generate_matches()

    print(f"Generated {len(matches)} matches")
    by_source = {}
    for m in matches:
        cat = m["source_category"]
        by_source[cat] = by_source.get(cat, 0) + 1

    print("\nMatches by source category:")
    for cat in sorted(by_source.keys()):
        print(f"  {cat}: {by_source[cat]} matches")

    export_to_jsonl(matches)
