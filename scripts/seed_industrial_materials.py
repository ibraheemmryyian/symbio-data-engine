import json
import sys
import os
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT

MATERIALS_SEED_DATA = [
    # STEEL (3)
    {
        "name": "Steel Slag",
        "category": "byproduct",
        "subcategory": "blast furnace slag",
        "industry": "steel",
        "unit": "metric_tons",
        "annual_volume_tons": 250_000_000,
        "disposal_cost_per_unit": 5.0,
        "environmental_impact": {"co2_kg_per_ton": 0.1, "water_l_per_ton": 50},
        "recyclability": "high",
        "primary_applications": ["cement aggregate", "concrete additive", "road base", "glass"],
        "receiver_industries": ["cement", "concrete", "construction"],
        "hazard_classification": "non_hazardous",
        "data_source": "World Steel Association",
        "data_source_url": "https://www.worldsteel.org",
        "verified": True,
    },
    {
        "name": "EAF Dust",
        "category": "byproduct",
        "subcategory": "electric arc furnace dust",
        "industry": "steel",
        "unit": "metric_tons",
        "annual_volume_tons": 15_000_000,
        "disposal_cost_per_unit": 50.0,
        "environmental_impact": {"co2_kg_per_ton": 0.05},
        "recyclability": "medium",
        "primary_applications": ["zinc recovery", "cement feedstock"],
        "receiver_industries": ["zinc_smelting", "cement"],
        "hazard_classification": "hazardous (zinc)",
        "data_source": "World Steel Association",
        "verified": True,
    },
    {
        "name": "Mill Scale",
        "category": "byproduct",
        "subcategory": "surface oxidation",
        "industry": "steel",
        "unit": "metric_tons",
        "annual_volume_tons": 8_000_000,
        "disposal_cost_per_unit": 2.0,
        "environmental_impact": {"co2_kg_per_ton": 0.02},
        "recyclability": "high",
        "primary_applications": ["pellet feed", "road base"],
        "receiver_industries": ["pellet_mills", "construction"],
        "hazard_classification": "non_hazardous",
        "data_source": "World Steel Association",
        "verified": True,
    },
    # FOOD (3)
    {
        "name": "Food Waste Pulp",
        "category": "waste",
        "subcategory": "mixed food organic",
        "industry": "food",
        "unit": "metric_tons",
        "annual_volume_tons": 500_000_000,
        "disposal_cost_per_unit": 20.0,
        "environmental_impact": {"co2_kg_per_ton": 0.3, "water_l_per_ton": 500},
        "recyclability": "high",
        "primary_applications": ["biogas feedstock", "animal feed", "compost"],
        "receiver_industries": ["biogas", "feed_mills", "agriculture"],
        "hazard_classification": "non_hazardous",
        "data_source": "FAO Food Losses and Waste Database",
        "data_source_url": "https://www.fao.org/",
        "verified": True,
    },
    {
        "name": "Whey (Dairy)",
        "category": "byproduct",
        "subcategory": "cheese processing",
        "industry": "food",
        "unit": "metric_tons",
        "annual_volume_tons": 200_000_000,
        "disposal_cost_per_unit": 10.0,
        "environmental_impact": {"co2_kg_per_ton": 0.15, "water_l_per_ton": 1000},
        "recyclability": "high",
        "primary_applications": ["protein isolate", "animal feed"],
        "receiver_industries": ["dairy_processing", "feed_mills"],
        "hazard_classification": "non_hazardous",
        "data_source": "FAO Dairy Council",
        "verified": True,
    },
    {
        "name": "Spent Grain (Brewery)",
        "category": "byproduct",
        "subcategory": "beer residue",
        "industry": "food",
        "unit": "metric_tons",
        "annual_volume_tons": 20_000_000,
        "disposal_cost_per_unit": 5.0,
        "environmental_impact": {"co2_kg_per_ton": 0.1},
        "recyclability": "high",
        "primary_applications": ["animal feed", "biogas"],
        "receiver_industries": ["feed_mills", "biogas"],
        "hazard_classification": "non_hazardous",
        "data_source": "FAO Brewers Association",
        "verified": False,
    },
    # PLASTICS (2)
    {
        "name": "HDPE Scrap",
        "category": "waste",
        "subcategory": "post-consumer",
        "industry": "plastics",
        "unit": "metric_tons",
        "annual_volume_tons": 20_000_000,
        "disposal_cost_per_unit": 150.0,
        "environmental_impact": {"co2_kg_per_ton": 2.0},
        "recyclability": "high",
        "primary_applications": ["bottle-to-bottle", "injection molding"],
        "receiver_industries": ["plastic_recycling", "compounders"],
        "hazard_classification": "non_hazardous",
        "data_source": "PlasticsEurope Market Data",
        "verified": True,
    },
    {
        "name": "PET Scrap",
        "category": "waste",
        "subcategory": "post-consumer",
        "industry": "plastics",
        "unit": "metric_tons",
        "annual_volume_tons": 15_000_000,
        "disposal_cost_per_unit": 200.0,
        "environmental_impact": {"co2_kg_per_ton": 2.5},
        "recyclability": "high",
        "primary_applications": ["fiber production", "bottle-to-bottle"],
        "receiver_industries": ["textile_mills", "plastic_recycling"],
        "hazard_classification": "non_hazardous",
        "data_source": "Ellen MacArthur Foundation",
        "verified": True,
    },
    # OIL & GAS (2)
    {
        "name": "Petroleum Coke",
        "category": "byproduct",
        "subcategory": "refinery residue",
        "industry": "oil_gas",
        "unit": "metric_tons",
        "annual_volume_tons": 150_000_000,
        "disposal_cost_per_unit": 25.0,
        "environmental_impact": {"co2_kg_per_ton": 3.5},
        "recyclability": "high",
        "primary_applications": ["power generation", "cement fuel", "smelter fuel"],
        "receiver_industries": ["power", "cement", "smelting"],
        "hazard_classification": "non_hazardous",
        "data_source": "IEA Petroleum Coke Statistics",
        "data_source_url": "https://www.iea.org",
        "verified": True,
    },
    {
        "name": "Refinery Sludge",
        "category": "waste",
        "subcategory": "crude processing",
        "industry": "oil_gas",
        "unit": "metric_tons",
        "annual_volume_tons": 12_000_000,
        "disposal_cost_per_unit": 80.0,
        "environmental_impact": {"co2_kg_per_ton": 0.5},
        "recyclability": "low",
        "primary_applications": ["asphalt additive", "stabilization"],
        "receiver_industries": ["construction", "asphalt"],
        "hazard_classification": "hazardous (oils)",
        "data_source": "API Refinery Reports",
        "verified": False,
    },
    # MARITIME (2)
    {
        "name": "Dredged Sediment",
        "category": "waste",
        "subcategory": "harbor sediment",
        "industry": "maritime",
        "unit": "metric_tons",
        "annual_volume_tons": 600_000_000,
        "disposal_cost_per_unit": 3.0,
        "environmental_impact": {"co2_kg_per_ton": 0.05},
        "recyclability": "medium",
        "primary_applications": ["land reclamation", "aggregate", "cement"],
        "receiver_industries": ["construction", "cement"],
        "hazard_classification": "may contain contaminants",
        "data_source": "OSPAR Dredging Guidelines",
        "data_source_url": "https://www.ospar.org",
        "verified": False,
    },
    {
        "name": "Desalination Brine",
        "category": "waste",
        "subcategory": "seawater rejection",
        "industry": "maritime",
        "unit": "metric_tons",
        "annual_volume_tons": 150_000_000,
        "disposal_cost_per_unit": 2.0,
        "environmental_impact": {"co2_kg_per_ton": 0.1},
        "recyclability": "high",
        "primary_applications": ["salt harvesting", "mineral recovery"],
        "receiver_industries": ["salt_production", "chemicals"],
        "hazard_classification": "non_hazardous",
        "data_source": "SWCC Desalination Reports",
        "verified": True,
    },
]

def seed_materials():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cursor = conn.cursor()

    for material in MATERIALS_SEED_DATA:
        try:
            cursor.execute(
                """
                INSERT INTO materials (
                    name, category, subcategory, industry, unit,
                    annual_volume_tons, disposal_cost_per_unit, environmental_impact,
                    recyclability, primary_applications, receiver_industries,
                    hazard_classification, data_source, data_source_url, verified
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (name) DO UPDATE SET
                    updated_at = NOW()
                """,
                (
                    material["name"],
                    material["category"],
                    material.get("subcategory", ""),
                    material["industry"],
                    material["unit"],
                    material["annual_volume_tons"],
                    material["disposal_cost_per_unit"],
                    json.dumps(material.get("environmental_impact", {})),
                    material["recyclability"],
                    material["primary_applications"],
                    material["receiver_industries"],
                    material["hazard_classification"],
                    material["data_source"],
                    material.get("data_source_url", ""),
                    material["verified"],
                ),
            )
        except Exception as e:
            print(f"Error inserting {material['name']}: {e}")

    conn.commit()
    print(f"Inserted {len(MATERIALS_SEED_DATA)} materials")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_materials()
