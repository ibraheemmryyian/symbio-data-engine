"""
Apply pricing schema and mappings using proper connection handling.
"""
from store.postgres import get_connection, execute_query

print("="*60)
print("STEP 1: CREATE TABLES")
print("="*60)

# Create tables using direct connection (required for DDL)
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS material_valuations (
                id SERIAL PRIMARY KEY,
                material_type_id VARCHAR(20) UNIQUE NOT NULL,
                material_name VARCHAR(100) NOT NULL,
                material_category VARCHAR(50),
                price_per_ton_usd DECIMAL(12,2) NOT NULL,
                price_per_lb_usd DECIMAL(12,4),
                source_count INT DEFAULT 1,
                confidence_score DECIMAL(3,2),
                last_updated TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS material_type_mapping (
                id SERIAL PRIMARY KEY,
                waste_material VARCHAR(200) NOT NULL,
                material_type_id VARCHAR(20) NOT NULL,
                match_confidence DECIMAL(3,2) DEFAULT 1.0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        print("[OK] Tables created")

print("\n" + "="*60)
print("STEP 2: INSERT PRICES")
print("="*60)

prices = [
    ("CU-BAREBRGHT", "copper bare bright", 10780.61, 4.89, "metals"),
    ("CU-WIRE1", "copper wire 1", 10560.15, 4.79, "metals"),
    ("CU-WIRE2", "copper wire 2", 10339.69, 4.69, "metals"),
    ("CU-LIGHT", "copper light", 10119.23, 4.59, "metals"),
    ("AL-6063", "aluminum 6063", 2094.39, 0.95, "metals"),
    ("AL-6061", "aluminum 6061", 1631.42, 0.74, "metals"),
    ("AL-UBC", "aluminum ubc", 1807.79, 0.82, "metals"),
    ("AL-WHEELS", "aluminum wheels", 1851.88, 0.84, "metals"),
    ("BR-YELLOW", "brass yellow", 6525.69, 2.96, "metals"),
    ("BR-RED", "brass red", 6878.43, 3.12, "metals"),
    ("ST-HMS1", "steel hms 1", 320.00, 0.15, "metals"),
    ("ST-BUSHEL", "steel busheling", 420.00, 0.19, "metals"),
    ("ST-SHREDDED", "steel shredded", 370.00, 0.17, "metals"),
    ("PB-BATTERIES", "lead batteries", 529.11, 0.24, "metals"),
    ("PB-SOLID", "lead solid", 1543.24, 0.70, "metals"),
]

with get_connection() as conn:
    with conn.cursor() as cur:
        for type_id, name, per_ton, per_lb, cat in prices:
            cur.execute("DELETE FROM material_valuations WHERE material_type_id = %s", (type_id,))
            cur.execute("""
                INSERT INTO material_valuations 
                    (material_type_id, material_name, material_category, 
                     price_per_ton_usd, price_per_lb_usd, source_count, confidence_score)
                VALUES (%s, %s, %s, %s, %s, 1, 0.8)
            """, (type_id, name, cat, per_ton, per_lb))
        conn.commit()
        print(f"[OK] Inserted {len(prices)} prices")

print("\n" + "="*60)
print("STEP 3: MAP MATERIALS")
print("="*60)

mapping_rules = [
    ("copper", "CU-BAREBRGHT"),
    ("aluminum", "AL-6063"),
    ("aluminium", "AL-6063"),
    ("brass", "BR-YELLOW"),
    ("bronze", "BR-YELLOW"),
    ("steel", "ST-HMS1"),
    ("iron", "ST-HMS1"),
    ("lead", "PB-SOLID"),
]

materials = execute_query("SELECT DISTINCT material FROM waste_listings")
mapped = 0

with get_connection() as conn:
    with conn.cursor() as cur:
        for row in materials:
            mat = row["material"].lower()
            for keyword, type_id in mapping_rules:
                if keyword in mat:
                    cur.execute("DELETE FROM material_type_mapping WHERE waste_material = %s", (row["material"],))
                    cur.execute("""
                        INSERT INTO material_type_mapping (waste_material, material_type_id, match_confidence)
                        VALUES (%s, %s, 0.85)
                    """, (row["material"], type_id))
                    mapped += 1
                    break
        conn.commit()

print(f"[OK] Mapped {mapped} materials")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
val_count = execute_query("SELECT COUNT(*) as c FROM material_valuations")[0]["c"]
map_count = execute_query("SELECT COUNT(*) as c FROM material_type_mapping")[0]["c"]
print(f"Valuations: {val_count}")
print(f"Mappings: {map_count} / 586 materials")
print("Ready for valuation queries!")
