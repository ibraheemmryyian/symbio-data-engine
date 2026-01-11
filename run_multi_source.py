"""
Run multi-source pricing spider and store all results.
"""
import logging
from store.postgres import get_connection, execute_query
from spiders.multi_source_spider import run_multi_source_spider

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

print("="*70)
print("MULTI-SOURCE PRICING COLLECTION")
print("="*70)

# Run the spider
results = run_multi_source_spider()

print(f"\nRaw prices: {results['raw_prices']}")
print(f"Aggregated materials: {results['aggregated_materials']}")

# Store raw prices
print("\n" + "="*70)
print("STORING RAW PRICES")
print("="*70)

with get_connection() as conn:
    with conn.cursor() as cur:
        # Create raw prices table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS material_prices_raw (
                id SERIAL PRIMARY KEY,
                material_name VARCHAR(100) NOT NULL,
                price_value DECIMAL(12,4) NOT NULL,
                price_unit VARCHAR(20) NOT NULL,
                currency VARCHAR(3) DEFAULT 'USD',
                source VARCHAR(50) NOT NULL,
                source_url TEXT,
                region VARCHAR(50),
                confidence DECIMAL(3,2),
                fetched_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Clear old prices
        cur.execute("DELETE FROM material_prices_raw")
        
        # Insert new prices
        for p in results['prices']:
            cur.execute("""
                INSERT INTO material_prices_raw 
                    (material_name, price_value, price_unit, currency, source, source_url, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (p.material, p.price_value, p.price_unit, p.currency, p.source, p.source_url, p.confidence))
        
        conn.commit()

print(f"[OK] {len(results['prices'])} raw prices stored")

# Store aggregated valuations
print("\n" + "="*70)
print("STORING AGGREGATED VALUATIONS")
print("="*70)

with get_connection() as conn:
    with conn.cursor() as cur:
        for material, data in results['aggregated'].items():
            type_id = material.upper().replace(" ", "-")[:20]
            
            cur.execute("DELETE FROM material_valuations WHERE material_type_id = %s", (type_id,))
            cur.execute("""
                INSERT INTO material_valuations 
                    (material_type_id, material_name, material_category, 
                     price_per_ton_usd, source_count, confidence_score)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                type_id, 
                material, 
                'multi_source',
                data['price_per_ton_usd'],
                data['source_count'],
                data['confidence']
            ))
        
        conn.commit()

print(f"[OK] {len(results['aggregated'])} aggregated valuations stored")

# Summary
print("\n" + "="*70)
print("FINAL STATS")
print("="*70)

raw_count = execute_query("SELECT COUNT(*) as c FROM material_prices_raw")[0]["c"]
val_count = execute_query("SELECT COUNT(*) as c FROM material_valuations")[0]["c"]

print(f"Raw price records: {raw_count}")
print(f"Material valuations: {val_count}")

# Show top prices
print("\nTOP 15 BY VALUE:")
top = execute_query("""
    SELECT material_type_id, material_name, price_per_ton_usd, source_count
    FROM material_valuations
    ORDER BY price_per_ton_usd DESC
    LIMIT 15
""")
for row in top:
    print(f"  {row['material_name']:<30} ${row['price_per_ton_usd']:>10,.0f}/ton  ({row['source_count']} src)")
