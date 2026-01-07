"""
SYMBIO TRAINING DATA EXPORT
============================
Generates AI training data from collected waste listings.
Run anytime: python export_training.py

DOES NOT affect collectors or processors - read-only!
"""
import json
from pathlib import Path
from datetime import datetime
from store.postgres import execute_query

OUTPUT_DIR = Path("exports")
OUTPUT_DIR.mkdir(exist_ok=True)

def export_all():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    print("🚀 Exporting AI Training Data...")
    
    # 1. RAW JSONL (for embeddings/RAG)
    print("\n1️⃣ Raw listings → JSONL")
    records = execute_query("""
        SELECT material, quantity_tons, source_company, treatment_method, 
               source_location, year, source_quote
        FROM waste_listings 
        WHERE material IS NOT NULL
    """)
    
    raw_file = OUTPUT_DIR / f"waste_listings_{timestamp}.jsonl"
    with open(raw_file, "w") as f:
        for r in records:
            f.write(json.dumps({k: str(v) if v else None for k, v in dict(r).items()}) + "\n")
    print(f"   → {raw_file} ({len(records)} records)")
    
    # 2. MATCHMAKING PAIRS (producer → consumer)
    print("\n2️⃣ Matchmaking pairs")
    producers = execute_query("""
        SELECT DISTINCT material, source_company, quantity_tons
        FROM waste_listings 
        WHERE treatment_method = 'Disposal/Released' AND quantity_tons > 0
    """)
    consumers = execute_query("""
        SELECT DISTINCT material, source_company
        FROM waste_listings 
        WHERE treatment_method = 'Recycled'
    """)
    
    # Create material → companies lookup
    consumer_map = {}
    for c in consumers:
        mat = c['material']
        if mat not in consumer_map:
            consumer_map[mat] = []
        consumer_map[mat].append(c['source_company'])
    
    pairs_file = OUTPUT_DIR / f"symbiosis_pairs_{timestamp}.jsonl"
    pair_count = 0
    with open(pairs_file, "w") as f:
        for p in producers:
            mat = p['material']
            if mat in consumer_map:
                pair = {
                    "producer": p['source_company'],
                    "material": mat,
                    "quantity_mt": float(p['quantity_tons']) if p['quantity_tons'] else 0,
                    "potential_consumers": consumer_map[mat][:5],
                    "match_type": "material_match"
                }
                f.write(json.dumps(pair) + "\n")
                pair_count += 1
    print(f"   → {pairs_file} ({pair_count} pairs)")
    
    # 3. FINE-TUNING Q&A
    print("\n3️⃣ Fine-tuning examples")
    qa_file = OUTPUT_DIR / f"finetune_qa_{timestamp}.jsonl"
    
    # Get ALL materials (NO LIMIT)
    top_mats = execute_query("""
        SELECT material, count(*) as cnt, SUM(quantity_tons) as total
        FROM waste_listings 
        GROUP BY material ORDER BY cnt DESC
    """)
    
    qa_count = 0
    with open(qa_file, "w") as f:
        for m in top_mats:
            mat = m['material']
            # Q: Who produces X? (Max 50 to fit 32k context)
            companies = execute_query(f"""
                SELECT DISTINCT source_company FROM waste_listings 
                WHERE material = %s LIMIT 50
            """, (mat,))
            
            if companies:
                comps = ', '.join([c['source_company'][:30] for c in companies])
                vol = float(m['total'] or 0)
                
                # Variation 1: Producer lookup
                qa1 = {
                    "instruction": f"What companies produce {mat} as industrial waste?",
                    "response": f"Based on EPA data, producers of {mat} include: {comps}. Total reported volume: {vol:.1f} metric tons."
                }
                f.write(json.dumps(qa1) + "\n")
                
                # Variation 2: Sourcing
                qa2 = {
                    "instruction": f"I need to source {mat} for recycling. Who has it?",
                    "response": f"Potential sources for {mat} include {comps}. There is approximately {vol:.1f} MT available in the market."
                }
                f.write(json.dumps(qa2) + "\n")
                
                qa_count += 2
    
    print(f"   → {qa_file} ({qa_count} Q&A pairs)")
    
    print("\n✅ EXPORT COMPLETE!")
    print(f"   Files in: {OUTPUT_DIR.absolute()}")

if __name__ == "__main__":
    export_all()
