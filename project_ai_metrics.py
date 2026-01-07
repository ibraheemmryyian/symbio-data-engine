
import psycopg2
import pandas as pd
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def calculate_ai_potential():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()
    
    print("CALCULATING AI TRAINING POTENTIAL...\n")
    
    # 1. Intelligent Filling (Imputation Potential)
    # How many "Paint Factories" do we have? (Using standard industries as proxies)
    cur.execute("""
        SELECT source_industry, COUNT(*) as facility_count, COUNT(DISTINCT material) as unique_waste_types
        FROM waste_listings
        WHERE source_industry IS NOT NULL
        GROUP BY source_industry
        ORDER BY facility_count DESC
        LIMIT 10
    """)
    industries = cur.fetchall()
    
    print("1. INTELLIGENT FILLING (Profile Prediction)")
    print("   We can teach the AI: 'If Industry X, then likely Waste Y'")
    for ind, facs, wastes in industries:
        if ind:
            print(f"   - {ind}: {facs:,} facilities providing {wastes} unique waste profiles.")
    
    # 2. Symbiosis Matches (Training Pairs)
    # Estimate exact matches (Material X -> Receiver Y)
    # Simple logic: Organic -> Composting, Metal -> Recycling
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material_category = 'Waste from chemical processing'")
    chem = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material_category = 'Waste from waste management facilities'") # Proxy for receivers
    receivers = cur.fetchone()[0]
    
    # In a full mesh, potential connections are Generator * Receiver (huge), but we limit to realistic radius
    # For training data, we create 1 positive match and 5 negative matches per listing
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    total_listings = cur.fetchone()[0]
    
    training_pairs = total_listings * 6 # 1 positive + 5 negative examples
    
    print("\n2. SYMBIOSIS MATCHING (Alpha-Zero Style)")
    print(f"   - Total Listings: {total_listings:,}")
    print(f"   - Potential Receivers (Est): {receivers:,}")
    print(f"   - Training Pairs (Pos/Neg): {training_pairs:,} examples")
    print("   (This is the dataset size for the Matchmaker Model)")

    # 3. Logistics (Route Optimization)
    # Records with specific location
    cur.execute("SELECT COUNT(*) FROM waste_listings WHERE source_location LIKE '%,%'") # Simple check for City, Country
    locs = cur.fetchone()[0]
    
    print("\n3. LOGISTICS & PLANNING")
    print(f"   - Geocoded Points: {locs:,}")
    print(f"   - Route Scenarios: {locs * 10:,} (Simulated transfers between random points)")

if __name__ == "__main__":
    calculate_ai_potential()
