
import psycopg2
import pandas as pd
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def analyze_data():
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    
    # 1. Column Density (How full is the data?)
    print("ANALYZING COLUMN DENSITY...")
    df = pd.read_sql("SELECT * FROM waste_listings LIMIT 50000", conn)
    density = df.count() / len(df) * 100
    print(density)
    
    # 2. Material Diversity (Top 10 vs Long Tail)
    print("\nANALYZING MATERIAL DIVERSITY...")
    cur = conn.cursor()
    cur.execute("""
        SELECT material, COUNT(*) as cnt 
        FROM waste_listings 
        GROUP BY material 
        ORDER BY cnt DESC 
        LIMIT 20
    """)
    top_materials = cur.fetchall()
    for m, c in top_materials:
        print(f"  - {m}: {c}")

    # 3. Treatment Fate
    print("\nANALYZING WASTE FATE...")
    cur.execute("""
        SELECT treatment_method, COUNT(*) as cnt 
        FROM waste_listings 
        GROUP BY treatment_method 
        ORDER BY cnt DESC
    """)
    fate = cur.fetchall()
    for m, c in fate:
        print(f"  - {m}: {c}")

if __name__ == "__main__":
    analyze_data()
