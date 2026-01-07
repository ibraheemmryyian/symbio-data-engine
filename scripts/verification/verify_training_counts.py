
import psycopg2
import json
import os
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def verify_counts():
    print("📊 AI TRAINING DATA BREAKDOWN\n")
    
    # 1. FILE-BASED COUNTS (The "Brain")
    training_dir = "data/training"
    
    # NLP / Report Generation (Chat JSONL)
    chat_path = os.path.join(training_dir, "symbio_chat_finetune_v1.jsonl")
    try:
        with open(chat_path, 'r', encoding='utf-8') as f:
            chat_count = sum(1 for _ in f)
        print(f"1. NLP / REPORT GENERATION (Fine-Tuning): {chat_count:,} Training Pairs")
        print(f"   - Purpose: Teaching the AI to write reports, analyze waste, and chat.")
    except Exception as e:
        print(f"1. NLP: Global Error {e}")

    # GNN (Graph Edges)
    graph_path = os.path.join(training_dir, "symbio_graph_edges.csv")
    try:
        with open(graph_path, 'r', encoding='utf-8') as f:
            # subtract header
            graph_count = sum(1 for _ in f) - 1
        print(f"2. GNN / NETWORK ANALYSIS (Graph Edges): {graph_count:,} Edges")
        print(f"   - Purpose: Predicting hidden connections between facilities.")
    except:
        print("2. GNN: Error")

    # NLP Corpus (Pre-training)
    corpus_path = os.path.join(training_dir, "symbio_corpus_v1.txt")
    try:
        size_mb = os.path.getsize(corpus_path) / (1024 * 1024)
        print(f"3. ORACLE PRE-TRAINING (Corpus): {size_mb:.2f} MB of Text")
        print(f"   - Purpose: Deep understanding of industrial waste physics.")
    except:
        print("3. Corpus: Error")

    # 2. DATABASE-BASED COUNTS (The "Business Logic")
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cur = conn.cursor()

    # Portfolio Generation (Unique Companies)
    cur.execute("SELECT COUNT(DISTINCT source_company) FROM waste_listings")
    companies = cur.fetchone()[0]
    print(f"4. PORTFOLIO GENERATION (Unique Profiles): {companies:,} Companies")
    print(f"   - Purpose: Generating investment/sales portfolios for specific targets.")

    # Matchmaking (Potential Matches)
    # Heuristic: How many facilities produce 'Metal' can match with others? 
    # Actually, simplistic view: Total Listings is the "Supply Side".
    cur.execute("SELECT COUNT(*) FROM waste_listings")
    listings = cur.fetchone()[0]
    print(f"5. MATCHMAKING (Supply Liquidity): {listings:,} Tradeable Assets")
    
    # Logistics (Unique Locations)
    cur.execute("SELECT COUNT(DISTINCT source_location) FROM waste_listings")
    locs = cur.fetchone()[0]
    print(f"6. LOGISTICS (Geospatial Nodes): {locs:,} Routing Points")

    conn.close()

if __name__ == "__main__":
    verify_counts()
