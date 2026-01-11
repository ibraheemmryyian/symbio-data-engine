"""
ENRICH ALPHA LIST
=================
Goal: Create the "Verified Inventory" (Top 1%) by enriching high-quality listings 
with deep process knowledge.
Strategy:
1. Filter Listings to companies where we have Process Knowledge.
2. Join on Company Name (Fuzzy Match).
3. Inject "Hard Data" (CAS, Volumes, Process Context).
"""
import pandas as pd
from fuzzywuzzy import process, fuzz

def enrich():
    print("Creating Alpha Verified Inventory...")
    
    # 1. LOAD DATA
    print("Loading datasets...")
    # Knowledge Base (The 463 reports)
    knowledge = pd.read_csv("exports/process_knowledge_v1.csv")
    # Simplify knowledge: Aggregate text per company
    knowledge_grouped = knowledge.groupby('Company').agg({
        'Keyword': lambda x: list(x)[:5],
        'Context (Excerpt)': lambda x: " | ".join(list(x)[:3]),
        'Filename': 'first'
    }).reset_index()
    
    print(f"Knowledge Base: {len(knowledge_grouped)} companies.")

    # Listings (The 850k)
    listings = pd.read_csv("exports/waste_listings_granular.csv") 
    listings['source_company'] = listings['source_company'].fillna('')
    print(f"Listings: {len(listings)} rows.")

    # 2. MATCHING LOGIC
    # We only want listings that match our Knowledge Base companies.
    # Create a lookup map for fast iteration
    kb_companies = knowledge_grouped['Company'].unique()
    
    alpha_rows = []
    
    print("Matching companies (this may take a minute)...")
    
    # Pre-compute fuzzy map for known companies in listings
    # To speed up, we get unique listing companies first
    unique_listing_companies = listings['source_company'].unique()
    
    # Map Listing Company -> Knowledge Company (if match > 90)
    company_map = {}
    
    for l_comp in unique_listing_companies:
        if not l_comp: continue
        # Quick exact check first
        if l_comp in kb_companies:
            company_map[l_comp] = l_comp
            continue
            
        # Fuzzy check (expensive, only do for potential targets)
        # Optimization: Only check if starts with same letter
        match, score = process.extractOne(l_comp, kb_companies, scorer=fuzz.token_sort_ratio)
        if score > 85:
            company_map[l_comp] = match

    print(f"Mapped {len(company_map)} companies from listings to knowledge base.")

    # 3. ENRICHMENT
    # Filter listings to only those in the map
    alpha_listings = listings[listings['source_company'].isin(company_map.keys())].copy()
    
    # Add Knowledge Columns
    def get_knowledge(row):
        l_comp = row['source_company']
        kb_comp = company_map.get(l_comp)
        if not kb_comp: return pd.Series(["", ""])
        
        k_row = knowledge_grouped[knowledge_grouped['Company'] == kb_comp].iloc[0]
        return pd.Series([k_row['Context (Excerpt)'], k_row['Filename']])

    print("Enriching rows...")
    alpha_listings[['process_context', 'evidence_file']] = alpha_listings.apply(get_knowledge, axis=1)

    # 4. EXPORT
    output_file = "exports/alpha_verified_inventory.csv"
    alpha_listings.to_csv(output_file, index=False)
    
    print(f"\nSUCCESS: Created Alpha List with {len(alpha_listings)} verified rows.")
    print(f"Saved to {output_file}")
    
    # Stats
    print("\nSAMPLE ROW:")
    print(alpha_listings[['source_company', 'waste_description', 'process_context']].head(1).T)

if __name__ == "__main__":
    enrich()
