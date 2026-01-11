"""
OVERLAP ANALYSIS
================
Goal: Check how many "Insight" companies exist in the "Listings" database.
This helps estimate the difficulty of enrichment.
"""
import pandas as pd

def check_overlap():
    print("Loading datasets...")
    # Load Insights (Small)
    insights = pd.read_csv("exports/process_knowledge_v1.csv")
    insight_companies = set(insights['Company'].unique())
    print(f"Insight Companies: {len(insight_companies)}")

    # Load Listings (Large) - Use chunks to avoid memory issues if massive, but 850k rows is manageable (~100MB)
    # Just read the 'company_name' column to speed up
    try:
        listings = pd.read_csv("exports/waste_listings_granular.csv", usecols=["company_name"])
        listing_companies = set(listings['company_name'].dropna().unique())
        print(f"Listing Companies: {len(listing_companies)}")
    except ValueError:
        # Fallback if column name is different
        print("Could not find 'company_name'. Checking columns...")
        df_head = pd.read_csv("exports/waste_listings_granular.csv", nrows=1)
        print(f"Columns: {list(df_head.columns)}")
        return

    # Check Direct Overlap
    exact_matches = insight_companies.intersection(listing_companies)
    print(f"Exact Matches: {len(exact_matches)}")
    
    # Check Normalization (lowercase, strip)
    insight_norm = {c.lower().strip() for c in insight_companies}
    listing_norm = {str(c).lower().strip() for c in listing_companies}
    norm_matches = insight_norm.intersection(listing_norm)
    print(f"Normalized Matches: {len(norm_matches)}")

if __name__ == "__main__":
    check_overlap()
