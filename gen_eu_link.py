import httpx

# The exact query for the dataset
QUERY = "Industrial Reporting under the Industrial Emissions Directive"
API_URL = "https://data.europa.eu/api/hub/search/search"

print(f"🔍 Searching for: '{QUERY}'...")

try:
    with httpx.Client(timeout=30.0) as client:
        # Construct a search params roughly matching their API
        # We'll use the 'q' param against their CKAN/Search API
        params = {
            "q": QUERY,
            "filter": "dataset",
            "limit": 1
        }
        # Note: The portal uses a complex API. Let's try to construct a search URL for the USER to click.
        # Direct search URL:
        user_link = f"https://data.europa.eu/data/datasets?query={QUERY.replace(' ', '+')}&locale=en&publisher=http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fauthority%2Fcorporate-body%2FEEA"
        
        print("\n✅ GENERATED DIRECT FILTER LINK:")
        print(user_link)
        
        # Verify if it returns 200
        resp = client.get(user_link)
        if resp.status_code == 200:
            print("   (Link is valid and accessible)")
        else:
            print(f"   (Warning: Status {resp.status_code})")

except Exception as e:
    print(f"Error: {e}")
