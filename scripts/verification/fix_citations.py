from store.postgres import get_connection

print("Adding source_quote column to waste_listings...")

with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE waste_listings 
            ADD COLUMN IF NOT EXISTS source_quote TEXT
        """)
        conn.commit()
        print("✅ source_quote column added!")

# Update column whitelist in postgres.py
print("\nNOTE: Also need to add 'source_quote' to VALID_COLUMNS in insert_waste_listing()")
print("This ensures future inserts include the citation.")
