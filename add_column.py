from store.postgres import get_connection

print("Adding treatment_method column to waste_listings table...")

with get_connection() as conn:
    with conn.cursor() as cur:
        try:
            cur.execute("""
                ALTER TABLE waste_listings 
                ADD COLUMN IF NOT EXISTS treatment_method VARCHAR(50)
            """)
            conn.commit()
            print("✅ Column added successfully!")
        except Exception as e:
            print(f"❌ Error: {e}")
            conn.rollback()

# Verify
from store.postgres import execute_query
cols = execute_query("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'waste_listings' AND column_name = 'treatment_method'
""")
if cols:
    print(f"Verified: treatment_method column exists")
else:
    print("Column NOT found!")
