"""Quick duplicate and quality check - minimal output."""
import psycopg2
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)
cur = conn.cursor()

# Total
cur.execute("SELECT COUNT(*) FROM waste_listings")
total = cur.fetchone()[0]

# Duplicates
cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT material, source_company, year, quantity_tons
        FROM waste_listings
        WHERE material IS NOT NULL
        GROUP BY material, source_company, year, quantity_tons
        HAVING COUNT(*) > 1
    ) d
""")
dupe_patterns = cur.fetchone()[0]

# Nulls
cur.execute("SELECT COUNT(*) FROM waste_listings WHERE material IS NULL OR material = ''")
null_mat = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM waste_listings WHERE quantity_tons IS NULL OR quantity_tons <= 0")
bad_qty = cur.fetchone()[0]

# BS check
cur.execute("SELECT COUNT(*) FROM waste_listings WHERE year > 2025 OR year < 1970")
bad_years = cur.fetchone()[0]

# Write results
with open("quality_report.txt", "w") as f:
    f.write("DATA QUALITY REPORT\n")
    f.write("===================\n")
    f.write(f"Total Records: {total:,}\n")
    f.write(f"Duplicate Patterns: {dupe_patterns}\n")
    f.write(f"Null/Empty Materials: {null_mat}\n")
    f.write(f"Bad Quantities (null/zero): {bad_qty}\n")
    f.write(f"Invalid Years: {bad_years}\n")
    f.write("\n")
    if dupe_patterns < 100 and null_mat < total * 0.05 and bad_qty < total * 0.1:
        f.write("STATUS: OK TO UPLOAD\n")
    else:
        f.write("STATUS: NEEDS CLEANING\n")

print("Report saved to quality_report.txt")
