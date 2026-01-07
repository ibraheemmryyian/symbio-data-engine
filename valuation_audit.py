import psycopg2
import os
from config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

def audit_valuation():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cur = conn.cursor()

        # 1. UNCLEAN (Raw Documents)
        cur.execute("SELECT COUNT(*) FROM documents")
        raw_count = cur.fetchone()[0]
        raw_size_mb = 0 # Size content not in DB column directly

        # 2. CLEAN (Processed Listings)
        cur.execute("SELECT COUNT(*), SUM(quantity_tons) FROM waste_listings")
        clean_count, total_tonnage = cur.fetchone()
        
        # 3. Source Breakdown (Clean)
        cur.execute("""
            SELECT d.source, COUNT(*), SUM(wl.quantity_tons) 
            FROM waste_listings wl
            JOIN documents d ON wl.document_id = d.id
            GROUP BY d.source
        """)
        breakdown = cur.fetchall()

        report = []
        report.append(f"\n📊 DATA VALUATION AUDIT")
        report.append(f"========================")
        report.append(f"1. 📦 RAW (Unclean) STORAGE")
        report.append(f"   - Documents:  {raw_count:,}")
        report.append(f"   - Status:     Ingested, waiting for processors")

        report.append(f"\n2. ✨ CLEAN (Structured) ASSETS")
        report.append(f"   - Listings:   {clean_count:,}")
        report.append(f"   - Vol (Tons): {total_tonnage:,.2f} Tons")

        report.append(f"\n3. 💰 VALUATION BREAKDOWN")
        # Assuming a modest avg waste management value of $50/ton for estimation context
        est_market_value = (total_tonnage or 0) * 50 
        report.append(f"   - Est. Market Vol: ${est_market_value:,.2f} (based on $50/ton avg)")
        
        report.append(f"\n4. 🌍 REGIONAL SPLIT")
        for source, count, tons in breakdown:
            report.append(f"   - {source.upper()}: {count:,} records | {(tons or 0):,.0f} tons")

        with open("valuation_report.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(report))
        print("Report saved to valuation_report.txt")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    audit_valuation()
