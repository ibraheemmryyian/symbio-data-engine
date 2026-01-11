#!/usr/bin/env python3
"""Quick audit to show data gaps without pricing."""
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()

print('='*60)
print('SYMBIO DATA ENGINE - DATA GAP ANALYSIS')
print('='*60)

# Overall counts
cur.execute('SELECT COUNT(*) FROM waste_listings')
total = cur.fetchone()[0]

cur.execute('SELECT COUNT(*) FROM waste_listings WHERE price_per_ton IS NOT NULL')
with_price = cur.fetchone()[0]

cur.execute('SELECT COUNT(*) FROM waste_listings WHERE quantity_tons IS NOT NULL')
with_quantity = cur.fetchone()[0]

cur.execute('SELECT COUNT(*) FROM waste_listings WHERE source_company IS NOT NULL')
with_company = cur.fetchone()[0]

cur.execute('SELECT COUNT(*) FROM waste_listings WHERE material_category IS NOT NULL')
with_category = cur.fetchone()[0]

print(f'\n[WASTE LISTINGS COMPLETENESS]')
print(f'   Total Records: {total:,}')
print(f'   With Pricing: {with_price:,} ({100*with_price/max(total,1):.1f}%)')
print(f'   With Quantity: {with_quantity:,} ({100*with_quantity/max(total,1):.1f}%)')
print(f'   With Company: {with_company:,} ({100*with_company/max(total,1):.1f}%)')
print(f'   With Category: {with_category:,} ({100*with_category/max(total,1):.1f}%)')

# By material category
print(f'\n[RECORDS BY CATEGORY - top 10]')
cur.execute('''
    SELECT material_category, COUNT(*) as cnt, 
           COUNT(price_per_ton) as priced,
           SUM(quantity_tons) as total_tons
    FROM waste_listings 
    GROUP BY material_category 
    ORDER BY cnt DESC LIMIT 10
''')
for row in cur.fetchall():
    cat, cnt, priced, tons = row
    cat_display = cat if cat else 'UNCATEGORIZED'
    tons_str = f'{tons:,.0f}t' if tons else 'N/A'
    print(f'   {cat_display}: {cnt:,} records, {priced} priced, {tons_str}')

# What we CAN value vs CANNOT
print(f'\n[VALUATION IMPACT]')
cur.execute('SELECT SUM(quantity_tons) FROM waste_listings WHERE price_per_ton IS NOT NULL')
valued_tons = cur.fetchone()[0] or 0
cur.execute('SELECT SUM(quantity_tons) FROM waste_listings WHERE price_per_ton IS NULL AND quantity_tons IS NOT NULL')
unvalued_tons = cur.fetchone()[0] or 0
total_tons = valued_tons + unvalued_tons

print(f'   Total waste tons tracked: {total_tons:,.0f}')
print(f'   Tons WITH pricing: {valued_tons:,.0f}')
print(f'   Tons WITHOUT pricing: {unvalued_tons:,.0f}')
if total_tons > 0:
    print(f'   UNPRICED: {100*unvalued_tons/total_tons:.1f}%')

# What this means for the platform
print(f'\n[BUSINESS IMPACT]')
if with_price == 0:
    print('   STATUS: CRITICAL - 0% pricing coverage')
    print('   - Cannot calculate waste-to-value conversions')
    print('   - Cannot power marketplace pricing suggestions')
    print('   - Cannot train AI valuation models')
    print('   - Cannot generate ROI projections for customers')
elif with_price / max(total, 1) < 0.1:
    print('   STATUS: SEVERE - <10% pricing coverage')
    print('   - Very limited valuation capability')
else:
    print(f'   STATUS: PARTIAL - {100*with_price/total:.1f}% pricing coverage')

conn.close()
