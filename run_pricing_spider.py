#!/usr/bin/env python3
"""
Run full pricing spider and display results.
Schema must be applied to Supabase manually or via admin.
"""
import logging
from spiders.pricing_spider import PricingSpider

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

print("="*60)
print("SYMBIO DATA ENGINE - PRICING SPIDER")
print("="*60)

# Run spider with all ScrapMonster materials
spider = PricingSpider(sources=["scrapmonster"], limit=20)
results = spider.run()

print("\n" + "="*60)
print("RESULTS")
print("="*60)
print(f"Raw prices collected: {results['raw_prices']}")
print(f"Unique materials: {results['aggregated_materials']}")
print(f"Errors: {results['errors']}")
print()

# Display prices in a nice table
print(f"{'Material':<30} {'$/lb':>10} {'$/ton':>12} {'Sources':>8}")
print("-"*60)

for material, data in sorted(results["prices"].items()):
    print(f"{material:<30} ${data['price_per_lb_usd']:>8.2f} ${data['price_per_ton_usd']:>10.2f} {data['source_count']:>8}")

print()
print("="*60)
print("These prices can now power your waste valuation reports!")
print("="*60)
