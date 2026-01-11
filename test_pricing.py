#!/usr/bin/env python3
"""Quick test of pricing spider."""
from spiders.pricing_spider import PricingSpider

spider = PricingSpider(sources=["scrapmonster"], limit=2)
results = spider.run()

print("="*50)
print(f"Raw prices: {results['raw_prices']}")
print(f"Aggregated: {results['aggregated_materials']}")
print(f"Errors: {results['errors']}")
print()
for m, d in list(results["prices"].items()):
    print(f"{m}: ${d['price_per_lb_usd']}/lb (${d['price_per_ton_usd']}/ton)")
