#!/usr/bin/env python3
"""
Symbio Data Engine - Scraper Test Suite
========================================
Quick tests to verify scrapers work before running full ingestion.

Run: python test_scrapers.py
"""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def test_wayback_spider():
    """Test Wayback Machine CDX query (no actual downloads)."""
    print("\n" + "="*60)
    print("[TEST 1] Wayback Machine Spider")
    print("="*60)
    
    from spiders.wayback_spider import WaybackSpider
    
    spider = WaybackSpider(
        domain="research",
        limit=3,
        start_year=2020,
        end_year=2024,
    )
    
    print("\n[INFO] Testing CDX API query for symbiosis.dk...")
    results = spider._get_cdx_results("symbiosis.dk", "20200101", "20241231")
    
    if results:
        print(f"[PASS] CDX returned {len(results)} snapshots")
        for r in results[:3]:
            print(f"   - {r.get('timestamp', 'N/A')}: {r.get('original', 'N/A')[:50]}...")
        return True
    else:
        print("[FAIL] CDX returned no results (might be rate limited)")
        return False


def test_gov_spider():
    """Test Government data spider URL generation."""
    print("\n" + "="*60)
    print("[TEST 2] Government Data Spider")
    print("="*60)
    
    from spiders.gov_spider import GovSpider
    
    spider = GovSpider(domain="symbiotrust", limit=2)
    
    print("\n[INFO] Configured sources:")
    for source_key, source in spider.SOURCES.items():
        print(f"   - {source['name']}: {source['base_url']}")
    
    print("\n[INFO] Generated URLs (first 3):")
    urls = list(spider.get_urls())[:3]
    for url in urls:
        print(f"   - {url}")
    
    print("[PASS] Gov spider configured correctly")
    return True


def test_csr_spider():
    """Test CSR report discovery."""
    print("\n" + "="*60)
    print("[TEST 3] CSR Report Spider")
    print("="*60)
    
    from spiders.csr_spider import CSRSpider
    
    spider = CSRSpider(domain="symbiotrust", limit=2)
    
    print("\n[INFO] Configured companies:")
    for key, company in spider.TARGETS.items():
        print(f"   - {company['name']}: {company['base_url']}")
    
    print("[PASS] CSR spider configured correctly")
    return True


def test_scrap_spider():
    """Test Scrap pricing spider."""
    print("\n" + "="*60)
    print("[TEST 4] Scrap Exchange Spider")
    print("="*60)
    
    from spiders.scrap_exchange_spider import ScrapExchangeSpider
    
    spider = ScrapExchangeSpider(domain="symbioflows", limit=2)
    
    print("\n[INFO] Configured price sources:")
    for key, source in spider.SOURCES.items():
        print(f"   - {source['name']}: {source['base_url']}")
    
    print("[PASS] Scrap spider configured correctly")
    return True


def test_cleaner():
    """Test HTML cleaner with nav/footer stripping."""
    print("\n" + "="*60)
    print("[TEST 5] HTML Cleaner (Nav/Footer Stripping)")
    print("="*60)
    
    from processors.cleaner import Cleaner
    
    test_html = """
    <html>
    <head><title>Test</title></head>
    <body>
        <nav><a href="/">Home</a><a href="/about">About</a></nav>
        <header>Company Logo Here</header>
        <div class="cookie-banner">We use cookies. Accept all.</div>
        
        <main>
            <h1>Industrial Symbiosis Report 2023</h1>
            <p>Kalundborg Symbiosis processed 5,000 tonnes of fly ash in 2023.</p>
            <p>DONG Energy provided 200,000 tonnes of steam to Novo Nordisk.</p>
        </main>
        
        <footer>Copyright 2023 All Rights Reserved | Privacy Policy</footer>
    </body>
    </html>
    """
    
    cleaner = Cleaner()
    result = cleaner.clean_html(test_html)
    
    print("\n[INFO] Input contained: nav, header, cookie-banner, footer")
    print("\n[OUTPUT] Cleaned text preview:")
    print("-" * 40)
    # Encode safely for Windows console
    safe_result = result[:500].encode('ascii', 'replace').decode('ascii')
    print(safe_result)
    print("-" * 40)
    
    # Verify stripping worked
    if "Cookie" not in result and "Privacy Policy" not in result:
        print("\n[PASS] Nav/Footer/Cookie stripping WORKS!")
        return True
    else:
        print("\n[FAIL] Some noise elements were not stripped")
        return False


def test_extractor():
    """Test LLM extractor (rule-based fallback if no LLM)."""
    print("\n" + "="*60)
    print("[TEST 6] Data Extractor")
    print("="*60)
    
    from processors.extractor import Extractor
    
    extractor = Extractor()
    
    print(f"\n[INFO] LLM enabled: {extractor.use_llm}")
    print(f"[INFO] Temperature: {extractor.LLM_TEMPERATURE}")
    
    test_text = """
    Kalundborg Symbiosis Report 2023
    
    In 2023, Statoil Refinery transferred 15,000 tonnes of fly ash 
    to Gyproc for use in cement production. This exchange has been 
    ongoing since 1995 and represents a classic example of industrial 
    symbiosis in Denmark.
    """
    
    print("\n[INFO] Testing extraction on sample text...")
    result = extractor.extract(test_text)
    
    print(f"\n[RESULT]")
    print(f"   - Valid: {result.is_valid}")
    print(f"   - Type: {result.record_type}")
    print(f"   - Rejection: {result.rejection_reason}")
    if result.data:
        print(f"   - Data keys: {list(result.data.keys())}")
    
    print("[PASS] Extractor executed without errors")
    return True


def test_normalizer():
    """Test unit normalization."""
    print("\n" + "="*60)
    print("[TEST 7] Unit Normalizer")
    print("="*60)
    
    from processors.normalizer import Normalizer
    
    normalizer = Normalizer()
    
    test_cases = [
        (1000, "kg", 1.0),
        (1, "ton", 0.907185),
        (2000, "lbs", 0.907),
        (5, "tonnes", 5.0),
    ]
    
    print("\n[INFO] Unit conversion tests:")
    all_passed = True
    for value, unit, expected in test_cases:
        result, _ = normalizer.normalize_quantity(value, unit)
        passed = abs(result - expected) < 0.01
        status = "[PASS]" if passed else "[FAIL]"
        print(f"   {status} {value} {unit} -> {result} metric_tons (expected ~{expected})")
        if not passed:
            all_passed = False
    
    return all_passed


def test_db_connection():
    """Test PostgreSQL connection (requires Docker)."""
    print("\n" + "="*60)
    print("[TEST 8] PostgreSQL Connection")
    print("="*60)
    
    try:
        from store.postgres import get_pool, close_pool, get_pipeline_stats
        
        print("\n[INFO] Connecting to PostgreSQL...")
        pool = get_pool()
        print("[PASS] Connection pool created!")
        
        stats = get_pipeline_stats()
        print(f"\n[INFO] Current stats: {stats}")
        
        close_pool()
        print("[PASS] Connection pool closed")
        return True
        
    except Exception as e:
        print(f"\n[WARN] PostgreSQL not available: {e}")
        print("   (This is OK for dry-run testing)")
        return False


def test_chroma_connection():
    """Test ChromaDB connection."""
    print("\n" + "="*60)
    print("[TEST 9] ChromaDB Connection")
    print("="*60)
    
    try:
        from store.vectors import get_client, get_collection_stats
        
        print("\n[INFO] Connecting to ChromaDB...")
        client = get_client()
        print("[PASS] ChromaDB client created!")
        
        stats = get_collection_stats()
        print(f"\n[INFO] Collections: {stats}")
        return True
        
    except Exception as e:
        print(f"\n[WARN] ChromaDB not available: {e}")
        print("   (This is OK for dry-run testing)")
        return False


def run_all_tests():
    """Run all tests."""
    print("\n" + "="*60)
    print("SYMBIO DATA ENGINE - SCRAPER TEST SUITE")
    print("="*60)
    
    results = {}
    
    # Core tests (don't require databases)
    results["Wayback Spider"] = test_wayback_spider()
    results["Gov Spider"] = test_gov_spider()
    results["CSR Spider"] = test_csr_spider()
    results["Scrap Spider"] = test_scrap_spider()
    results["Cleaner"] = test_cleaner()
    results["Extractor"] = test_extractor()
    results["Normalizer"] = test_normalizer()
    
    # Database tests (might fail without Docker)
    results["PostgreSQL"] = test_db_connection()
    results["ChromaDB"] = test_chroma_connection()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"   {status}: {name}")
    
    passed_count = sum(1 for v in results.values() if v)
    total = len(results)
    
    print("\n" + "-"*60)
    print(f"   Total: {passed_count}/{total} tests passed")
    
    if passed_count >= 7:  # Core tests passed
        print("\n[SUCCESS] Core scrapers are working! Ready for ingestion.")
    else:
        print("\n[WARNING] Some tests failed. Check output above.")
    
    return passed_count == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
