# Industrial Byproducts Ingestion Pipeline

This document explains the SymbioFlows industrial byproduct ingestion system for steel slag, food waste, plastics, petcoke, and maritime waste.

## Quick Start

### Ingest World Steel data

```bash
python main.py ingest symbioflows --source worldsteel --limit 100
python main.py process --source worldsteel --batch-size 50
```

### Ingest FAO food waste data

```bash
python main.py ingest symbioflows --source fao --limit 200
python main.py process --source fao
```

### All industrial sources

```bash
python main.py ingest symbioflows --source worldsteel,fao,plastics,petcoke,maritime
python main.py process --continuous
```

## Supported Sources

| Source | Industry | Byproducts | Status |
|--------|----------|-----------|--------|
| `worldsteel` | Steel | Slag, EAF Dust, Mill Scale | Ready |
| `fao` | Food | Waste Pulp, Whey, Spent Grain | Ready |
| `plastics` | Plastics | HDPE/PET Scrap, Film | Ready |
| `petcoke` | Oil & Gas | Petroleum Coke, Refinery Sludge | Ready |
| `maritime` | Maritime | Dredged Sediments, Brine | Ready |

## Data Schema

Industrial byproducts are stored in the `waste_listings` table with these key fields:

- `material`: Byproduct name (e.g. "Steel Slag")
- `source_company`: Facility or region name
- `source_country`: ISO 2-letter country code (e.g. "US", "DE", "JP")
- `source_location`: City, region, or facility identifier
- `quantity_tons`: Annual production volume in metric tons
- `price_per_unit`: Market disposal/sale cost (USD/ton) - optional
- `year`: Reporting year
- `verification_method`: Data source or audit method (required)
- `source_quote`: Verbatim citation from source (required for citation rule)
- `extraction_confidence`: LLM confidence (0.0-1.0)

### Material Categories

Valid `material_category` values:

- `byproduct` - Secondary product from industrial process
- `waste` - Material requiring disposal
- `emission` - Gaseous or particulate emissions
- `energy` - Recoverable energy (e.g. waste heat)
- `water` - Process water or wastewater

### Supported Industries

| Industry | Examples |
|----------|----------|
| `steel` | Iron & steel mills, foundries |
| `food` | Food processing, agriculture |
| `plastics` | Plastics manufacturing, converters |
| `oil_gas` | Oil refining, petrochemicals |
| `chemical` | Chemical manufacturing |
| `pharma` | Pharmaceutical production |
| `maritime` | Ports, shipbreaking, fishing |
| `textiles` | Fabric and apparel production |
| `mining` | Mineral extraction |

## Zero Hallucination Policy

All records are validated against the Citation Rule:

1. Every extraction MUST include a `source_quote` field
2. The quote MUST be found verbatim in the source text
3. Records without valid citations are rejected
4. Extraction confidence must be at least 0.7 for acceptance

This ensures all data is traceable and verifiable.

## Symbiotic Matching

After ingestion, generate symbiotic routes to connect waste producers with receivers:

```bash
python scripts/improve_symbiotic_matching.py
```

Output: `exports/improved_symbiotic_matches.jsonl`

Routes include:
- Material source -> receiver compatibility (0.7-1.0 score)
- Geographic proximity factors
- Annual volume and pricing
- Environmental impact reduction (CO2 savings)

### Example Match

```json
{
  "source_company": "ArcelorMittal Gent",
  "source_country": "BE",
  "material": "Steel Slag",
  "quantity_tons": 275000,
  "receiver_company": "Cementir Holdings",
  "receiver_industry": "cement",
  "compatibility_score": 0.94,
  "co2_savings_tons": 41250,
  "market_price_per_ton": 18.50
}
```

## Data Quality Requirements

All records are validated for:

- Valid `source_quote` (minimum 10 characters, verbatim from source)
- `extraction_confidence` >= 0.7
- Real facility/company names (no "Unknown", "TBD", or generic terms)
- `source_country` must be valid ISO 3166-1 alpha-2 code
- `year` between 1970 and current year
- Quantity > 0 and <= 1 billion tons
- `verification_method` populated and descriptive

Records failing validation are logged and skipped. Review the logs to diagnose issues.

## CLI Reference

### Ingest Command

```bash
# Ingest from single source
python main.py ingest symbioflows --source worldsteel --limit 100

# Ingest from multiple sources
python main.py ingest symbioflows --source worldsteel,fao,plastics --limit 500

# Dry-run to preview without saving
python main.py ingest symbioflows --source worldsteel --dry-run
```

Options:
- `--limit N` - Maximum records to ingest (default: 1000)
- `--dry-run` - Preview without saving to database
- `--domain symbioflows` - Set ingestion domain

### Process Command

```bash
# Process with batch size
python main.py process --source worldsteel --batch-size 50

# Continuous processing (night mode)
python main.py process --continuous

# Process all pending
python main.py process --source all
```

Options:
- `--batch-size N` - Records per batch (default: 25)
- `--continuous` - Run until interrupted (for overnight jobs)
- `--source worldsteel|fao|plastics|petcoke|maritime|all`

### Status Command

```bash
# Get pipeline status
python main.py status

# Verbose status with record counts
python main.py status --verbose
```

### Export Command

```bash
# Export as JSONL (recommended for AI training)
python main.py export all --format jsonl

# Export as CSV
python main.py export all --format csv

# Export specific source
python main.py export worldsteel --format jsonl
```

## Troubleshooting

### No matches generated?

Verify the data pipeline is complete:

```sql
-- Check material reference data
SELECT COUNT(*) FROM materials;  -- should be > 0

-- Check ingested records
SELECT COUNT(*) FROM waste_listings 
WHERE source_industry IN ('steel', 'food', 'plastics', 'oil_gas', 'maritime');

-- Check for pending documents
SELECT COUNT(*) FROM documents WHERE status='pending';
```

### Records rejected during processing?

Check the application logs for validation errors:

- Verify CSV headers match expected column mappings
- Ensure `verification_method` is populated
- Check that company names are specific (not "Unknown" or "TBD")
- Confirm `source_quote` exists in source text

### Import speed is slow?

Optimize performance:

```bash
# Use larger batch size (requires more memory)
python main.py process --batch-size 100

# Use continuous mode for overnight processing
python main.py process --source worldsteel --continuous

# Check database connection pool
# Verify POSTGRES_HOST and POSTGRES_PORT in .env
```

### High rejection rate?

Debug extraction issues:

```bash
# Run with verbose logging
export LOG_LEVEL=DEBUG
python main.py process --source worldsteel --batch-size 5

# Check a specific document
SELECT * FROM documents WHERE id='<doc-id>' LIMIT 1;
SELECT * FROM documents WHERE status='failed' LIMIT 5;
```

## Data Pipeline Architecture

```
Spiders (worldsteel, fao, etc)
    |
    v
documents table (raw CSV/PDF/HTML)
    |
    v
processors/gov_processor.py (column mapping, CSV parsing)
    |
    v
processors/extractor.py (LLM extraction with zero-hallucination)
    |
    v
processors/models.py (Pydantic validation)
    |
    v
waste_listings table (validated records)
    |
    v
scripts/improve_symbiotic_matching.py
    |
    v
improved_symbiotic_matches.jsonl (for AI/matching engine)
```

## Performance Expectations

- Ingestion: 100-500 records/second (depends on CSV size)
- Processing: 10-50 records/second (LLM extraction)
- Matching: 1000+ routes/second

For optimal performance:
- Run processing with `--continuous` overnight
- Use `--batch-size 50-100` on modern hardware
- Ensure PostgreSQL and ChromaDB are on local network

## Testing

Run end-to-end tests:

```bash
pytest tests/test_industrial_ingestion_e2e.py -v
```

Tests verify:
- CSV parsing from all sources
- Pydantic model validation
- Citation rule enforcement
- Company name normalization
- Industry and category constraints

## Examples

### Ingest and process World Steel data

```bash
# Ingest 100 world steel records
python main.py ingest symbioflows --source worldsteel --limit 100

# Process them (extract quantities, facilities, etc)
python main.py process --source worldsteel --batch-size 50

# Export to JSONL for training/analysis
python main.py export worldsteel --format jsonl > exports/worldsteel_2024.jsonl

# Generate symbiotic matches
python scripts/improve_symbiotic_matching.py
```

### Monitor progress

```bash
# Check pipeline status
python main.py status --verbose

# Watch for errors
tail -f logs/symbio_data_engine.log | grep -i "error\|rejected"
```

## References

- World Steel Association: https://www.worldsteel.org/
- FAO Food Loss & Waste Database: https://www.fao.org/platform-food-loss-waste
- PlasticsEurope Market Data: https://www.plasticseurope.org/
- IEA Petcoke Database: https://www.iea.org/
- IMO Maritime Waste Guidelines: https://www.imo.org/

## Support

For issues or questions:

1. Check troubleshooting section above
2. Review logs: `logs/symbio_data_engine.log`
3. Verify environment: `python config.py`
4. Test validation: `pytest tests/test_industrial_ingestion_e2e.py -v`
