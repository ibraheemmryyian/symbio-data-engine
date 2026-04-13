# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Symbio Data Engine is a planetary-scale industrial waste intelligence ETL pipeline. It ingests data from EU E-PRTR, US EPA TRI, MENA government sources, and corporate CSR reports, processes them through an LLM-assisted extraction pipeline, and stores results in PostgreSQL + ChromaDB for AI training datasets and waste symbiosis matching.

## Environment Setup

Copy `.env.example` to `.env`. Required variables:

```
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
CHROMA_HOST, CHROMA_PORT
LLM_API_KEY, LLM_MODEL, LLM_BASE_URL   # See "LLM Configuration" below
TESSERACT_CMD                            # Windows: C:\Program Files\Tesseract-OCR\tesseract.exe
SUPABASE_URL, SUPABASE_KEY              # For ingest_to_supabase.py only
```

### LLM Configuration

Two extraction methods are available:

**Local LLM (LM Studio)** — Recommended for privacy and cost:
- Set `LLM_BASE_URL=http://localhost:1234/v1` and `LLM_API_KEY=lm-studio`
- Download and run [LM Studio](https://lmstudio.ai/)
- Load model: `qwen2.5-3b-instruct` (128k context, fits entirely in VRAM)
- **Important:** LM Studio must be running before starting `python main.py process`
- Processor automatically uses local extraction unless `--use-cloud` is passed

**Cloud API (OpenAI-compatible)** — Use `--use-cloud` flag:
- Set `LLM_BASE_URL=https://api.openai.com/v1` and `LLM_API_KEY=sk-...`
- OR point to any OpenAI-compatible endpoint (Anthropic, local inference server, etc.)
- Command: `python main.py process --source all --use-cloud`
- Slower but no local GPU needed

## Common Commands

```bash
# Initialize database schemas and directories
python main.py init

# Run spiders (domain: symbioflows | symbiotrust | research | all)
# Government data sources: wayback, gov (EPA TRI), eprtr (EU), csr, mena
# Industrial sources: worldsteel, fao, plastics, petcoke, maritime
python main.py ingest all --source eprtr --limit 100
python main.py ingest all --source csr --dry-run
python main.py ingest all --source worldsteel --limit 500

# Run processing pipeline
python main.py process --source all --batch-size 50
python main.py process --source eprtr --continuous   # night mode
python main.py process --source all --use-cloud      # use OpenAI instead of local LLM
python main.py process --source all --reprocess      # reprocess already-completed docs

# Export data
python main.py export symbioflows --format jsonl
python main.py export research --format csv

# Check pipeline status
python main.py status

# Bulk ingest CSV to Supabase
python ingest_to_supabase.py   # reads exports/symbio_data_engine_READY.csv
```

## Architecture

### Data Flow

```
Spiders → documents table (raw) → Processors → waste_listings / carbon_emissions / symbiosis_exchanges
```

1. **Spiders** (`spiders/`) crawl sources and insert raw documents into postgres with `status=pending`
2. **`processors/__init__.py:run_pipeline()`** fetches pending documents and routes by type:
   - CSV → `gov_processor.py` (EPA TRI column mapping, E-PRTR column mapping)
   - PDF → `pdf_processor.py` (PyPDF2 → pdfminer → Camelot/Tabula → Tesseract fallback chain)
   - Text/HTML → `cleaner.py` → `normalizer.py` → `extractor.py` (LLM JSON extraction)
3. **Store** (`store/postgres.py`) upserts results; `store/vectors.py` handles ChromaDB embeddings

### Zero Hallucination Policy

Every LLM extraction (`processors/extractor.py`) uses temperature=0.0 and requires a `source_quote` field that must be found verbatim in the source text. Validation happens in `processors/models.py` via Pydantic. Records without verifiable citations are rejected. The `verification_method` field (DQ-001) is required on `WasteListingExtraction`.

### Key Models (`processors/models.py`)

All extraction records extend `CitedRecord` which enforces mandatory `source_quote` (verbatim text from document):

- **`WasteListingExtraction`** — material, quantity_tons, price_per_ton, quality_grade (A/B/C/contaminated), year, verification_method (DQ-001 required)
- **`CarbonEmissionExtraction`** — co2_tons by scope (1/2/3), with scope-sum cross-validation
- **`SymbiosisExchangeExtraction`** — source→target company material exchange with co2_savings quantification
- **`IndustrialByproductExtraction`** — Special model for industrial sources; includes industry, source_country, receiver_industry, extraction_confidence

Extraction results are wrapped in `ExtractionResult` with `record_type` and `is_valid` flag. Results flow to database insert functions based on type:
- `waste_listing` → `insert_waste_listing()`
- `carbon_emission` → `insert_carbon_emission()`
- `symbiosis_exchange` → `insert_symbiosis_exchange()`
- `industrial_byproduct` → `insert_industrial_byproduct()`

### Spider Registry (`spiders/__init__.py`)

`run_spider(domain, source, limit)` maps source names to spider classes:
- `wayback` → WaybackSpider (Internet Archive CDX API, historical Kalundborg data)
- `gov` → GovSpider (EPA TRI bulk CSVs 1987–2024, E-PRTR 2007–2024)
- `csr` → CSRSpider (corporate sustainability PDFs)
- `mena` → MENASpider (UAE/Saudi industrial sources)
- `eprtr` → EPRTRSpider (EU pollutant release register)

All spiders extend `BaseSpider` which handles rate limiting (configurable req/sec), user agent rotation, retry with exponential backoff, and content-hash deduplication.

### Processor Routing (`processors/__init__.py`)

`run_pipeline()` handles routing and reprocessing:
- **CSV + industrial sources** (worldsteel, fao, plastics, petcoke, maritime) → `gov_processor.process_csv_industrial()` with source-specific column mapping
- **CSV + government sources** (epa_tri, eprtr, mena, saudi) → `gov_processor.process_csv()` with government-standard mappings
- **PDF documents** → `pdf_processor.extract_text()` → `normalizer.normalize()` → LLM extraction
- **HTML/Text** → `cleaner.clean()` → `normalizer.normalize()` → LLM extraction

**Reprocessing:** Pass `--reprocess` to re-run already-completed documents. Useful when extraction logic changes or LLM model is upgraded.

### Database (`store/postgres.py`)

ThreadedConnectionPool (1–10 conns). Key helpers:
- `insert_document()` — upserts by content_hash, returns UUID
- `update_document_status()` — pending → completed / completed_no_data / failed
- `insert_waste_listing()` — upserts on (document_id, material)
- `insert_industrial_byproduct()` — special pipeline for industrial sources with industry/receiver_industry fields
- `VALID_COLUMNS` whitelist on all insert functions prevents column injection

### LLM Extraction Strategies (`processors/local_extractor.py` vs `processors/extractor.py`)

**LocalExtractor** (default, used when `--use-cloud` is NOT passed):
- Feeds entire document into LLM as single prompt (up to ~100k tokens for Qwen 3B)
- No chunking—faster for small/medium documents
- Post-extracts with regex validators for quantities, years, CO2, CAS numbers, prices
- Returns JSON with field-level confidence scores
- Requires LM Studio running on `http://localhost:1234/v1`

**Extractor** (cloud API, used with `--use-cloud` flag):
- Chunks documents into 8k-token segments
- Calls OpenAI-compatible API per chunk
- Merges results across chunks
- Slower but works without local GPU
- Useful for very large documents (>50k tokens)

Both use temperature=0.0 and enforce zero-hallucination via `source_quote` requirement.

### Configuration (`config.py`)

Single source of truth for all paths, DB credentials, spider settings, LLM config, and export formats. Reads from environment via `python-dotenv`. All data directories are auto-created on import.

## Data Scale

- 860,000+ waste listings, 38 years (1987–2024)
- 590 unique chemical profiles
- 1.37M knowledge graph edges
- 686K behavioral cloning pairings (for Llama 3/Mistral fine-tuning)
- Export target: `.jsonl` format for LLM training datasets

## Troubleshooting

**"LLM call failed: No connection could be made"**
- LM Studio is not running or not listening on the configured port
- Check: Is LM Studio open? Is `LLM_BASE_URL=http://localhost:1234/v1` correct?
- Fix: Start LM Studio, load the model, verify it's accepting connections
- Workaround: Use `--use-cloud` to switch to OpenAI API instead

**"Pending documents stuck in processing"**
- Run `python main.py status` to see breakdown: completed vs completed_no_data vs failed
- Check `main.log` for extraction errors (file not found, parse errors, LLM timeouts)
- `completed_no_data` usually means LLM extraction found nothing—check if LLM is running

**"File path errors on Windows"**
- Use forward slashes in `.env` paths: `C:/Program Files/...` not `C:\Program Files\...`
- Or use raw strings: `TESSERACT_CMD=r"C:\Program Files\Tesseract-OCR\tesseract.exe"`
- Python handles both; avoid backslash confusion in environment files

**"PostgreSQL connection refused"**
- Check: Is PostgreSQL running? `psql -h localhost -U symbio -d symbio_data`
- Verify `.env` credentials match your PostgreSQL user/password
- Check `POSTGRES_HOST` — typically `localhost` for local dev

## Tests

`tests/` contains data quality verification scripts (not pytest), run directly:
```bash
python tests/verify_financial_matrix.py
python tests/simulate_correct_financials.py
```

## Data Organization

Raw documents are stored by source in `data/raw/<source>/`:
```
data/raw/
  wayback/         # HTML from Internet Archive
  gov/             # EPA TRI / E-PRTR CSVs
  csr/             # Corporate sustainability PDFs
  mena/            # MENA government sources
  eprtr/           # EU E-PRTR bulk data
  worldsteel/      # Industrial byproduct CSVs
  fao/             # Agricultural data CSVs
  plastics/        # Plastics exchange CSVs
  petcoke/         # Petroleum coke data
  maritime/        # Maritime shipping data
```

Documents inherit `source` field from directory name, which determines processor routing in `run_pipeline()`.
