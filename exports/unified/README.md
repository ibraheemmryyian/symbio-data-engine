# Unified LLM Training Corpus

This directory contains the **unified export** combining all domains for LLM training.

## Purpose

Train custom AI models on 50 years of industrial waste and symbiosis data.

## Data Format

Combined JSONL with consistent schema:

```jsonl
{"type": "waste_listing", "text": "...", "structured": {...}}
{"type": "carbon_emission", "text": "...", "structured": {...}}
{"type": "symbiosis_exchange", "text": "...", "structured": {...}}
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| type | string | Record type (waste_listing, carbon_emission, symbiosis_exchange) |
| text | string | Original text for context |
| structured | object | Extracted structured data |
| source | string | Data source (wayback, epa, csr, etc.) |
| year | int | Year of the data |
| confidence | float | Extraction confidence score |

## Training Guidelines

### For Fine-Tuning

1. Filter by confidence > 0.7 for high-quality data
2. Balance record types for even coverage
3. Include both text and structured fields

### For RAG Applications

1. Use text field for embeddings
2. Store structured data as metadata
3. Filter by domain for specialized queries

## Statistics

Generated exports include:
- Document count per source
- Year distribution
- Material category breakdown
- Confidence score distribution

## File Naming

```
unified_YYYYMMDD.jsonl      # Full export with date
unified_latest.jsonl        # Symlink to most recent
unified_sample_1000.jsonl   # Sample for testing
```
