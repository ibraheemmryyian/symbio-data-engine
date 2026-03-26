
# SymbioFlows Data Engine
**Planetary-Scale Industrial Waste Intelligence & Symbiosis Marketplace**

> **License:** Proprietary (Showcase Only) | **Status:** V1.0 (Bridge Guard Verified)
>
> **What is SymbioFlows?** A global industrial symbiosis marketplace that transforms waste into profit by connecting waste generators, recyclers, and symbiosis partners with real pricing, verified availability, and AI-powered recommendations. This repository contains the **data intelligence engine** that powers it.

## What is Industrial Symbiosis?

Industrial symbiosis is the practice of exchanging waste, by-products, and energy between industries to create circular economy value. Instead of landfilling or incinerating waste, symbiosis enables:

- **Polymer Waste Recycling** — Polyethylene, polypropylene, and polyester waste streams from manufacturing become feedstock for new products
- **Solvent Recovery** — Used solvents from pharmaceuticals, coatings, and chemicals are recovered and reused, reducing disposal costs by 60-80%
- **Material Exchanges** — Industrial by-products (fly ash, slag, heat) flow from one facility to another, creating closed-loop material networks
- **Carbon Reduction** — Eliminating transportation to distant landfills and replacing virgin material extraction can reduce per-ton CO2 by 40-70%

SymbioFlows is the **real-time marketplace** where these exchanges happen. This engine provides the **AI-driven data backbone** — historical pricing, chemical profiles, waste composition analysis, and symbiosis opportunity scoring.

## Overview

The Symbio Data Engine is a **planetary-scale ETL (Extract-Transform-Load) pipeline** designed to ingest, normalize, and analyze industrial waste streams across 50+ years of historical data. It ingests raw data from government registries (EPA TRI, EU E-PRTR), corporate sustainability reports, and MENA industrial sources, processes them through **zero-hallucination LLM extraction**, validates every record against source citations, and outputs structured datasets for:

1. **SymbioFlows Marketplace** — Real-time waste availability, pricing, and symbiosis matching
2. **SymbioTrust Verification** — Fraud detection, carbon anomaly scoring, and material authenticity
3. **AI Training Corpora** — Behavioral cloning datasets for Llama 3 / Mistral fine-tuning on industrial data

## 📊 Capabilities & Data Scale

This engine has successfully indexed, normalized, and validated:

- **860,000+ Industrial Waste Listings** — Real material flows with quantity, quality grade, and disposal cost
- **38 Years of Historical Data** — 1987–2024 EPA TRI, EU E-PRTR, and corporate CSR archives
- **590 Unique Chemical Profiles** — From polyethylene scraps to spent solvents to specialty metals
- **1.37 Million Knowledge Graph Edges** — Generator → Material → Treatment → Buyer pathways
- **686,000+ Behavioral Cloning Pairs** — Fine-tuning datasets for waste classification and symbiosis scoring
- **Global Coverage** — EPA (US), E-PRTR (EU-27), MENA industrial registries, corporate sustainability reports

The data spans all major waste categories:
- **Organic Waste** — Food processing by-products, agricultural residues, fermentation slurries
- **Polymers & Plastics** — Polyethylene, polypropylene, polyester films, industrial scrap
- **Solvents & Chemicals** — Used solvents, paint waste, pharmaceutical residues, specialty chemicals
- **Metals & Rare Earths** — Scrap copper, aluminum, precious metal recovery streams, electronics recycling
- **Mineral & Construction** — Slag, fly ash, concrete waste, quarry spoils, soil remediation materials
- **Hazardous Waste** — Compliant handling with hazard codes, disposal routes, and regulatory frameworks

## 🏗️ Architecture & Data Pipeline

The engine is built on a **modular, fault-tolerant Python architecture** with the following pipeline stages:

```
Raw Data Sources → Spiders → Document Ingestion → Processors → Validation → PostgreSQL + ChromaDB → Export (JSONL/CSV)
```

### Pipeline Stages

**1. Data Ingestion (Spiders)**
- **EPA TRI Spider** — Ingests US EPA Toxics Release Inventory (1987–2024), 25,000+ chemical releases per year
- **E-PRTR Spider** — Crawls EU Pollutant Release and Transfer Register (2007–2024), 28,000+ facility records
- **Corporate CSR Spider** — Extracts sustainability reports from Fortune 500 companies, ISO certifications, and GRI disclosures
- **MENA Spider** — Indexes UAE, Saudi Arabia, Qatar industrial registries, environmental compliance data
- **Wayback Machine Spider** — Historical recovery of delisted facilities and legacy datasets via Internet Archive CDX API

All spiders include:
- **Rate Limiting** — Respectful crawling (configurable req/sec), user-agent rotation, exponential backoff
- **Content Deduplication** — Hash-based duplicate detection prevents re-processing of known records
- **Error Recovery** — Transient failures are retried; permanent failures are logged for manual review

**2. Document Processing (Processors)**
Raw documents are routed through specialized processors based on content type:
- **CSV Processor** — Direct column mapping for standardized government feeds (EPA TRI, E-PRTR)
- **PDF Processor** — Multi-stage extraction: PyPDF2 → pdfminer → Camelot/Tabula → Tesseract OCR (fallback chain)
- **LLM Extractor** — AI-assisted information extraction from unstructured text and images with **zero-hallucination guarantees**

**3. LLM Extraction with Zero-Hallucination Validation**
Every LLM extraction enforces:
- **Source Citation Requirement** — Every extracted field must cite a verbatim quote from the source document
- **Temperature=0.0** — Deterministic output prevents speculative or "creative" hallucinations
- **Pydantic Validation** — Structured schema enforcement with type coercion and range checks
- **Cross-Field Validation** — Carbon scope sums, material mass conservation, regulatory code compliance

Records without verifiable citations are **rejected**. This approach ensures the training data fed to downstream models is grounded in observable fact.

**4. Storage Layer (PostgreSQL + ChromaDB)**
- **PostgreSQL** — Normalized relational schema with `uuid-ossp` and `pg_trgm` extensions for full-text search
- **ChromaDB** — Vector embeddings for semantic similarity search (e.g., "find solvents similar to this waste stream")
- **ThreadedConnectionPool** — 1–10 concurrent connections, automatic failover on connection loss

**5. Export & Training Data**
- **JSONL Format** — One record per line, ready for LLM fine-tuning (Llama 3, Mistral, GPT)
- **CSV Export** — Tabular format for data warehouses and business intelligence tools
- **Behavioral Cloning Pairs** — Question-answer pairs for training waste classification models

### Module Breakdown

*   **`spiders/`**: Autonomous crawlers with content deduplication, rate limiting, and retry logic
*   **`processors/`**: Normalization logic, CSV column mapping, PDF extraction chains, LLM integration
*   **`store/`**: PostgreSQL CRUD operations, vector embeddings, connection pooling
*   **`config.py`**: Single source of truth for paths, credentials, spider settings, LLM config
*   **`main.py`**: CLI entry point for init, ingest, process, export, and status commands


## 🎯 Use Cases & Industry Applications

### Waste Generators (Supply Side)
Industrial facilities with waste streams (polymers, solvents, metals) use SymbioFlows to:
- **Find Buyers in Real Time** — Query available waste streams by material type, quantity, quality grade, and price
- **Reduce Disposal Costs** — Sell waste streams instead of paying landfill/incineration fees (typical savings: 40-60%)
- **Diversify Revenue** — Convert waste into a commodity product with active buyers
- **Track Compliance** — Verify hazard codes, transport routes, and regulatory approvals

### Waste Buyers & Recyclers (Demand Side)
Secondary processors and recyclers use SymbioFlows to:
- **Source Raw Materials** — Access 38 years of waste pricing data to forecast cost and negotiate better terms
- **Discover New Suppliers** — Find industrial facilities with compatible waste streams and quality grades
- **Optimize Supply Chains** — Match regional demand with nearby waste generators (reduce transport emissions)
- **Verify Material Authenticity** — SymbioTrust module screens for contamination, fraud, and anomalous carbon claims

### Enterprise Procurement & Sustainability Teams
Corporate buyers (automotive, packaging, consumer goods) use SymbioFlows to:
- **Meet Circular Economy Targets** — Track recycled content sourcing and close material loops
- **Cut Supply Chain Emissions** — Replace virgin material with certified recycled alternatives
- **Unlock ESG Disclosures** — Quantify waste diversion, material recovery, and CO2 avoidance for GRI/SASB/TCFD reporting
- **Negotiate Volume Discounts** — Historical pricing data enables informed purchasing decisions

## 📈 Data Quality & Validation

Every record in the SymbioFlows database is validated against:

1. **Time-Travel Logic** — No future dates, no illogical temporal sequences
2. **Mass Conservation** — No negative quantities, realistic facility-level volumes
3. **Referential Integrity** — All waste codes, facility IDs, and chemical CAS numbers match regulatory registries
4. **Source Attribution** — Every extracted field cites a verbatim quote from the original document
5. **Regulatory Compliance** — Hazard codes, disposal routes, and transport categories match EPA/ECHA frameworks
6. **Statistical Anomalies** — Carbon scope sums validated; outliers flagged for manual review

This rigorous approach ensures the training data powering SymbioFlows AI models is **grounded in reality**, not speculation.

## 🛡️ Security & Privacy

**This repository contains SOURCE CODE ONLY.** Proprietary data, pricing algorithms, and credentials are strictly git-ignored:
- Raw data exports (`exports/*.csv`) — Not included
- LLM extraction logic — Not exposed
- API credentials (`.env`) — Never committed
- Training datasets — Served directly to SymbioFlows, not stored in public repo

For the complete SymbioFlows experience (marketplace matching, pricing, symbiosis recommendations), visit **[SymbioFlows.com](https://symbioflows.com)**.

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.9+
- PostgreSQL 12+ with `uuid-ossp` and `pg_trgm` extensions
- Tesseract OCR (for PDF fallback chain)
- LM Studio or OpenAI-compatible API endpoint

### Quick Start

1.  **Clone the Repo:**
    ```bash
    git clone https://github.com/ibraheemmryyian/symbio-data-engine.git
    cd symbio-data-engine
    ```

2.  **Set Up Environment:**
    ```bash
    cp .env.example .env
    # Edit .env with your PostgreSQL, Chroma, and LLM credentials
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Initialize Database:**
    ```bash
    python main.py init
    ```

5.  **Run the Ingestion Pipeline:**
    ```bash
    # Ingest EPA TRI data (limit 100 records for testing)
    python main.py ingest all --source eprtr --limit 100

    # Process documents through LLM extraction
    python main.py process --source eprtr --batch-size 50

    # Continuous processing (night mode)
    python main.py process --continuous
    ```

6.  **Export Data:**
    ```bash
    python main.py export all --format jsonl
    python main.py export all --format csv
    ```

7.  **Check Pipeline Status:**
    ```bash
    python main.py status
    ```

## 🔍 Data Quality Audits

The system includes comprehensive verification tools:

- **`stress_test_master.py`** — Rigorous audits of time logic, mass conservation, and referential integrity
- **`audit_supabase_spec.py`** — Cross-validates data against Supabase schema
- **`verify_calculations.py`** — Confirms scope-sum calculations and statistical outliers

Run audits to verify data integrity:
```bash
python tests/stress_test_master.py
python tests/verify_calculations.py
```

## 🔗 Related Projects

- **[SymbioFlows](https://symbioflows.com)** — The industrial symbiosis marketplace powered by this engine
- **[SymbioTrust](https://symbioflows.com/trust)** — AI-powered verification and fraud detection
- **Behavioral Cloning Datasets** — Fine-tuning corpora for Llama 3 / Mistral on industrial waste classification

## 📄 License & Attribution

**License:** Proprietary (Source code available for evaluation and integration)

Built by **Ibraheem Mryyian** | Powering the Global Circular Economy | [SymbioFlows.com](https://symbioflows.com)
