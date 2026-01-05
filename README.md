# 🏭 Symbio Data Engine

**The Library of Alexandria for Industrial Symbiosis**

A comprehensive data pipeline for collecting, processing, and exporting industrial waste and symbiosis data (1978-present).

---

## 🎯 Purpose

1. **SymbioFlows Marketplace** — Real pricing and availability data for waste materials
2. **SymbioTrust Verification** — Fraud detection and carbon anomaly scoring
3. **AI Training Corpus** — 50 years of structured industrial data for LLM training
4. **Research Database** — Historical symbiosis exchange records

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Tesseract OCR (for PDF processing)

### Installation

```bash
# Clone and enter directory
cd symbio_data_engine

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Copy environment template
copy .env.example .env  # Windows
cp .env.example .env    # Linux/Mac

# Start databases
docker-compose up -d

# Initialize database schemas
python main.py init
```

---

## 📁 Architecture

```
symbio_data_engine/
├── spiders/              # Web crawlers
│   ├── base_spider.py    # Common logic (rate limiting, retries)
│   ├── wayback_spider.py # Wayback Machine (1978-2024)
│   ├── gov_spider.py     # EPA, E-PRTR government data
│   ├── csr_spider.py     # Corporate Sustainability Reports
│   └── scrap_exchange_spider.py  # Real-time pricing
├── processors/           # Data cleaning pipeline
│   ├── cleaner.py        # HTML strip, encoding fix
│   ├── normalizer.py     # Unit conversion, entity resolution
│   ├── extractor.py      # LLM-powered JSON extraction
│   └── pdf_processor.py  # OCR + table extraction
├── store/                # Database layer
│   ├── postgres.py       # PostgreSQL connection
│   ├── vectors.py        # ChromaDB integration
│   └── schemas.sql       # Table definitions
├── exports/              # LLM training data output
│   ├── symbioflows/
│   ├── symbiotrust/
│   ├── research/
│   └── unified/
├── agents/               # Autonomous processing
│   └── refinery_agent.py
├── config.py             # Configuration
├── main.py               # CLI entry point
└── docker-compose.yml    # Database containers
```

---

## 🔧 CLI Commands

```bash
# Ingest data from sources
python main.py ingest symbioflows --source wayback
python main.py ingest research --source gov --limit 1000

# Process raw documents
python main.py process --source all --batch-size 100

# Export for LLM training
python main.py export unified --format jsonl

# Check pipeline status
python main.py status --verbose

# Initialize databases
python main.py init
```

---

## 🗄️ Data Domains

| Domain | Description | Key Tables |
|--------|-------------|------------|
| **SymbioFlows** | Waste marketplace data | `waste_listings` |
| **SymbioTrust** | Carbon verification | `carbon_emissions`, `fraud_flags` |
| **Research** | Historical symbiosis | `symbiosis_exchanges` |
| **Unified** | Combined training corpus | All tables |

---

## 📡 Data Sources

| Source | Type | Priority |
|--------|------|----------|
| Kalundborg Symbiosis | Historical | 🔥 #1 |
| EPA TRI | Government | #2 |
| E-PRTR (Europe) | Government | #3 |
| Corporate CSR Reports | Corporate | #4 |
| Scrap Metal Exchanges | Real-time | #5 |

---

## 🐳 Docker Services

```bash
# Start all services
docker-compose up -d

# Start with admin UI (for development)
docker-compose --profile dev up -d

# View logs
docker-compose logs -f postgres

# Stop services
docker-compose down
```

**Services:**
- PostgreSQL: `localhost:5432`
- ChromaDB: `localhost:8000`
- Adminer (dev): `localhost:8080`

---

## 📊 Success Metrics

| Metric | Target (Dangerous) | Target (Untouchable) |
|--------|-------------------|---------------------|
| Facilities | 100+ | 500+ |
| Years | 10+ | 30+ |
| Materials | 50+ | 100+ |
| Transactions | 1,000+ | 10,000+ |
| Documents | 5,000+ | 50,000+ |

---

## 📄 License

Proprietary - Symbio Technologies
