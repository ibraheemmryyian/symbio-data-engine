
# 🌍 Symbio Data Engine
**Planetary-Scale Industrial Waste Intelligence**

> **License:** Proprietary (Showcase Only) | **Status:** V1.0 (Bridge Guard Verified)

## Overview
The Symbio Data Engine is an autonomous ETL pipeline designed to ingest, normalize, and analyze industrial waste streams at a global scale. It powers the "Symbio" platform, enabling AI-driven matchmaking between waste generators and recyclers.

This repository contains the **Engine Core**—the spiders, processors, and AI training pipelines that built the dataset.

**⚠️ NOTE:** The underlying data (860,000+ records) is proprietary and is **not included** in this public repository.

## 🏗️ Architecture
The engine is built on a modular Python architecture:
*   **`spiders/`**: Autonomous crawlers for scraping E-PRTR, EPA, and specialized MENA sources.
*   **`processors/`**: Normalization logic connecting raw CSV/PDF data to the Unified Schema.
*   **`store/`**: PostgreSQL interface with `uuid-ossp` and `pg_trgm` extensions for high-speed ingestion.
*   **`scripts/ingestion`**: Specific ETL workflows (e.g., `prepare_training_data.py`).
*   **`scripts/verification`**: "Bridge Guard" audit tools (e.g., `stress_test_master.py`).

## 📊 Capabilities
This engine has successfully processed:
*   **Volumetrics:** 860,000+ Industrial Waste Listings.
*   **Temporal Depth:** 38 Years of Historical Data (1987-2024).
*   **Graph Intelligence:** 1.3 Million Knowledge Graph Edges (Generator -> Material -> Treatment).
*   **AI Readiness:** Generates fine-tuning datasets (`.jsonl`) for Llama 3 / Mistral training.

## 🛠️ Setup & Installation
1.  **Clone the Repo:**
    ```bash
    git clone https://github.com/ibraheemmryyian/symbio-data-engine.git
    cd symbio-data-engine
    ```
2.  **Environment:**
    ```bash
    cp .env.example .env
    # Add your PostgreSQL credentials and API keys
    ```
3.  **Run the Pipeline:**
    ```bash
    # Start the "Night Watch" autonomous loop
    python main.py process --continuous
    ```

## 🛡️ Verification
The system includes `stress_test_master.py`, a rigorous audit script that verifies:
*   Time Travel Logic (No future dates).
*   Mass Conservation (No negative tons).
*   Referential Integrity (No orphan records).

---
*Built by Ibraheem Mryyian. Powering the Circular Economy.*
