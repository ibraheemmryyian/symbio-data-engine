-- ============================================
-- SYMBIO DATA ENGINE - Database Schemas
-- ============================================
-- The Library of Alexandria for Industrial Symbiosis
-- ============================================

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy text search

-- ============================================
-- CORE: Document Tracking
-- ============================================

-- Raw documents ingested from sources
CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source VARCHAR(50) NOT NULL,           -- wayback, epa, eprtr, csr, scrap
    source_url TEXT NOT NULL,
    wayback_timestamp VARCHAR(14),          -- For Wayback Machine URLs
    document_type VARCHAR(50),              -- pdf, html, csv, json
    file_path TEXT,                         -- Local storage path
    content_hash VARCHAR(64) UNIQUE,        -- SHA-256 for deduplication
    size_bytes BIGINT,
    status VARCHAR(20) DEFAULT 'pending',   -- pending, processing, completed, failed
    error_message TEXT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_documents_source ON documents(source);
CREATE INDEX idx_documents_status ON documents(status);
CREATE INDEX idx_documents_hash ON documents(content_hash);

-- Extracted raw data before normalization
CREATE TABLE IF NOT EXISTS raw_extractions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    extraction_type VARCHAR(50),            -- text, table, metadata
    raw_content TEXT,
    structured_data JSONB,
    extraction_method VARCHAR(50),          -- pypdf, camelot, tesseract, llm
    confidence DECIMAL(3,2),
    extracted_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_extractions_document ON raw_extractions(document_id);

-- ============================================
-- SYMBIOFLOWS: Waste Marketplace
-- ============================================

CREATE TABLE IF NOT EXISTS waste_listings (
    id SERIAL PRIMARY KEY,
    document_id UUID REFERENCES documents(id),
    
    -- Material identification
    material VARCHAR(100) NOT NULL,
    material_category VARCHAR(50),          -- metals, plastics, organics, chemicals, etc.
    material_subcategory VARCHAR(50),
    cas_number VARCHAR(20),                 -- Chemical Abstracts Service number
    
    -- Quantity and pricing
    quantity_tons DECIMAL(15,2),
    quantity_unit VARCHAR(20) DEFAULT 'metric_tons',
    price_per_ton DECIMAL(10,2),
    currency VARCHAR(3) DEFAULT 'USD',
    price_type VARCHAR(20),                 -- spot, contract, average
    
    -- Source information
    source_company VARCHAR(255),
    source_industry VARCHAR(100),
    source_location VARCHAR(255),
    source_country VARCHAR(3),              -- ISO 3166-1 alpha-3
    source_coordinates POINT,
    
    -- Quality and availability
    quality_grade VARCHAR(20),              -- A, B, C, contaminated
    purity_percentage DECIMAL(5,2),
    availability_status VARCHAR(20),        -- available, reserved, sold
    listing_date DATE,
    expiry_date DATE,
    
    -- Extraction metadata
    extraction_confidence DECIMAL(3,2),
    data_source_url TEXT,
    year INTEGER,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_waste_material ON waste_listings(material);
CREATE INDEX idx_waste_category ON waste_listings(material_category);
CREATE INDEX idx_waste_company ON waste_listings(source_company);
CREATE INDEX idx_waste_year ON waste_listings(year);

-- 🛡️ UNIQUE constraint for UPSERT support (Updated for CSV listings)
CREATE UNIQUE INDEX idx_waste_listing_granular 
    ON waste_listings(document_id, material, source_company, year, quantity_tons) 
    WHERE document_id IS NOT NULL AND material IS NOT NULL;

-- ============================================
-- SYMBIOTRUST: Carbon Verification
-- ============================================

CREATE TABLE IF NOT EXISTS carbon_emissions (
    id SERIAL PRIMARY KEY,
    document_id UUID REFERENCES documents(id),
    
    -- Company identification
    company VARCHAR(255) NOT NULL,
    company_normalized VARCHAR(255),        -- Cleaned/matched name
    facility VARCHAR(255),
    industry VARCHAR(100),
    country VARCHAR(3),
    
    -- Emissions data
    year INTEGER NOT NULL,
    co2_tons DECIMAL(15,2),
    co2_scope1 DECIMAL(15,2),               -- Direct emissions
    co2_scope2 DECIMAL(15,2),               -- Indirect (energy)
    co2_scope3 DECIMAL(15,2),               -- Value chain
    co2_avoided_tons DECIMAL(15,2),         -- Through circular economy
    
    -- Methodology
    methodology VARCHAR(100),               -- GHG Protocol, ISO 14064, etc.
    verification_body VARCHAR(255),
    verification_date DATE,
    
    -- Trust scoring
    anomaly_score DECIMAL(3,2),             -- 0-1, higher = more suspicious
    verified BOOLEAN DEFAULT FALSE,
    verification_notes TEXT,
    
    -- Extraction metadata
    extraction_confidence DECIMAL(3,2),
    data_source_url TEXT,
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_carbon_company ON carbon_emissions(company);
CREATE INDEX idx_carbon_year ON carbon_emissions(year);
CREATE INDEX idx_carbon_anomaly ON carbon_emissions(anomaly_score);

-- 🛡️ UNIQUE constraint for UPSERT support
CREATE UNIQUE INDEX idx_carbon_unique_company_year 
    ON carbon_emissions(company, year) 
    WHERE company IS NOT NULL AND year IS NOT NULL;

-- Fraud detection flags
CREATE TABLE IF NOT EXISTS fraud_flags (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER,                      -- Reference to any entity
    entity_type VARCHAR(50) NOT NULL,       -- carbon_emission, waste_listing, company
    
    flag_type VARCHAR(100) NOT NULL,        -- impossible_reduction, data_inconsistency, etc.
    severity VARCHAR(20) NOT NULL,          -- low, medium, high, critical
    description TEXT,
    
    -- Detection details
    detection_method VARCHAR(50),           -- statistical, rule_based, ml_model
    detection_model_version VARCHAR(20),
    confidence DECIMAL(3,2),
    
    -- Resolution
    status VARCHAR(20) DEFAULT 'open',      -- open, investigating, resolved, false_positive
    resolution_notes TEXT,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    
    detected_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_fraud_entity ON fraud_flags(entity_type, entity_id);
CREATE INDEX idx_fraud_severity ON fraud_flags(severity);
CREATE INDEX idx_fraud_status ON fraud_flags(status);

-- ============================================
-- RESEARCH: Historical Symbiosis
-- ============================================

CREATE TABLE IF NOT EXISTS symbiosis_exchanges (
    id SERIAL PRIMARY KEY,
    document_id UUID REFERENCES documents(id),
    
    -- Exchange identification
    year INTEGER NOT NULL,
    eco_park VARCHAR(100),                  -- Kalundborg, Ulsan, etc.
    park_country VARCHAR(3),
    
    -- Parties
    source_company VARCHAR(255),
    source_company_normalized VARCHAR(255),
    source_industry VARCHAR(100),
    target_company VARCHAR(255),
    target_company_normalized VARCHAR(255),
    target_industry VARCHAR(100),
    
    -- Material flow
    material VARCHAR(100),
    material_category VARCHAR(50),
    volume_tons DECIMAL(15,2),
    volume_unit VARCHAR(20) DEFAULT 'metric_tons',
    exchange_type VARCHAR(50),              -- waste, byproduct, energy, water
    
    -- Value
    economic_value DECIMAL(15,2),
    currency VARCHAR(3),
    co2_savings_tons DECIMAL(15,2),
    
    -- Metadata
    exchange_started_year INTEGER,
    exchange_ended_year INTEGER,            -- NULL if ongoing
    notes TEXT,
    
    -- Extraction metadata
    extraction_confidence DECIMAL(3,2),
    data_source_url TEXT,
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_symbiosis_year ON symbiosis_exchanges(year);
CREATE INDEX idx_symbiosis_park ON symbiosis_exchanges(eco_park);
CREATE INDEX idx_symbiosis_material ON symbiosis_exchanges(material);
CREATE INDEX idx_symbiosis_source ON symbiosis_exchanges(source_company);
CREATE INDEX idx_symbiosis_target ON symbiosis_exchanges(target_company);

-- 🛡️ UNIQUE constraint for UPSERT support
CREATE UNIQUE INDEX idx_symbiosis_unique_exchange 
    ON symbiosis_exchanges(source_company, target_company, material, year) 
    WHERE source_company IS NOT NULL AND target_company IS NOT NULL;

-- ============================================
-- ENTITY RESOLUTION
-- ============================================

-- Canonical company names for deduplication
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    canonical_name VARCHAR(255) NOT NULL UNIQUE,
    aliases TEXT[],                         -- Array of known aliases
    industry VARCHAR(100),
    country VARCHAR(3),
    website VARCHAR(255),
    founded_year INTEGER,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_companies_name ON companies USING gin(canonical_name gin_trgm_ops);

-- ============================================
-- PIPELINE TRACKING
-- ============================================

-- Track pipeline runs for monitoring
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_type VARCHAR(50) NOT NULL,     -- ingest, process, export
    domain VARCHAR(50),                     -- symbioflows, symbiotrust, research
    source VARCHAR(50),
    
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',   -- running, completed, failed
    
    documents_processed INTEGER DEFAULT 0,
    documents_failed INTEGER DEFAULT 0,
    
    error_message TEXT,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_pipeline_status ON pipeline_runs(status);
CREATE INDEX idx_pipeline_type ON pipeline_runs(pipeline_type);

-- ============================================
-- VIEWS FOR COMMON QUERIES
-- ============================================

-- Summary statistics
CREATE OR REPLACE VIEW v_pipeline_stats AS
SELECT 
    source,
    status,
    COUNT(*) as document_count,
    SUM(size_bytes) as total_bytes,
    MIN(ingested_at) as first_ingested,
    MAX(ingested_at) as last_ingested
FROM documents
GROUP BY source, status;

-- Material summary across all sources
CREATE OR REPLACE VIEW v_material_summary AS
SELECT 
    material,
    material_category,
    COUNT(*) as listing_count,
    SUM(quantity_tons) as total_tons,
    AVG(price_per_ton) as avg_price,
    MIN(year) as earliest_year,
    MAX(year) as latest_year
FROM waste_listings
GROUP BY material, material_category;

-- Company emissions trends
CREATE OR REPLACE VIEW v_company_emissions AS
SELECT 
    company_normalized as company,
    year,
    SUM(co2_tons) as total_emissions,
    SUM(co2_avoided_tons) as total_avoided,
    AVG(anomaly_score) as avg_anomaly_score
FROM carbon_emissions
GROUP BY company_normalized, year
ORDER BY company, year;
