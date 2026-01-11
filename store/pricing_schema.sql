-- ============================================
-- SYMBIO DATA ENGINE - Pricing Lookup Tables
-- ============================================
-- Volatile pricing data - updated by spiders
-- JOIN with waste_listings for instant valuation
-- ============================================

-- Material pricing from multiple sources (raw)
CREATE TABLE IF NOT EXISTS material_prices_raw (
    id SERIAL PRIMARY KEY,
    material_name VARCHAR(100) NOT NULL,
    material_category VARCHAR(50),
    
    -- Pricing
    price_value DECIMAL(12,4) NOT NULL,
    price_unit VARCHAR(20) NOT NULL,      -- 'lb', 'kg', 'ton', 'mt'
    currency VARCHAR(3) DEFAULT 'USD',
    
    -- Source tracking
    source VARCHAR(50) NOT NULL,          -- scrapmonster, rockaway, iscrap, etc.
    source_url TEXT,
    region VARCHAR(50),                   -- us_east, us_west, eu, asia
    
    -- Timestamps
    price_date DATE,                      -- when source quoted this price
    fetched_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_raw_prices_material ON material_prices_raw(material_name);
CREATE INDEX idx_raw_prices_source ON material_prices_raw(source);
CREATE INDEX idx_raw_prices_date ON material_prices_raw(fetched_at);

-- Aggregated valuations (averaged from raw prices)
CREATE TABLE IF NOT EXISTS material_valuations (
    id SERIAL PRIMARY KEY,
    material_type_id VARCHAR(20) UNIQUE NOT NULL,  -- 'CU-001', 'AL-002', etc.
    material_name VARCHAR(100) NOT NULL,
    material_category VARCHAR(50),
    
    -- Normalized pricing (always USD per metric ton)
    price_per_ton_usd DECIMAL(12,2) NOT NULL,
    price_per_lb_usd DECIMAL(12,4),
    
    -- Averaging metadata
    source_count INT DEFAULT 1,            -- how many sources were averaged
    price_range_low DECIMAL(12,2),
    price_range_high DECIMAL(12,2),
    confidence_score DECIMAL(3,2),         -- 0-1, higher = more sources
    
    -- Timestamps
    last_updated TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT valid_price CHECK (price_per_ton_usd > 0)
);

CREATE INDEX idx_valuations_material ON material_valuations(material_name);
CREATE INDEX idx_valuations_category ON material_valuations(material_category);
CREATE INDEX idx_valuations_type_id ON material_valuations(material_type_id);

-- Material type mapping (maps your waste streams to pricing categories)
CREATE TABLE IF NOT EXISTS material_type_mapping (
    id SERIAL PRIMARY KEY,
    waste_material VARCHAR(100) NOT NULL,     -- from waste_listings.material
    material_type_id VARCHAR(20) NOT NULL,    -- FK to material_valuations
    match_confidence DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_mapping_waste ON material_type_mapping(waste_material);

-- ============================================
-- VIEW: Instant valuation across all waste
-- ============================================
CREATE OR REPLACE VIEW v_waste_valued AS
SELECT 
    wl.id,
    wl.material,
    wl.material_category,
    wl.quantity_tons,
    wl.source_company,
    wl.source_country,
    wl.year,
    mv.price_per_ton_usd,
    mv.source_count,
    mv.confidence_score,
    mv.last_updated as price_updated,
    ROUND((wl.quantity_tons * mv.price_per_ton_usd)::numeric, 2) as estimated_value_usd
FROM waste_listings wl
LEFT JOIN material_type_mapping mtm ON LOWER(wl.material) = LOWER(mtm.waste_material)
LEFT JOIN material_valuations mv ON mtm.material_type_id = mv.material_type_id
WHERE wl.quantity_tons IS NOT NULL AND wl.quantity_tons > 0;
