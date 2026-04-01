-- store/materials_schema.sql
CREATE TABLE IF NOT EXISTS materials (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    category        VARCHAR(50) NOT NULL,
    subcategory     VARCHAR(100),
    industry        VARCHAR(100) NOT NULL,
    unit            VARCHAR(20) NOT NULL DEFAULT 'metric_tons',
    annual_volume_tons DECIMAL(15,2),
    disposal_cost_per_unit DECIMAL(12,2),
    environmental_impact JSONB DEFAULT '{}',
    recyclability   VARCHAR(20) NOT NULL DEFAULT 'unknown',
    primary_applications TEXT[],
    receiver_industries  TEXT[],
    hazard_classification VARCHAR(100),
    data_source     VARCHAR(100),
    data_source_url TEXT,
    verified        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS materials_category_idx ON materials(category);
CREATE INDEX IF NOT EXISTS materials_industry_idx ON materials(industry);
CREATE INDEX IF NOT EXISTS materials_recyclability_idx ON materials(recyclability);
