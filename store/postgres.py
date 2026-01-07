"""
Symbio Data Engine - PostgreSQL Connection
==========================================
Connection pooling, query helpers, and database initialization.

🛡️ FEATURES:
- ThreadedConnectionPool with keepalive
- Upsert support for all tables
- Stuck document recovery
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor, execute_values

import config

logger = logging.getLogger(__name__)

# Connection pool (initialized lazily)
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool
    
    if _connection_pool is None:
        try:
            # 🛡️ Keepalive settings for long-running connections
            keepalive_kwargs = {
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            }
            
            _connection_pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                **keepalive_kwargs,
            )
            logger.info(f"Connected to PostgreSQL at {config.POSTGRES_HOST}:{config.POSTGRES_PORT}")
        except psycopg2.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    return _connection_pool


@contextmanager
def get_connection():
    """
    Context manager for database connections.
    
    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM documents")
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        pool.putconn(conn)


def execute_query(
    query: str,
    params: tuple = None,
    fetch: bool = True,
    as_dict: bool = True,
) -> Optional[list[dict[str, Any]]]:
    """
    Execute a query and optionally fetch results.
    
    Args:
        query: SQL query string
        params: Query parameters
        fetch: Whether to fetch results
        as_dict: Return results as dictionaries
    
    Returns:
        List of results if fetch=True, else None
    """
    with get_connection() as conn:
        cursor_factory = RealDictCursor if as_dict else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(query, params)
            
            if fetch:
                results = cur.fetchall()
                return [dict(row) for row in results] if as_dict else results
            
            return None


def execute_many(
    query: str,
    data: list[tuple],
    page_size: int = 1000,
) -> int:
    """
    Execute a query with multiple value sets efficiently.
    
    Args:
        query: SQL query with %s placeholders
        data: List of tuples to insert
        page_size: Batch size for insertion
    
    Returns:
        Number of rows affected
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, data, page_size=page_size)
            return cur.rowcount


def init_database(reset: bool = False) -> None:
    """
    Initialize database with schemas.
    
    Args:
        reset: If True, drop all tables first (WARNING: destroys data)
    """
    schema_file = Path(__file__).parent / "schemas.sql"
    
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            if reset:
                logger.warning("Resetting database - all data will be lost!")
                # Drop all tables in reverse dependency order
                cur.execute("""
                    DROP TABLE IF EXISTS 
                        pipeline_runs,
                        fraud_flags,
                        carbon_emissions,
                        symbiosis_exchanges,
                        waste_listings,
                        raw_extractions,
                        documents,
                        companies
                    CASCADE;
                    
                    DROP VIEW IF EXISTS 
                        v_pipeline_stats,
                        v_material_summary,
                        v_company_emissions
                    CASCADE;
                """)
                conn.commit()
            
            # Execute schema file
            with open(schema_file, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            
            cur.execute(schema_sql)
            conn.commit()
            
    logger.info("Database initialized successfully")


# ============================================
# Domain-Specific Helpers
# ============================================

def insert_document(
    source: str,
    source_url: str,
    document_type: str,
    content_hash: str,
    file_path: str = None,
    **kwargs,
) -> str:
    """Insert a new document record and return its UUID."""
    query = """
        INSERT INTO documents (source, source_url, document_type, content_hash, file_path, metadata)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (content_hash) DO UPDATE SET
            source_url = EXCLUDED.source_url,
            file_path = COALESCE(EXCLUDED.file_path, documents.file_path),
            metadata = documents.metadata || EXCLUDED.metadata
        RETURNING id
    """
    
    import json
    metadata = json.dumps(kwargs.get("metadata", {}))
    
    result = execute_query(
        query,
        (source, source_url, document_type, content_hash, file_path, metadata),
        fetch=True,
    )
    
    return str(result[0]["id"]) if result else None


def update_document_status(
    document_id: str,
    status: str,
    error_message: str = None,
) -> None:
    """Update document processing status."""
    query = """
        UPDATE documents 
        SET status = %s, 
            error_message = %s,
            processed_at = CASE WHEN %s = 'completed' THEN NOW() ELSE processed_at END
        WHERE id = %s
    """
    execute_query(query, (status, error_message, status, document_id), fetch=False)


def insert_waste_listing(data: dict) -> int:
    """
    Insert a waste listing with UPSERT support.
    
    🛡️ ON CONFLICT: Updates if same document_id + material exists.
    """
    # Valid columns in waste_listings table (filter out Pydantic-only fields)
    VALID_COLUMNS = {
        "document_id", "material", "material_category", "material_subcategory",
        "cas_number", "quantity_tons", "quantity_unit", "price_per_ton", "currency",
        "price_type", "source_company", "source_industry", "source_location",
        "source_country", "quality_grade", "purity_percentage", "treatment_method",
        "availability_status", "listing_date", "expiry_date", "extraction_confidence",
        "data_source_url", "year", "source_quote"  # Added for Citation Rule
    }
    
    # Filter out None values AND columns not in database
    data = {k: v for k, v in data.items() if v is not None and k in VALID_COLUMNS}
    columns = list(data.keys())
    values = list(data.values())
    
    # Build upsert query
    query = sql.SQL("""
        INSERT INTO waste_listings ({columns})
        VALUES ({placeholders})
        ON CONFLICT (document_id, material) 
        WHERE document_id IS NOT NULL AND material IS NOT NULL
        DO UPDATE SET
            quantity_tons = EXCLUDED.quantity_tons,
            source_location = EXCLUDED.source_location,
            source_company = EXCLUDED.source_company,
            extraction_confidence = EXCLUDED.extraction_confidence,
            created_at = NOW()
        RETURNING id
    """).format(
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
    )
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()
            return result[0] if result else None


def insert_carbon_emission(data: dict) -> int:
    """
    Insert a carbon emission record with UPSERT support.
    
    🛡️ ON CONFLICT: Updates if same company + year exists.
    """
    data = {k: v for k, v in data.items() if v is not None}
    columns = list(data.keys())
    values = list(data.values())
    
    query = sql.SQL("""
        INSERT INTO carbon_emissions ({columns})
        VALUES ({placeholders})
        ON CONFLICT (company, year) 
        WHERE company IS NOT NULL AND year IS NOT NULL
        DO UPDATE SET
            co2_tons = COALESCE(EXCLUDED.co2_tons, carbon_emissions.co2_tons),
            extraction_confidence = EXCLUDED.extraction_confidence,
            created_at = NOW()
        RETURNING id
    """).format(
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
    )
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()
            return result[0] if result else None


def insert_symbiosis_exchange(data: dict) -> int:
    """
    Insert a symbiosis exchange record with UPSERT support.
    
    🛡️ ON CONFLICT: Updates if same source+target+material+year exists.
    """
    data = {k: v for k, v in data.items() if v is not None}
    columns = list(data.keys())
    values = list(data.values())
    
    query = sql.SQL("""
        INSERT INTO symbiosis_exchanges ({columns})
        VALUES ({placeholders})
        ON CONFLICT (source_company, target_company, material, year) 
        WHERE source_company IS NOT NULL AND target_company IS NOT NULL
        DO UPDATE SET
            volume_tons = COALESCE(EXCLUDED.volume_tons, symbiosis_exchanges.volume_tons),
            extraction_confidence = EXCLUDED.extraction_confidence,
            created_at = NOW()
        RETURNING id
    """).format(
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
    )
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()
            return result[0] if result else None


def get_pending_documents(source: str = None, limit: int = 100) -> list[dict]:
    """
    Get documents pending processing.
    
    🛡️ Also recovers stuck 'processing' documents older than 1 hour.
    """
    # Base condition - wrapped in parentheses for proper AND logic with source filter
    query = """
        SELECT * FROM documents 
        WHERE (status = 'pending'
           OR (status = 'processing' AND 
               processed_at < NOW() - INTERVAL '1 hour'))
    """
    params = []
    
    if source:
        query += " AND source = %s"
        params.append(source)
    
    query += " ORDER BY ingested_at ASC LIMIT %s"
    params.append(limit)
    
    return execute_query(query, tuple(params)) or []


def get_pipeline_stats() -> dict:
    """Get summary statistics from the pipeline."""
    query = """
        SELECT 
            (SELECT COUNT(*) FROM documents) as total_documents,
            (SELECT COUNT(*) FROM documents WHERE status = 'completed') as processed_documents,
            (SELECT COUNT(*) FROM documents WHERE status = 'pending') as pending_documents,
            (SELECT COUNT(*) FROM documents WHERE status = 'processing') as stuck_documents,
            (SELECT COUNT(*) FROM waste_listings) as waste_listings,
            (SELECT COUNT(*) FROM carbon_emissions) as carbon_records,
            (SELECT COUNT(*) FROM symbiosis_exchanges) as symbiosis_exchanges,
            (SELECT COUNT(*) FROM fraud_flags WHERE status = 'open') as open_fraud_flags
    """
    result = execute_query(query)
    return result[0] if result else {}


def close_pool() -> None:
    """Close the connection pool."""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Connection pool closed")
