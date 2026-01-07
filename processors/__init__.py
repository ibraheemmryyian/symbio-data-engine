"""
Symbio Data Engine - Processors Module
======================================
Data cleaning, normalization, and extraction pipeline.

Pipeline: Raw → Clean → Normalize → Extract → Store
"""

from .cleaner import Cleaner, clean_text, clean_html
from .normalizer import Normalizer, normalize_units, resolve_company
from .extractor import Extractor, extract_structured_data
from .pdf_processor import PDFProcessor, extract_pdf_text, extract_pdf_tables
from .gov_processor import GovProcessor
from .models import (
    WasteListingExtraction,
    CarbonEmissionExtraction,
    SymbiosisExchangeExtraction,
    ExtractionResult,
    validate_and_create,
)


def run_pipeline(
    source: str = "all",
    reprocess: bool = False,
    batch_size: int = 100,
) -> dict:
    """
    Run the full processing pipeline on pending documents.
    
    Args:
        source: Document source to process (wayback, gov, csr, scrap, all)
        reprocess: Re-process already processed documents
        batch_size: Number of documents to process per batch
    
    Returns:
        Dict with processing results
    """
    from store.postgres import get_pending_documents, update_document_status
    
    results = {
        "source": source,
        "processed": 0,
        "errors": 0,
    }
    
    # Get pending documents
    documents = get_pending_documents(
        source=source if source != "all" else None,
        limit=batch_size,
    )
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"DEBUG: run_pipeline found {len(documents)} docs for source={source}")
    
    cleaner = Cleaner()
    normalizer = Normalizer()
    extractor = Extractor()
    pdf_processor = PDFProcessor()
    gov_processor = GovProcessor()
    
    for doc in documents:
        try:
            logger.info(f"DEBUG: Processing doc {doc.get('id')} type={doc.get('document_type')} path={doc.get('file_path')}")
            doc_type = doc.get("document_type", "")
            
            if doc_type == "csv":
                # Specialized pipeline for structured Gov data
                source_map = {
                    "government": "epa_tri",
                    "gov": "epa_tri",
                    "eprtr": "eprtr", 
                    "mena": "generic",
                    "bayanat": "generic",
                    "saudi": "generic"
                }
                src_type = source_map.get(doc.get("source"), "epa_tri")
                results_list = gov_processor.process_csv(doc["file_path"], source_type=src_type)
            else:
                # Standard Text Pipeline
                if doc_type == "pdf":
                    text = pdf_processor.extract_text(doc["file_path"])
                    tables = pdf_processor.extract_tables(doc["file_path"])
                else:
                    text = cleaner.clean(doc["file_path"])
                    tables = []
                
                # Normalize
                normalized = normalizer.normalize(text)
                
                # Extract structured data
                results_list = extractor.extract_multiple(normalized, doc_type=doc_type)
            
            # Save valid extractions to database
            from store.postgres import (
                insert_waste_listing,
                insert_carbon_emission,
                insert_symbiosis_exchange
            )
            
            logger.info(f"DEBUG: Got {len(results_list)} extraction results for doc {doc.get('id')}")
            
            success_count = 0
            for res in results_list:
                if res.is_valid and res.data:
                    try:
                        # Inject document_id for source traceability
                        res.data["document_id"] = doc["id"]
                        
                        if res.record_type == "waste_listing":
                            insert_waste_listing(res.data)
                        elif res.record_type == "carbon_emission":
                            insert_carbon_emission(res.data)
                        elif res.record_type == "symbiosis_exchange":
                            insert_symbiosis_exchange(res.data)
                        success_count += 1
                    except Exception as db_err:
                        # Log DB error but continue processing other facts
                        logger.error(f"DEBUG: DB insertion failed: {db_err}")
                        results["errors"] += 1
            
            # Update status
            status = "completed" if success_count > 0 else "completed_no_data"
            update_document_status(doc["id"], status)
            results["processed"] += 1
            
        except Exception as e:
            update_document_status(doc["id"], "failed", str(e))
            results["errors"] += 1
    
    return results


__all__ = [
    # Classes
    "Cleaner",
    "Normalizer",
    "Extractor",
    "PDFProcessor",
    # Functions
    "clean_text",
    "clean_html",
    "normalize_units",
    "resolve_company",
    "extract_structured_data",
    "extract_pdf_text",
    "extract_pdf_tables",
    "run_pipeline",
]
