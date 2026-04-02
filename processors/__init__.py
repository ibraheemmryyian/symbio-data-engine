"""
Symbio Data Engine - Processors Module
======================================
Data cleaning, normalization, and extraction pipeline.

Pipeline: Raw → Clean → Normalize → Extract → Store
"""

from .cleaner import Cleaner, clean_text, clean_html
from .normalizer import Normalizer, normalize_units, resolve_company
from .extractor import Extractor, extract_structured_data
from .local_extractor import LocalExtractor
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
    use_local_llm: bool = True,
) -> dict:
    """
    Run the full processing pipeline on pending documents.

    Args:
        source: Document source to process (wayback, gov, csr, scrap, all)
        reprocess: Re-process already processed documents
        batch_size: Number of documents to process per batch
        use_local_llm: Use local LLM with regex validation (True) or cloud API (False)

    Returns:
        Dict with processing results
    """
    from store.postgres import get_pending_documents, update_document_status

    results = {
        "source": source,
        "processed": 0,
        "errors": 0,
        "extraction_method": "local_llm_with_regex" if use_local_llm else "cloud_api",
    }

    # Get pending documents
    documents = get_pending_documents(
        source=source if source != "all" else None,
        limit=batch_size,
    )

    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"DEBUG: run_pipeline found {len(documents)} docs for source={source}")
    logger.info(f"DEBUG: Using extraction method: {results['extraction_method']}")

    cleaner = Cleaner()
    normalizer = Normalizer()
    extractor = LocalExtractor() if use_local_llm else Extractor()
    pdf_processor = PDFProcessor()
    gov_processor = GovProcessor()
    
    for doc in documents:
        try:
            logger.info(f"DEBUG: Processing doc {doc.get('id')} type={doc.get('document_type')} path={doc.get('file_path')}")
            doc_type = doc.get("document_type", "")
            
            if doc_type == "csv":
                # Specialized pipeline for structured Gov data
                source = doc.get("source", "")
                industrial_sources = ["worldsteel", "fao", "plastics", "petcoke", "maritime"]

                if source in industrial_sources:
                    # Industrial CSV pipeline
                    results_list = gov_processor.process_csv_industrial(doc["file_path"], source_type=source)
                else:
                    # Government CSV pipeline
                    source_map = {
                        "government": "epa_tri",
                        "gov": "epa_tri",
                        "eprtr": "eprtr",
                        "mena": "generic",
                        "bayanat": "generic",
                        "saudi": "generic"
                    }
                    src_type = source_map.get(source, "epa_tri")
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
                try:
                    if use_local_llm:
                        # LocalExtractor.extract_document returns ExtractionResult objects
                        results_list = extractor.extract_document(normalized, doc_type=doc_type, source=doc.get("source", "unknown"))
                    else:
                        # Cloud Extractor.extract_multiple also returns ExtractionResult objects
                        results_list = extractor.extract_multiple(normalized, doc_type=doc_type)
                except Exception as extract_err:
                    logger.error(f"Extraction failed for doc {doc.get('id')}: {extract_err}", exc_info=True)
                    update_document_status(doc["id"], "failed", f"Extraction error: {str(extract_err)[:200]}")
                    results["errors"] += 1
                    continue

                if not isinstance(results_list, list):
                    logger.error(f"Extractor returned non-list: {type(results_list)}")
                    results_list = []

            # Save valid extractions to database
            from store.postgres import (
                insert_waste_listing,
                insert_carbon_emission,
                insert_symbiosis_exchange,
                insert_industrial_byproduct,
            )

            logger.info(f"DEBUG: Got {len(results_list)} extraction results for doc {doc.get('id')}")

            stats = {}
            success_count = 0
            for res in results_list:
                if res.is_valid and res.data:
                    try:
                        # Inject document_id for source traceability
                        res.data["document_id"] = doc["id"]

                        if res.record_type == "waste_listing":
                            insert_waste_listing(res.data)
                            stats["waste_listings"] = stats.get("waste_listings", 0) + 1
                        elif res.record_type == "carbon_emission":
                            insert_carbon_emission(res.data)
                            stats["carbon_emissions"] = stats.get("carbon_emissions", 0) + 1
                        elif res.record_type == "symbiosis_exchange":
                            insert_symbiosis_exchange(res.data)
                            stats["symbiosis_exchanges"] = stats.get("symbiosis_exchanges", 0) + 1
                        elif res.record_type == "industrial_byproduct":
                            rec = res.data
                            insert_industrial_byproduct(
                                document_id=doc["id"],
                                material_name=rec.get("material_name"),
                                industry=rec.get("industry"),
                                source_company=rec.get("source_company"),
                                source_country=rec.get("source_country"),
                                source_location=rec.get("source_location"),
                                quantity_tons=rec.get("quantity_tons"),
                                year=rec.get("year"),
                                price_per_unit=rec.get("price_per_unit", 0),
                                currency=rec.get("currency", "USD"),
                                receiver_industry=rec.get("primary_receiver_industry"),
                                verification_method=rec.get("verification_method", "Industrial Source"),
                                extraction_confidence=rec.get("extraction_confidence", 0.85),
                            )
                            stats["industrial_byproducts"] = stats.get("industrial_byproducts", 0) + 1
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
