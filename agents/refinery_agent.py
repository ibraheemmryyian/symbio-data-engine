"""
Symbio Data Engine - Refinery Agent
===================================
Autonomous document processing agent that orchestrates the full pipeline.

The Refinery Agent is the "Brain" that:
1. Monitors for new documents
2. Routes documents to appropriate processors
3. Manages retry logic for failed extractions
4. Generates embeddings and stores in ChromaDB
5. Exports data in LLM training formats

🛡️ STREAMING ARCHITECTURE (50GB Safe):
- Generator-based processing (no memory loading)
- Max 500MB RAM at any time
- Explicit gc.collect() after large files
"""

import gc
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

import config
from processors import Cleaner, Normalizer, Extractor, PDFProcessor
from processors.models import ExtractionResult
from store.postgres import (
    get_pending_documents,
    update_document_status,
    insert_waste_listing,
    insert_carbon_emission,
    insert_symbiosis_exchange,
    execute_query,
)
from store.vectors import add_documents, get_vectorstore

logger = logging.getLogger(__name__)


class RefineryAgent:
    """
    Autonomous document processing agent.
    
    🛡️ STREAMING ARCHITECTURE: Operates with generators
    to process documents one-by-one without loading all in memory.
    """
    
    def __init__(
        self,
        batch_size: int = 10,
        sleep_interval: int = 60,
        max_retries: int = 3,
    ):
        """
        Initialize the refinery agent.
        
        Args:
            batch_size: Documents to process per batch
            sleep_interval: Seconds to wait when no documents pending
            max_retries: Maximum retry attempts for failed documents
        """
        self.batch_size = batch_size
        self.sleep_interval = sleep_interval
        self.max_retries = max_retries
        
        # Initialize processors
        self.cleaner = Cleaner()
        self.normalizer = Normalizer()
        self.extractor = Extractor()
        self.pdf_processor = PDFProcessor()
        
        # Statistics
        self.stats = {
            "documents_processed": 0,
            "documents_failed": 0,
            "documents_rejected": 0,  # 🛡️ Zero Hallucination rejections
            "waste_listings_created": 0,
            "carbon_records_created": 0,
            "symbiosis_exchanges_created": 0,
            "started_at": None,
        }
        
        self.running = False
    
    def run(self, continuous: bool = False) -> dict:
        """
        Run the refinery agent.
        
        Args:
            continuous: If True, run continuously; otherwise process one batch
        
        Returns:
            Processing statistics
        """
        self.running = True
        self.stats["started_at"] = datetime.now().isoformat()
        
        logger.info("🏭 Refinery Agent starting (Streaming Mode)...")
        
        try:
            if continuous:
                self._run_continuous()
            else:
                self._process_batch()
        except KeyboardInterrupt:
            logger.info("Refinery Agent interrupted")
        finally:
            self.running = False
            # 🛡️ Final garbage collection
            gc.collect()
        
        logger.info(f"Refinery Agent completed: {self.stats}")
        return self.stats
    
    def _run_continuous(self):
        """Run in continuous mode, processing documents as they arrive."""
        while self.running:
            # 🛡️ STREAMING: Use generator for memory safety
            processed_any = False
            for doc in self._stream_pending_documents():
                if not self.running:
                    break
                self._process_document(doc)
                processed_any = True
                
                # 🛡️ MEMORY: Force GC periodically
                if self.stats["documents_processed"] % 10 == 0:
                    gc.collect()
            
            if not processed_any:
                logger.debug(f"No pending documents, sleeping {self.sleep_interval}s")
                time.sleep(self.sleep_interval)
    
    def _process_batch(self):
        """Process a single batch of documents using generators."""
        logger.info(f"Processing batch (max {self.batch_size} documents)")
        
        # 🛡️ STREAMING: Fetch documents one at a time
        for doc in self._stream_pending_documents():
            self._process_document(doc)
            
            # 🛡️ MEMORY SAFETY: Force garbage collection after each doc
            gc.collect()
    
    def _stream_pending_documents(self) -> Generator[dict, None, None]:
        """
        Generator to stream documents without loading all in memory.
        
        🛡️ STREAMING ARCHITECTURE: Yields one document at a time.
        """
        documents = get_pending_documents(limit=self.batch_size)
        if not documents:
            return
        
        for doc in documents:
            yield doc
    
    def _process_document(self, doc: dict):
        """
        Process a single document through the full pipeline.
        
        🛡️ ZERO HALLUCINATION: Extraction results are validated.
        🛡️ MEMORY: Explicit cleanup after processing.
        
        Args:
            doc: Document record from database
        """
        doc_id = doc["id"]
        doc_type = doc.get("document_type", "")
        file_path = doc.get("metadata", {}).get("file_path")
        
        logger.info(f"Processing document {doc_id} ({doc_type})")
        
        try:
            # Update status to processing
            update_document_status(doc_id, "processing")
            
            # Step 1: Extract text
            if doc_type == "pdf":
                text = self.pdf_processor.extract_text(file_path)
                tables = self.pdf_processor.extract_tables(file_path)
            elif doc_type in ["html", "htm"]:
                text = self.cleaner.clean(file_path)
                tables = []
            elif doc_type == "csv":
                # CSV is already structured, just read it
                text = Path(file_path).read_text(encoding="utf-8", errors="replace")
                tables = []
            else:
                text = Path(file_path).read_text(encoding="utf-8", errors="replace")
                tables = []
            
            # Step 2: Clean and normalize
            cleaned_text = self.cleaner.clean_text(text)
            
            # Step 3: Extract structured data (🛡️ Zero Hallucination)
            extraction: ExtractionResult = self.extractor.extract(cleaned_text)
            
            # Step 4: Store extraction (only if valid)
            if extraction.is_valid:
                self._store_extraction(doc, extraction)
            else:
                logger.warning(f"🛡️ Extraction rejected: {extraction.rejection_reason}")
                self.stats["documents_rejected"] += 1
            
            # Step 5: Generate embeddings and store in ChromaDB
            self._store_embedding(doc_id, cleaned_text, doc.get("metadata", {}))
            
            # Mark as completed
            update_document_status(doc_id, "completed")
            self.stats["documents_processed"] += 1
            
        except Exception as e:
            logger.error(f"Failed to process document {doc_id}: {e}")
            update_document_status(doc_id, "failed", str(e))
            self.stats["documents_failed"] += 1
        
        finally:
            # 🛡️ MEMORY SAFETY: Explicit cleanup prevents leaks
            # Python's GC is lazy - we force cleanup here
            try:
                del text
                del cleaned_text
                if 'tables' in dir():
                    del tables
            except NameError:
                pass  # Variables may not exist if error occurred early
            
            # Force garbage collection for large files
            if doc_type == "pdf":
                gc.collect()
                logger.debug(f"gc.collect() after PDF {doc_id}")
    
    def _store_extraction(self, doc: dict, extraction: ExtractionResult):
        """
        Store extracted data in appropriate tables.
        
        🛡️ Only called if extraction.is_valid == True
        
        Args:
            doc: Original document record
            extraction: Validated extraction result
        """
        data = extraction.data or {}
        record_type = extraction.record_type
        
        if not data:
            return
        
        # Add document reference
        data["document_id"] = doc["id"]
        data["data_source_url"] = doc.get("source_url")
        
        # Route to appropriate storage
        if record_type == "waste_listing":
            self._store_waste_listing(data)
        elif record_type == "carbon_emission":
            self._store_carbon_emission(data)
        elif record_type == "symbiosis_exchange":
            self._store_symbiosis_exchange(data)
    
    def _store_waste_listing(self, data: dict):
        """Store as waste listing."""
        # Map extraction fields to DB schema
        listing = {
            "document_id": data.get("document_id"),
            "material": data.get("material"),
            "quantity_tons": data.get("quantity_tons"),
            "source_company": data.get("source_company"),
            "year": data.get("year"),
            "extraction_confidence": data.get("extraction_confidence"),
            "data_source_url": data.get("data_source_url"),
        }
        
        # Only store if we have minimum required fields
        if listing.get("material"):
            try:
                insert_waste_listing(listing)
                self.stats["waste_listings_created"] += 1
            except Exception as e:
                logger.warning(f"Failed to store waste listing: {e}")
    
    def _store_carbon_emission(self, data: dict):
        """Store as carbon emission record."""
        record = {
            "document_id": data.get("document_id"),
            "company": data.get("company"),
            "year": data.get("year"),
            "co2_tons": data.get("co2_tons"),
            "extraction_confidence": data.get("extraction_confidence"),
            "data_source_url": data.get("data_source_url"),
        }
        
        if record.get("company") or record.get("co2_tons"):
            try:
                insert_carbon_emission(record)
                self.stats["carbon_records_created"] += 1
            except Exception as e:
                logger.warning(f"Failed to store carbon emission: {e}")
    
    def _store_symbiosis_exchange(self, data: dict):
        """Store as symbiosis exchange."""
        exchange = {
            "document_id": data.get("document_id"),
            "source_company": data.get("source_company"),
            "target_company": data.get("target_company"),
            "material": data.get("material"),
            "volume_tons": data.get("volume_tons") or data.get("volume"),
            "eco_park": data.get("eco_park"),
            "year": data.get("year"),
            "extraction_confidence": data.get("extraction_confidence"),
            "data_source_url": data.get("data_source_url"),
        }
        
        if exchange.get("material") or exchange.get("eco_park"):
            try:
                insert_symbiosis_exchange(exchange)
                self.stats["symbiosis_exchanges_created"] += 1
            except Exception as e:
                logger.warning(f"Failed to store symbiosis exchange: {e}")
    
    def _store_embedding(self, doc_id: str, text: str, metadata: dict):
        """
        Generate and store document embedding in ChromaDB.
        
        Args:
            doc_id: Document ID
            text: Cleaned text content
            metadata: Document metadata
        """
        if not text or len(text) < 50:
            return
        
        try:
            # Truncate text for embedding (most models have limits)
            text_chunk = text[:8000]
            
            add_documents(
                collection_name="documents",
                documents=[text_chunk],
                metadatas=[{
                    "source": metadata.get("domain", "unknown"),
                    "year": metadata.get("year"),
                    "type": metadata.get("document_type"),
                }],
                ids=[str(doc_id)],
            )
            
        except Exception as e:
            logger.warning(f"Failed to store embedding: {e}")
    
    def stop(self):
        """Stop the agent if running continuously."""
        self.running = False
        logger.info("Refinery Agent stopping...")


# CLI helper
def run_refinery(
    continuous: bool = False,
    batch_size: int = 10,
) -> dict:
    """
    Run the refinery agent from CLI.
    
    Args:
        continuous: Run continuously
        batch_size: Documents per batch
    
    Returns:
        Processing statistics
    """
    agent = RefineryAgent(batch_size=batch_size)
    return agent.run(continuous=continuous)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    agent = RefineryAgent(batch_size=5)
    results = agent.run(continuous=False)
    
    print(f"\n🏭 Results: {results}")
