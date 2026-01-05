"""
Symbio Data Engine - PDF Processor
==================================
Specialized processor for PDF documents.

Operations:
1. Text extraction (PyPDF2)
2. Table extraction (Camelot/Tabula)
3. OCR fallback (Tesseract)
4. Image extraction for scanned documents
"""

import logging
import os
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)


class PDFProcessor:
    """
    PDF processing for sustainability reports and documents.
    
    Uses multiple extraction methods:
    1. PyPDF2 for text extraction
    2. Camelot for table extraction (lattice-based tables)
    3. Tabula for table extraction (stream-based tables)
    4. Tesseract OCR for scanned documents
    """
    
    def __init__(self):
        """Initialize PDF processor with available backends."""
        self.has_pypdf = self._check_pypdf()
        self.has_camelot = self._check_camelot()
        self.has_tabula = self._check_tabula()
        self.has_tesseract = self._check_tesseract()
        
        logger.info(
            f"PDF backends - PyPDF2: {self.has_pypdf}, "
            f"Camelot: {self.has_camelot}, "
            f"Tabula: {self.has_tabula}, "
            f"Tesseract: {self.has_tesseract}"
        )
    
    def _check_pypdf(self) -> bool:
        try:
            import PyPDF2
            return True
        except ImportError:
            return False
    
    def _check_camelot(self) -> bool:
        try:
            import camelot
            return True
        except ImportError:
            return False
    
    def _check_tabula(self) -> bool:
        try:
            import tabula
            return True
        except ImportError:
            return False
    
    def _check_tesseract(self) -> bool:
        try:
            import pytesseract
            # Check if tesseract is installed
            pytesseract.get_tesseract_version()
            return True
        except Exception:
            return False
    
    def extract_text(self, file_path: str | Path) -> str:
        """
        Extract text from PDF.
        
        Args:
            file_path: Path to PDF file
        
        Returns:
            Extracted text content
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"PDF not found: {file_path}")
        
        # Try PyPDF2 first
        if self.has_pypdf:
            text = self._extract_with_pypdf(file_path)
            if text and len(text.strip()) > 100:
                return text
        
        # Fall back to OCR if text extraction failed
        if self.has_tesseract:
            logger.info(f"Falling back to OCR for {file_path}")
            return self._extract_with_ocr(file_path)
        
        logger.warning(f"No PDF extraction method available for {file_path}")
        return ""
    
    def _extract_with_pypdf(self, file_path: Path) -> str:
        """Extract text using PyPDF2."""
        try:
            import PyPDF2
            
            text_parts = []
            
            with open(file_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
            
            return "\n\n".join(text_parts)
            
        except Exception as e:
            logger.warning(f"PyPDF2 extraction failed: {e}")
            return ""
    
    def _extract_with_ocr(self, file_path: Path) -> str:
        """Extract text using OCR (for scanned documents)."""
        try:
            import gc
            import pytesseract
            from pdf2image import convert_from_path
            
            # Set tesseract path if configured
            if config.TESSERACT_CMD:
                pytesseract.pytesseract.tesseract_cmd = config.TESSERACT_CMD
            
            # Convert PDF pages to images
            images = convert_from_path(file_path)
            
            text_parts = []
            
            for i, image in enumerate(images):
                page_text = pytesseract.image_to_string(image)
                text_parts.append(page_text)
                logger.debug(f"OCR page {i+1}: {len(page_text)} chars")
                
                # 🛡️ MEMORY: Explicitly delete image after processing
                del image
            
            # 🛡️ MEMORY: Clean up all images and force GC
            del images
            gc.collect()
            
            return "\n\n".join(text_parts)
            
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return ""
    
    def extract_tables(
        self,
        file_path: str | Path,
        pages: str = "all",
    ) -> list[dict]:
        """
        Extract tables from PDF.
        
        Args:
            file_path: Path to PDF file
            pages: Page specification ("all", "1", "1-3", etc.)
        
        Returns:
            List of tables as dicts with 'headers' and 'data'
        """
        file_path = Path(file_path)
        
        tables = []
        
        # Try Camelot first (better for lattice tables)
        if self.has_camelot:
            tables = self._extract_tables_camelot(file_path, pages)
            if tables:
                return tables
        
        # Fall back to Tabula (better for stream tables)
        if self.has_tabula:
            tables = self._extract_tables_tabula(file_path, pages)
        
        return tables
    
    def _extract_tables_camelot(
        self,
        file_path: Path,
        pages: str,
    ) -> list[dict]:
        """Extract tables using Camelot."""
        try:
            import camelot
            
            # Try lattice mode first (for bordered tables)
            tables = camelot.read_pdf(
                str(file_path),
                pages=pages,
                flavor="lattice",
            )
            
            if len(tables) == 0:
                # Try stream mode (for borderless tables)
                tables = camelot.read_pdf(
                    str(file_path),
                    pages=pages,
                    flavor="stream",
                )
            
            result = []
            
            for table in tables:
                df = table.df
                
                # First row as headers
                headers = df.iloc[0].tolist()
                data = df.iloc[1:].values.tolist()
                
                result.append({
                    "headers": headers,
                    "data": data,
                    "accuracy": table.accuracy,
                    "page": table.page,
                })
            
            return result
            
        except Exception as e:
            logger.warning(f"Camelot extraction failed: {e}")
            return []
    
    def _extract_tables_tabula(
        self,
        file_path: Path,
        pages: str,
    ) -> list[dict]:
        """Extract tables using Tabula."""
        try:
            import tabula
            
            dfs = tabula.read_pdf(
                str(file_path),
                pages=pages,
                multiple_tables=True,
            )
            
            result = []
            
            for df in dfs:
                if df.empty:
                    continue
                
                headers = df.columns.tolist()
                data = df.values.tolist()
                
                result.append({
                    "headers": headers,
                    "data": data,
                    "accuracy": None,  # Tabula doesn't provide accuracy
                })
            
            return result
            
        except Exception as e:
            logger.warning(f"Tabula extraction failed: {e}")
            return []
    
    def get_metadata(self, file_path: str | Path) -> dict:
        """
        Extract PDF metadata.
        
        Args:
            file_path: Path to PDF file
        
        Returns:
            Dict with metadata (title, author, creation_date, etc.)
        """
        file_path = Path(file_path)
        
        if not self.has_pypdf:
            return {}
        
        try:
            import PyPDF2
            
            with open(file_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                info = reader.metadata
                
                if not info:
                    return {}
                
                return {
                    "title": info.get("/Title"),
                    "author": info.get("/Author"),
                    "creator": info.get("/Creator"),
                    "producer": info.get("/Producer"),
                    "creation_date": str(info.get("/CreationDate")),
                    "modification_date": str(info.get("/ModDate")),
                    "page_count": len(reader.pages),
                }
                
        except Exception as e:
            logger.warning(f"Metadata extraction failed: {e}")
            return {}
    
    def is_scanned(self, file_path: str | Path) -> bool:
        """
        Check if PDF is a scanned document (image-based).
        
        Args:
            file_path: Path to PDF file
        
        Returns:
            True if PDF appears to be scanned/image-based
        """
        # Try to extract text - if minimal, likely scanned
        text = self._extract_with_pypdf(Path(file_path))
        
        # If very little text, likely scanned
        # (~100 chars per page for scanned PDFs with some OCR)
        metadata = self.get_metadata(file_path)
        page_count = metadata.get("page_count", 1)
        
        chars_per_page = len(text) / max(page_count, 1)
        
        return chars_per_page < 100


# Convenience functions
def extract_pdf_text(file_path: str | Path) -> str:
    """Extract text from PDF."""
    return PDFProcessor().extract_text(file_path)


def extract_pdf_tables(file_path: str | Path) -> list[dict]:
    """Extract tables from PDF."""
    return PDFProcessor().extract_tables(file_path)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        processor = PDFProcessor()
        
        print(f"Processing: {pdf_path}")
        print(f"Metadata: {processor.get_metadata(pdf_path)}")
        print(f"Is scanned: {processor.is_scanned(pdf_path)}")
        
        text = processor.extract_text(pdf_path)
        print(f"\nExtracted {len(text)} characters")
        print(f"Preview: {text[:500]}...")
        
        tables = processor.extract_tables(pdf_path)
        print(f"\nFound {len(tables)} tables")
    else:
        print("Usage: python pdf_processor.py <path_to_pdf>")
