"""
Symbio Data Engine - Base Spider
================================
Abstract base class for all web crawlers with common functionality.
"""

import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import httpx

import config
from store.postgres import insert_document, update_document_status

logger = logging.getLogger(__name__)


class BaseSpider(ABC):
    """
    Abstract base spider with common functionality.
    
    Features:
    - Rate limiting
    - User agent rotation
    - Retry logic with exponential backoff
    - Content hashing for deduplication
    - Automatic document storage
    """
    
    name: str = "base"
    source: str = "unknown"
    
    def __init__(
        self,
        domain: str = "research",
        limit: Optional[int] = None,
        rate_limit: float = None,
    ):
        """
        Initialize spider.
        
        Args:
            domain: Target domain (symbioflows, symbiotrust, research)
            limit: Maximum documents to fetch
            rate_limit: Requests per second (default from config)
        """
        self.domain = domain
        self.limit = limit
        self.rate_limit = rate_limit or config.SPIDER_RATE_LIMIT
        self.delay = 1.0 / self.rate_limit
        
        self.session = self._create_session()
        self.documents_fetched = 0
        self.errors = 0
        
        # Ensure data directories exist
        self.raw_dir = config.RAW_DIR / self.source
        self.raw_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_session(self) -> httpx.Client:
        """Create HTTP session with default settings."""
        return httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": self._get_user_agent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )
    
    def _get_user_agent(self) -> str:
        """Get a random user agent from rotation list."""
        return random.choice(config.USER_AGENTS)
    
    def _rotate_user_agent(self) -> None:
        """Rotate to a new user agent."""
        self.session.headers["User-Agent"] = self._get_user_agent()
    
    def _rate_limit_wait(self) -> None:
        """Wait to respect rate limiting."""
        time.sleep(self.delay)
    
    def _hash_content(self, content: bytes) -> str:
        """Generate SHA-256 hash of content for deduplication."""
        return hashlib.sha256(content).hexdigest()
    
    def fetch(
        self,
        url: str,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> Optional[httpx.Response]:
        """
        Fetch a URL with retry logic.
        
        Args:
            url: URL to fetch
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        
        Returns:
            Response object or None on failure
        """
        self._rate_limit_wait()
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = backoff_factor ** (attempt + 2)
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    self._rotate_user_agent()
                elif response.status_code == 404:
                    logger.debug(f"Not found: {url}")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    
            except httpx.TimeoutException:
                logger.warning(f"Timeout for {url}, attempt {attempt + 1}/{max_retries}")
            except httpx.RequestError as e:
                logger.error(f"Request error for {url}: {e}")
            
            # Exponential backoff
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                time.sleep(wait_time)
        
        self.errors += 1
        return None
    
    def save_raw(
        self,
        content: bytes,
        url: str,
        document_type: str = "html",
        metadata: dict = None,
    ) -> Optional[str]:
        """
        Save raw content to disk and database.
        
        Args:
            content: Raw content bytes
            url: Source URL
            document_type: Type of document (html, pdf, csv)
            metadata: Additional metadata
        
        Returns:
            Document ID if saved, None if duplicate
        """
        content_hash = self._hash_content(content)
        
        # Generate filename from hash
        extension = document_type if document_type else "bin"
        filename = f"{content_hash[:16]}.{extension}"
        file_path = self.raw_dir / filename
        
        # Save to disk
        file_path.write_bytes(content)
        
        # Save to database
        doc_id = insert_document(
            source=self.source,
            source_url=url,
            document_type=document_type,
            content_hash=content_hash,
            file_path=str(file_path),
            metadata={
                "domain": self.domain,
                "file_path": str(file_path),
                "size_bytes": len(content),
                "fetched_at": datetime.now().isoformat(),
                **(metadata or {}),
            },
        )
        
        if doc_id:
            self.documents_fetched += 1
            logger.info(f"Saved document: {filename} ({len(content)} bytes)")
        else:
            logger.debug(f"Duplicate content: {content_hash[:16]}")
        
        return doc_id
    
    def should_continue(self) -> bool:
        """Check if spider should continue fetching."""
        if self.limit is None:
            return True
        return self.documents_fetched < self.limit
    
    @abstractmethod
    def get_urls(self) -> Generator[str, None, None]:
        """
        Generate URLs to crawl.
        
        Yields:
            URLs to fetch
        """
        pass
    
    @abstractmethod
    def parse(self, response: httpx.Response, url: str) -> Optional[dict]:
        """
        Parse a response and extract data.
        
        Args:
            response: HTTP response
            url: Original URL
        
        Returns:
            Extracted data dict or None
        """
        pass
    
    def run(self) -> dict:
        """
        Execute the spider.
        
        Returns:
            Dict with results summary
        """
        logger.info(f"Starting {self.name} spider for domain '{self.domain}'")
        start_time = time.time()
        
        try:
            for url in self.get_urls():
                if not self.should_continue():
                    logger.info(f"Reached limit of {self.limit} documents")
                    break
                
                response = self.fetch(url)
                if response:
                    try:
                        self.parse(response, url)
                    except Exception as e:
                        logger.error(f"Parse error for {url}: {e}")
                        self.errors += 1
                        
        except KeyboardInterrupt:
            logger.info("Spider interrupted by user")
        finally:
            self.session.close()
        
        elapsed = time.time() - start_time
        
        results = {
            "spider": self.name,
            "domain": self.domain,
            "documents": self.documents_fetched,
            "errors": self.errors,
            "elapsed_seconds": round(elapsed, 2),
        }
        
        logger.info(
            f"Spider complete: {self.documents_fetched} documents, "
            f"{self.errors} errors, {elapsed:.1f}s elapsed"
        )
        
        return results
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
