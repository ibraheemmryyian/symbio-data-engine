"""
Symbio Data Engine - Text/HTML Cleaner
======================================
First stage of the processing pipeline.

Operations:
1. HTML stripping (trafilatura)
2. Text normalization (ftfy)
3. Deduplication (content hashing)
4. Whitespace normalization
5. 🛡️ Nav/Footer/Cookie banner removal
"""

import hashlib
import logging
import re
from pathlib import Path
from typing import Optional, Generator

import ftfy
import trafilatura

logger = logging.getLogger(__name__)


# 🛡️ REGEX PATTERNS FOR NOISE REMOVAL
# These patterns strip common web noise before extraction
NOISE_PATTERNS = [
    # Navigation elements
    (r"<nav[^>]*>.*?</nav>", re.DOTALL | re.I),
    (r"<header[^>]*>.*?</header>", re.DOTALL | re.I),
    (r"<footer[^>]*>.*?</footer>", re.DOTALL | re.I),
    (r"<aside[^>]*>.*?</aside>", re.DOTALL | re.I),
    (r"<menu[^>]*>.*?</menu>", re.DOTALL | re.I),
    
    # Cookie/GDPR banners
    (r"<div[^>]*(?:cookie|gdpr|consent|privacy-banner)[^>]*>.*?</div>", re.DOTALL | re.I),
    (r"<div[^>]*class=[\"'][^\"']*(?:cookie|consent|gdpr|banner)[^\"']*[\"'][^>]*>.*?</div>", re.DOTALL | re.I),
    
    # Ads and promos
    (r"<div[^>]*(?:advertisement|ad-container|promo)[^>]*>.*?</div>", re.DOTALL | re.I),
    (r"<!--\s*ad\s*-->.*?<!--\s*/ad\s*-->", re.DOTALL | re.I),
    
    # Social sharing
    (r"<div[^>]*(?:social-share|share-buttons)[^>]*>.*?</div>", re.DOTALL | re.I),
    
    # Script/Style (catch-all)
    (r"<script[^>]*>.*?</script>", re.DOTALL | re.I),
    (r"<style[^>]*>.*?</style>", re.DOTALL | re.I),
    (r"<noscript[^>]*>.*?</noscript>", re.DOTALL | re.I),
    
    # Comments
    (r"<!--.*?-->", re.DOTALL),
]

# Text-level patterns (applied after HTML stripping)
TEXT_NOISE_PATTERNS = [
    # Cookie policy text
    r"(?:we use cookies|this (?:site|website) uses cookies|cookie policy|accept (?:all )?cookies).*?(?:\.|$)",
    # Navigation remnants  
    r"(?:skip to (?:main )?content|jump to navigation|back to top)",
    # Social media prompts
    r"(?:follow us on|share (?:this|on)|like us on facebook)",
    # Copyright footers
    r"(?:all rights reserved|©\s*\d{4}|privacy policy\s*\|\s*terms)",
]


class Cleaner:
    """
    Text and HTML cleaning processor.
    
    🛡️ "CAR WASH" PIPELINE:
    Stage 1: Strip nav, footer, ads, cookies
    Stage 2: Extract main content (trafilatura)
    Stage 3: Normalize text (ftfy, whitespace)
    """
    
    def __init__(self):
        self.processed_hashes = set()
        
        # Compile regex patterns for performance
        self._compiled_patterns = [
            (re.compile(pattern), flags) 
            for pattern, flags in NOISE_PATTERNS
        ]
        self._text_patterns = [
            re.compile(pattern, re.I) 
            for pattern in TEXT_NOISE_PATTERNS
        ]
    
    def clean(self, file_path: str | Path) -> str:
        """
        Clean a document from file path.
        
        Args:
            file_path: Path to raw document
        
        Returns:
            Cleaned text content
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        content = file_path.read_bytes()
        
        # Determine file type
        if file_path.suffix.lower() in [".html", ".htm"]:
            return self.clean_html(content)
        else:
            return self.clean_text(content.decode("utf-8", errors="replace"))
    
    def clean_html(self, html: bytes | str) -> str:
        """
        Extract clean text from HTML.
        
        🛡️ CAR WASH PROCESS:
        1. Strip noise elements (nav, footer, cookies)
        2. Extract with trafilatura
        3. Apply text cleaning
        """
        if isinstance(html, bytes):
            html = html.decode("utf-8", errors="replace")
        
        # 🛡️ Stage 1: Strip noise elements
        html = self._strip_noise_elements(html)
        
        # 🛡️ Stage 2: Extract main content with trafilatura
        text = trafilatura.extract(
            html,
            include_tables=True,
            include_links=False,
            include_images=False,
            no_fallback=False,
            favor_recall=True,  # Get more content
        )
        
        if not text:
            # Fallback to basic HTML stripping
            text = self._strip_html_basic(html)
        
        # 🛡️ Stage 3: Clean text
        return self.clean_text(text)
    
    def _strip_noise_elements(self, html: str) -> str:
        """
        🛡️ Strip nav, footer, cookie banners, ads from HTML.
        
        This is the first stage of the "Car Wash" pipeline.
        """
        for pattern, flags in NOISE_PATTERNS:
            html = re.sub(pattern, "", html, flags=flags)
        return html
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text.
        
        Operations:
        1. Fix encoding issues (ftfy)
        2. Remove noise patterns (cookies, navigation)
        3. Normalize unicode
        4. Clean whitespace
        5. Remove control characters
        """
        if not text:
            return ""
        
        # Fix encoding issues
        text = ftfy.fix_text(text)
        
        # 🛡️ Remove text-level noise (cookie notices, etc.)
        text = self._remove_text_noise(text)
        
        # Normalize unicode
        text = self._normalize_unicode(text)
        
        # Clean whitespace
        text = self._clean_whitespace(text)
        
        # Remove control characters
        text = self._remove_control_chars(text)
        
        return text.strip()
    
    def _remove_text_noise(self, text: str) -> str:
        """Remove cookie notices, navigation text, etc."""
        for pattern in self._text_patterns:
            text = pattern.sub("", text)
        return text
    
    def _strip_html_basic(self, html: str) -> str:
        """Basic HTML tag stripping fallback."""
        # Remove script and style elements (already done, but double-check)
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.I)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.I)
        
        # Remove HTML tags
        html = re.sub(r"<[^>]+>", " ", html)
        
        # Decode HTML entities
        import html as html_module
        html = html_module.unescape(html)
        
        return html
    
    def _normalize_unicode(self, text: str) -> str:
        """Normalize unicode characters."""
        import unicodedata
        
        # Normalize to NFC form
        text = unicodedata.normalize("NFC", text)
        
        # Replace common problematic characters
        replacements = {
            "\u2018": "'",  # Left single quote
            "\u2019": "'",  # Right single quote
            "\u201c": '"',  # Left double quote
            "\u201d": '"',  # Right double quote
            "\u2013": "-",  # En dash
            "\u2014": "-",  # Em dash
            "\u2026": "...",  # Ellipsis
            "\u00a0": " ",  # Non-breaking space
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        return text
    
    def _clean_whitespace(self, text: str) -> str:
        """Normalize whitespace."""
        # Replace tabs with spaces
        text = text.replace("\t", " ")
        
        # Replace multiple spaces with single space
        text = re.sub(r" +", " ", text)
        
        # Replace multiple newlines with double newline
        text = re.sub(r"\n\s*\n+", "\n\n", text)
        
        return text
    
    def _remove_control_chars(self, text: str) -> str:
        """Remove control characters except newlines and tabs."""
        # Keep only printable characters, newlines, and tabs
        return "".join(
            char for char in text
            if char.isprintable() or char in "\n\t"
        )
    
    def is_duplicate(self, text: str) -> bool:
        """Check if text content is a duplicate."""
        content_hash = hashlib.sha256(text.encode()).hexdigest()
        
        if content_hash in self.processed_hashes:
            return True
        
        self.processed_hashes.add(content_hash)
        return False
    
    def clean_streaming(self, file_paths: list) -> Generator[tuple[str, str], None, None]:
        """
        🛡️ STREAMING: Clean files one at a time using generator.
        
        Args:
            file_paths: List of file paths to clean
        
        Yields:
            Tuple of (file_path, cleaned_text)
        """
        for path in file_paths:
            try:
                cleaned = self.clean(path)
                yield (str(path), cleaned)
            except Exception as e:
                logger.error(f"Failed to clean {path}: {e}")
                yield (str(path), "")


# Convenience functions
def clean_html(html: bytes | str) -> str:
    """Clean HTML content."""
    return Cleaner().clean_html(html)


def clean_text(text: str) -> str:
    """Clean text content."""
    return Cleaner().clean_text(text)


if __name__ == "__main__":
    # Quick test
    sample_html = """
    <html>
    <head><title>Test</title></head>
    <body>
        <nav><a href="/">Home</a> | <a href="/about">About</a></nav>
        <div class="cookie-banner">We use cookies. Accept all.</div>
        <h1>Sample Document</h1>
        <p>This is the main content with "smart quotes" and an em—dash.</p>
        <p>In 2023, ACME Corp produced 15,000 tonnes of fly ash.</p>
        <footer>© 2023 All Rights Reserved | Privacy Policy</footer>
    </body>
    </html>
    """
    
    cleaner = Cleaner()
    result = cleaner.clean_html(sample_html)
    print("🛡️ Cleaned text (nav/footer/cookies stripped):")
    print(result)
