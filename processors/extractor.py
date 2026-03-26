"""
Symbio Data Engine - Structured Data Extractor
==============================================
Third stage of the processing pipeline.

🛡️ ZERO HALLUCINATION POLICY:
1. Temperature 0.0 - No creativity/randomness
2. Citation Rule - Every fact must have source_quote
3. Pydantic Validation - Strict type/bound checking
4. Quote Verification - Reject if quote not in source
"""

import gc
import json
import logging
import re
from typing import Optional, Generator

import config
from .models import (
    WasteListingExtraction,
    CarbonEmissionExtraction,
    SymbiosisExchangeExtraction,
    ExtractionResult,
    validate_and_create,
    CitedRecord,
)

logger = logging.getLogger(__name__)


# Extraction schemas for different document types
# 🛡️ NOTE: source_quote and extraction_confidence are REQUIRED
EXTRACTION_SCHEMAS = {
    "waste_listing": {
        "material": "string - name of the waste material",
        "quantity": "number - amount in original units",
        "unit": "string - unit of measurement",
        "price": "number - price if available",
        "currency": "string - currency code",
        "company": "string - source company name",
        "location": "string - location/address",
        "year": "number - year of the listing",
        "source_quote": "string - EXACT sentence from text (REQUIRED)",
        "extraction_confidence": "number 0-1 (REQUIRED)",
    },
    "carbon_emission": {
        "company": "string - company name",
        "facility": "string - facility name if available",
        "year": "number - reporting year",
        "co2_total": "number - total CO2 emissions in tons",
        "co2_scope1": "number - Scope 1 emissions",
        "co2_scope2": "number - Scope 2 emissions",
        "co2_scope3": "number - Scope 3 emissions",
        "methodology": "string - reporting methodology used",
        "source_quote": "string - EXACT sentence from text (REQUIRED)",
        "extraction_confidence": "number 0-1 (REQUIRED)",
    },
    "symbiosis_exchange": {
        "source_company": "string - company providing the waste",
        "target_company": "string - company receiving the waste",
        "material": "string - material being exchanged",
        "volume": "number - volume in original units",
        "unit": "string - unit of measurement",
        "year": "number - year of the exchange",
        "eco_park": "string - name of eco-industrial park if applicable",
        "source_quote": "string - EXACT sentence from text (REQUIRED)",
        "extraction_confidence": "number 0-1 (REQUIRED)",
    },
}


class Extractor:
    """
    Structured data extraction processor.
    
    🛡️ ZERO HALLUCINATION POLICY ENFORCED:
    - Temperature 0.0 for all LLM calls
    - source_quote required for every extraction
    - Pydantic validation on all outputs
    - Citation verification against original text
    """
    
    # 🛡️ LLM temperature (MUST be 0 for zero hallucination)
    LLM_TEMPERATURE = 0.0
    
    def __init__(self, use_llm: bool = True):
        """
        Initialize extractor.
        
        Args:
            use_llm: Whether to use LLM for extraction
        """
        self.use_llm = use_llm and bool(config.LLM_API_KEY)
        self.client = None
        
        if self.use_llm:
            self._init_llm_client()
    
    def _init_llm_client(self):
        """Initialize LLM client."""
        try:
            import httpx
            self.client = httpx.Client(
                base_url=config.LLM_BASE_URL,
                headers={"Authorization": f"Bearer {config.LLM_API_KEY}"},
                timeout=600.0,
            )
            logger.info("LLM client initialized (temperature=0.0)")
        except Exception as e:
            logger.warning(f"Failed to initialize LLM client: {e}")
            self.use_llm = False
    
    def extract(
        self,
        text: str,
        doc_type: str = None,
        schema: str = None,
    ) -> ExtractionResult:
        """
        Extract structured data from text with STRICT validation.
        
        🛡️ ZERO HALLUCINATION POLICY:
        1. LLM extracts with source_quote requirement
        2. Pydantic validates all fields
        3. Citation verified against original text
        4. Record REJECTED if any check fails
        
        Args:
            text: Cleaned text content
            doc_type: Document type hint
            schema: Extraction schema to use
        
        Returns:
            ExtractionResult with validated data or rejection reason
        """
        # Determine schema
        if schema is None:
            schema = self._detect_schema(text, doc_type)
        
        schema_def = EXTRACTION_SCHEMAS.get(schema, EXTRACTION_SCHEMAS["waste_listing"])
        
        # COST OPTIMIZATION: Try rule-based extraction FIRST (Free)
        # If it yields valid data, skip the LLM entirely.
        raw_data = self._extract_with_rules(text, schema)
        if raw_data:
            result = self._validate_extraction(raw_data, text, schema)
            if result.is_valid:
                logger.info("Rule-based extraction successful - Skipped LLM")
                return result
        
        # If rules fail, try LLM (Costly but smarter)
        if self.use_llm:
            raw_data = self._extract_with_llm(text, schema_def, schema)
            if raw_data:
                # Validate with Pydantic and verify citation
                result = self._validate_extraction(raw_data, text, schema)
                if result.is_valid:
                    return result
        
        return ExtractionResult.failure(schema, "No data extracted")
    
    def extract_multiple(
        self,
        text: str,
        doc_type: str = None,
        chunk_size: int = 60000,
        overlap: int = 5000,
    ) -> list[ExtractionResult]:
        """
        🛡️ Extract MULTIPLE facts using a SLIDING WINDOW.
        
        Processes the entire text in overlapping chunks to ensure no data is missed
        even in very large documents (>200 pages).
        """
        results = []
        
        # 1. Chunk the text
        chunks = []
        if len(text) <= chunk_size:
            chunks = [text]
        else:
            # Overlapping window
            start = 0
            while start < len(text):
                end = start + chunk_size
                chunks.append(text[start:end])
                if end >= len(text):
                    break
                start += (chunk_size - overlap)
        
        logger.info(f"📄 Processing {len(chunks)} chunks for document.")

        # 2. Process each chunk
        for i, chunk in enumerate(chunks):
            if self.use_llm:
                logger.info(f"   - Processing chunk {i+1}/{len(chunks)} ({len(chunk)} chars)")
                raw_extractions = self._extract_multiple_with_llm(chunk)
                
                for raw_data in raw_extractions:
                    schema = raw_data.pop("_schema", "waste_listing")
                    result = self._validate_extraction(raw_data, chunk, schema)
                    
                    if result.is_valid:
                        # 🛡️ DEDUPLICATION: Avoid adding the same fact from overlapping chunks
                        # We use source_quote as a simple fingerprint
                        if not any(r.data.get("source_quote") == result.data.get("source_quote") for r in results):
                            results.append(result)
                    else:
                        logger.debug(f"Fact rejected: {result.rejection_reason}")
        
        # 3. Fallback to rule-based if LLM found nothing
        if not results:
            single_result = self.extract(text, doc_type)
            if single_result.is_valid:
                results.append(single_result)
        
        return results
    
    def _extract_multiple_with_llm(self, text: str) -> list[dict]:
        """
        🛡️ LLM extraction for MULTIPLE facts from a document.

        Returns a list of raw extraction dicts, each to be validated individually.
        """
        # PROPRIETARY: Extraction prompt redacted for security
        # This prompt enforces zero-hallucination extraction with mandatory source citations
        prompt = self._build_extraction_prompt(text)

        try:
            # 🛡️ STATELESS REQUEST: Use a fresh client for every chunk to prevent context accumulation
            import httpx
            with httpx.Client(
                base_url=config.LLM_BASE_URL,
                headers={
                    "Authorization": f"Bearer {config.LLM_API_KEY}",
                    "Connection": "close" # Force socket close
                },
                timeout=600.0,
            ) as client:
                response = client.post(
                    "/chat/completions",
                    json={
                        "model": config.LLM_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": self.LLM_TEMPERATURE,
                        "repeat_penalty": 1.15,
                        "response_format": {"type": "json_object"},
                    },
                )
            
            if response.status_code != 200:
                logger.error(f"LLM API Error: {response.status_code} - {response.text}")
                return []
            
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            parsed = self._parse_json_response(content)
            
            # Ensure it's a list
            if isinstance(parsed, dict):
                return [parsed]
            elif isinstance(parsed, list):
                return parsed
            else:
                return []
                
        except Exception as e:
            logger.warning(f"Multi-extraction failed: {e}")
            return []
    
    def _detect_schema(self, text: str, doc_type: str = None) -> str:
        """Detect appropriate schema based on content."""
        text_lower = text.lower()
        
        # Check for carbon/emissions content
        if any(word in text_lower for word in ["co2", "carbon", "emission", "ghg", "greenhouse"]):
            return "carbon_emission"
        
        # Check for symbiosis/exchange content
        if any(word in text_lower for word in ["symbiosis", "exchange", "industrial park", "eco-park"]):
            return "symbiosis_exchange"
        
        # Default to waste listing
        return "waste_listing"
    
    def _validate_extraction(
        self,
        raw_data: dict,
        original_text: str,
        schema: str,
    ) -> ExtractionResult:
        """
        Validate extraction with Pydantic and verify citation.
        
        🛡️ ZERO HALLUCINATION: Record rejected if:
        - Pydantic validation fails
        - source_quote not found in original text
        """
        model_map = {
            "waste_listing": WasteListingExtraction,
            "carbon_emission": CarbonEmissionExtraction,
            "symbiosis_exchange": SymbiosisExchangeExtraction,
        }
        
        model_class = model_map.get(schema)
        if not model_class:
            return ExtractionResult.failure(schema, f"Unknown schema: {schema}")
        
        record = validate_and_create(model_class, raw_data, original_text)
        
        if record:
            return ExtractionResult.success(schema, record)
        else:
            return ExtractionResult.failure(schema, "Citation not found in source text")
    
    def _extract_with_llm(
        self,
        text: str,
        schema: dict,
        schema_name: str,
    ) -> Optional[dict]:
        """
        Extract using LLM with ZERO HALLUCINATION settings.
        
        🛡️ STRICT REQUIREMENTS:
        - temperature=0.0 (NO randomness)
        - source_quote REQUIRED in output
        - extraction_confidence REQUIRED
        """
        if not self.client:
            return None
        
        # PROPRIETARY: Extraction prompt redacted for security
        # This prompt enforces zero-hallucination extraction with mandatory source citations
        prompt = self._build_single_extraction_prompt(text, schema)

        try:
            response = self.client.post(
                "/chat/completions",
                json={
                    "model": config.LLM_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": self.LLM_TEMPERATURE,
                    "repeat_penalty": 1.15,
                    "response_format": {"type": "json_object"},
                },
            )
            
            if response.status_code != 200:
                logger.warning(f"LLM API error: {response.status_code}")
                return None
            
            data = response.json()
            content = data["choices"][0]["message"]["content"]
            
            # Parse JSON from response
            return self._parse_json_response(content)
            
        except Exception as e:
            logger.warning(f"LLM extraction failed: {e}")
            return None
    
    def _parse_json_response(self, content: str) -> Optional[dict]:
        """Parse JSON from LLM response."""
        # Try direct parsing
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        
        # Try to extract JSON from markdown code block
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        
        # Try to find JSON list in text
        json_match = re.search(r"\[[\s\S]*\]", content)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass

        # Try to find JSON object in text
        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        
        return None
    
    def _build_extraction_prompt(self, text: str) -> str:
        """
        PROPRIETARY: Multi-extraction prompt template.
        The actual prompt is kept locally and not committed to GitHub.
        """
        return f"[PROPRIETARY PROMPT - REDACTED]\n\nText:\n{text[:1000]}..."

    def _build_single_extraction_prompt(self, text: str, schema: dict) -> str:
        """
        PROPRIETARY: Single-extraction prompt template.
        The actual prompt is kept locally and not committed to GitHub.
        """
        return f"[PROPRIETARY PROMPT - REDACTED]\n\nSchema: {list(schema.keys())}\n\nText:\n{text[:1000]}..."

    def _extract_with_rules(self, text: str, schema: str) -> dict:
        """Extract using rule-based patterns (fallback)."""
        if schema == "waste_listing":
            return self._extract_waste_listing(text)
        elif schema == "carbon_emission":
            return self._extract_carbon_emission(text)
        elif schema == "symbiosis_exchange":
            return self._extract_symbiosis_exchange(text)
        else:
            return {}
    
    def _extract_waste_listing(self, text: str) -> dict:
        """Rule-based extraction for waste listings."""
        result = {
            "material": None,
            "quantity": None,
            "unit": None,
            "company": None,
            "year": None,
            "source_quote": None,
            "extraction_confidence": 0.6,  # Rule-based = lower confidence
        }
        
        # Extract quantities with the surrounding sentence as quote
        qty_pattern = r"([^.]*?([\d,]+\.?\d*)\s*(metric\s*tons?|tonnes?|tons?|kg|mt)[^.]*\.)"
        qty_match = re.search(qty_pattern, text, re.IGNORECASE)
        if qty_match:
            result["source_quote"] = qty_match.group(1).strip()
            result["quantity"] = float(qty_match.group(2).replace(",", ""))
            result["unit"] = qty_match.group(3).lower()
        
        # Extract year
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if year_match:
            result["year"] = int(year_match.group())
        
        # Extract materials (simple keyword match)
        materials = ["steel", "iron", "copper", "aluminum", "plastic", "paper", "slag", "fly ash"]
        for mat in materials:
            if mat in text.lower():
                result["material"] = mat
                break
        
        return result
    
    def _extract_carbon_emission(self, text: str) -> dict:
        """Rule-based extraction for carbon emissions."""
        result = {
            "company": None,
            "year": None,
            "co2_tons": None,
            "source_quote": None,
            "extraction_confidence": 0.6,
        }
        
        # Extract CO2 amounts with surrounding sentence
        co2_pattern = r"([^.]*?([\d,]+\.?\d*)\s*(?:million\s*)?(?:tonnes?|tons?|mt)\s*(?:of\s*)?(?:CO2|carbon)[^.]*\.)"
        co2_match = re.search(co2_pattern, text, re.IGNORECASE)
        if co2_match:
            result["source_quote"] = co2_match.group(1).strip()
            value = float(co2_match.group(2).replace(",", ""))
            if "million" in co2_match.group(1).lower():
                value *= 1_000_000
            result["co2_tons"] = value
        
        # Extract year
        year_match = re.search(r"\b(20[0-2]\d)\b", text)
        if year_match:
            result["year"] = int(year_match.group())
        
        return result
    
    def _extract_symbiosis_exchange(self, text: str) -> dict:
        """Rule-based extraction for symbiosis exchanges."""
        result = {
            "source_company": None,
            "target_company": None,
            "material": None,
            "volume": None,
            "eco_park": None,
            "year": None,
            "source_quote": None,
            "extraction_confidence": 0.6,
        }
        
        # Check for known eco-parks and capture sentence
        parks = ["kalundborg", "ulsan", "tianjin", "kawasaki"]
        for park in parks:
            if park in text.lower():
                result["eco_park"] = park.title()
                # Find sentence containing the park name
                park_pattern = rf"([^.]*{park}[^.]*\.)"
                park_match = re.search(park_pattern, text, re.IGNORECASE)
                if park_match:
                    result["source_quote"] = park_match.group(1).strip()
                break
        
        # Extract volume
        vol_pattern = r"([\d,]+\.?\d*)\s*(tonnes?|tons?|mt)\b"
        vol_match = re.search(vol_pattern, text, re.IGNORECASE)
        if vol_match:
            result["volume"] = float(vol_match.group(1).replace(",", ""))
        
        # Extract year
        year_match = re.search(r"\b(19|20)\d{2}\b", text)
        if year_match:
            result["year"] = int(year_match.group())
        
        return result


# Convenience function
def extract_structured_data(text: str, schema: str = None) -> ExtractionResult:
    """Extract structured data from text with validation."""
    return Extractor().extract(text, schema=schema)


if __name__ == "__main__":
    # Quick test with Zero Hallucination verification
    sample = """
    In 2023, the Kalundborg Symbiosis facilitated the exchange of 15,000 tonnes 
    of fly ash from Asnæs Power Station to Gyproc A/S for use in plasterboard 
    production. This resulted in CO2 savings of approximately 5,000 tonnes.
    """
    
    extractor = Extractor(use_llm=False)
    result = extractor.extract(sample)
    
    print("🛡️ Zero Hallucination Extraction:")
    print(f"  Valid: {result.is_valid}")
    print(f"  Requires Review: {result.requires_review}")
    if result.data:
        print(f"  Quote: {result.data.get('source_quote', 'N/A')[:80]}...")
    else:
        print(f"  Rejection: {result.rejection_reason}")
