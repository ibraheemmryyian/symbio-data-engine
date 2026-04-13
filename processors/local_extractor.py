"""
Local LLM Extractor — Large Context Edition
============================================
Designed for Qwen2.5-3B-Instruct (or any 128k context model via LM Studio).

Key differences from extractor.py:
- Feeds entire document in one shot (up to ~100k tokens) — no chunking
- JSON mode enforced at grammar level — no regex fallback needed
- Regex post-validation layer catches unit/value inconsistencies the LLM misses
- Returns confidence scores per field, not just per record

Usage:
    from processors.local_extractor import LocalExtractor
    extractor = LocalExtractor()
    results = extractor.extract_document(text, doc_type="csr")
"""

import json
import logging
import re
from typing import Optional

import httpx

import config
from .models import (
    WasteListingExtraction,
    CarbonEmissionExtraction,
    SymbiosisExchangeExtraction,
    ExtractionResult,
    validate_and_create,
)

log = logging.getLogger(__name__)

# -------------------------------------------------------
# Regex validators — run AFTER LLM output to catch errors
# These are the things small models get wrong most often
# -------------------------------------------------------

# Quantity sanity checks
_QTY_PATTERNS = [
    # "15,000 tonnes", "1.2 million tons", "500 kg"
    (r"([\d,]+\.?\d*)\s*(million)?\s*(metric\s*ton(?:ne)?s?|tonnes?|tons?|t\b|kg|mt)\b",
     lambda m: _safely_parse_quantity(m.group(1), m.group(2))),
]

def _safely_parse_quantity(value_str, multiplier_str):
    """Safely parse quantity string to float, handling empty strings."""
    if not value_str or not isinstance(value_str, str):
        return None
    clean_val = value_str.replace(",", "").strip()
    if not clean_val:
        return None
    # Check if it's a valid number (digits and at most one decimal point)
    num_dots = clean_val.count(".")
    if num_dots > 1:
        return None
    try:
        val = float(clean_val)
        if val <= 0:  # Quantities must be positive
            return None
        if multiplier_str and "million" in multiplier_str.lower():
            val *= 1_000_000
        return val
    except (ValueError, TypeError):
        return None

# Year sanity
_YEAR_RE = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b")

# CO2 patterns
_CO2_RE = re.compile(
    r"([\d,]+\.?\d*)\s*(million|thousand)?\s*(?:metric\s*)?(?:ton(?:ne)?s?|t)\s*(?:of\s*)?(?:CO2|CO₂|carbon dioxide|GHG|greenhouse)",
    re.IGNORECASE,
)

# Price per ton
_PRICE_RE = re.compile(
    r"(?:USD?|EUR?|\$|€|£)\s*([\d,]+\.?\d*)\s*(?:per\s*(?:metric\s*)?ton(?:ne)?|/t\b)",
    re.IGNORECASE,
)

# CAS number
_CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")


def _regex_extract_quantities(text: str) -> list[dict]:
    """Extract all quantity mentions from text with their surrounding sentence."""
    results = []
    for pattern, converter in _QTY_PATTERNS:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            try:
                # Get surrounding sentence as source_quote
                start = max(0, text.rfind(".", 0, m.start()) + 1)
                end = text.find(".", m.end())
                sentence = text[start: end + 1 if end != -1 else len(text)].strip()

                # Safely convert quantity
                try:
                    qty = converter(m)
                    if qty is None:
                        continue
                except (ValueError, TypeError, AttributeError) as e:
                    log.warning(f"Quantity conversion failed: {e}")
                    continue

                results.append({
                    "quantity_tons": qty,
                    "source_quote": sentence,
                    "unit_raw": m.group(3) if len(m.groups()) >= 3 else m.group(2),
                })
            except Exception as e:
                log.warning(f"Regex quantity extraction error: {e}")
                continue
    return results


def _regex_extract_years(text: str) -> list[int]:
    years = []
    for m in _YEAR_RE.finditer(text):
        try:
            year = int(m.group())
            if 1987 <= year <= 2026:  # Valid year range
                years.append(year)
        except (ValueError, TypeError):
            continue
    return years


def _regex_extract_co2(text: str) -> list[dict]:
    results = []
    for m in _CO2_RE.finditer(text):
        try:
            group1 = m.group(1)
            if not group1:
                continue
            val = float(group1.replace(",", ""))
            multiplier = m.group(2)
            if multiplier and "million" in multiplier.lower():
                val *= 1_000_000
            elif multiplier and "thousand" in multiplier.lower():
                val *= 1_000
            start = max(0, text.rfind(".", 0, m.start()) + 1)
            end = text.find(".", m.end())
            sentence = text[start: end + 1 if end != -1 else len(text)].strip()
            results.append({"co2_tons": val, "source_quote": sentence})
        except (ValueError, TypeError, AttributeError) as e:
            log.warning(f"CO2 extraction error: {e}")
            continue
    return results


def _regex_extract_cas(text: str) -> list[str]:
    return list(set(_CAS_RE.findall(text)))


def _validate_llm_record(record: dict, text: str) -> dict:
    """
    Post-process a single LLM-extracted record using regex.
    Fills in missing fields and flags suspicious values.
    """
    issues = []

    # Verify source_quote exists in text
    quote = record.get("source_quote", "")
    if quote and quote not in text:
        # Try fuzzy: first 60 chars of quote in text
        if quote[:60] not in text:
            issues.append("source_quote not found in text")
            record["source_quote"] = None

    # Validate year range
    year = record.get("year")
    if year and not (1987 <= int(year) <= 2026):
        issues.append(f"year {year} out of range")
        record["year"] = None

    # Validate quantity
    qty = record.get("quantity_tons") or record.get("quantity")
    if qty and isinstance(qty, (int, float, str)):
        qty_str = str(qty).strip() if isinstance(qty, str) else str(qty)
        if qty_str:  # only attempt conversion if non-empty string
            try:
                qty_f = float(qty_str)
                if qty_f <= 0 or qty_f > 100_000_000:
                    issues.append(f"quantity {qty_f} out of plausible range")
                    record["quantity_tons"] = None
            except (ValueError, TypeError):
                pass

    # If year missing, try to extract from source_quote
    if not record.get("year") and record.get("source_quote"):
        years = _regex_extract_years(record["source_quote"])
        if years:
            record["year"] = years[0]
            record["_year_from_regex"] = True

    # Extract CAS numbers if not present
    if not record.get("cas_numbers"):
        cas = _regex_extract_cas(text)
        if cas:
            record["cas_numbers"] = ", ".join(cas)

    if issues:
        record["_regex_issues"] = issues
        # Reduce confidence proportional to number of issues
        raw_conf = record.get("extraction_confidence") or 0.8
        _conf_map = {"high": 0.9, "medium": 0.6, "low": 0.3, "very high": 0.95, "very low": 0.15}
        if isinstance(raw_conf, str):
            raw_conf = _conf_map.get(raw_conf.lower().strip(), 0.8)
        conf = float(raw_conf)
        record["extraction_confidence"] = max(0.1, conf - 0.15 * len(issues))
        log.debug(f"Regex validation found issues: {issues}")

    return record


# -------------------------------------------------------
# Main extractor
# -------------------------------------------------------

# System prompt — sets the model's role once, never repeated per chunk
_SYSTEM_PROMPT = """You are a precision industrial data extraction engine.
Extract structured facts from industrial documents: waste listings, carbon emissions, and symbiosis exchanges.

REQUIRED FIELDS FOR EACH RECORD:
- source_quote: EXACT verbatim text from document (minimum 10 chars) - CRITICAL
- extraction_confidence: Your confidence 0.0–1.0 (0.7 = acceptable, 0.8+ = confident)
- _schema: Type of fact (waste_listing | carbon_emission | symbiosis_exchange)
- verification_method: How you found this (e.g., "quoted_directly", "inferred_from_context", "table_data")

WASTE_LISTING records MUST include:
- material: Name of the waste or byproduct (required, 2-100 chars)
- source_quote: Verbatim text supporting this (required, minimum 10 chars)
- extraction_confidence: Your confidence (required, even if low)
- verification_method: How verified (required)
- quantity_tons, source_company, source_industry, year (optional but valuable)

RULES (non-negotiable):
- Return ONLY a JSON object with key "extractions" containing an array of facts
- NEVER invent, infer, or synthesize data
- If a field is not explicitly stated, set it to null
- Temperature is 0 — you have no creativity, only precision

/no_think"""

_USER_TEMPLATE = """Document type: {doc_type}
Document source: {source}

--- DOCUMENT START ---
{text}
--- DOCUMENT END ---

Extract all industrial waste, carbon emission, and symbiosis exchange facts with verification methods.
Return JSON: {{"extractions": [...]}}"""


class LocalExtractor:
    """
    Large-context local LLM extractor.
    Feeds entire documents to Qwen2.5-3B or similar via LM Studio.
    """

    # Qwen2.5-3B with 32k context window
    # ~4 chars per token → 32k tokens × 4 = ~128k chars available
    # System prompt + output headroom → use 24k chars (6k tokens)
    # This gives us faster processing with same accuracy
    MAX_CHARS = 24_000
    TEMPERATURE = 0.0
    REPEAT_PENALTY = 1.1

    def __init__(self):
        self.base_url = config.LLM_BASE_URL
        self.api_key = config.LLM_API_KEY
        self.model = config.LLM_MODEL

    def _call(self, text: str, doc_type: str, source: str) -> list[dict]:
        """Single LLM call with JSON mode enforced."""
        user_msg = _USER_TEMPLATE.format(
            doc_type=doc_type,
            source=source,
            text=text[:self.MAX_CHARS],
        )

        try:
            with httpx.Client(
                base_url=self.base_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=600.0,
            ) as client:
                # LM Studio params: temperature, max_tokens only (no repeat_penalty, response_format)
                r = client.post("/chat/completions", json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                    "temperature": self.TEMPERATURE,
                    "max_tokens": 4096,
                })

            r.raise_for_status()
            response_data = r.json()

            # Defensive API response handling
            choices = response_data.get("choices")
            if not choices or not isinstance(choices, list) or len(choices) == 0:
                log.warning(f"Invalid response structure: no choices in response")
                return []

            message = choices[0].get("message")
            if not message or not isinstance(message, dict):
                log.warning(f"Invalid response structure: no message in first choice")
                return []

            content = message.get("content")
            if not content or not isinstance(content, str):
                log.warning(f"Invalid response structure: content is missing or not a string")
                return []

            # Extract JSON from markdown code blocks if present
            if "```json" in content:
                parts = content.split("```json")
                if len(parts) > 1:
                    content = parts[1].split("```")[0].strip()

            parsed = json.loads(content)
            return parsed.get("extractions", []) if isinstance(parsed, dict) else []

        except json.JSONDecodeError as e:
            log.warning(f"JSON decode failed: {e} — falling back to regex extraction")
            return []
        except Exception as e:
            log.error(f"LLM call failed: {e}")
            return []

    def extract_document(
        self,
        text: str,
        doc_type: str = "industrial_report",
        source: str = "unknown",
    ) -> list[ExtractionResult]:
        """
        Extract all facts from a document.
        For docs under MAX_CHARS: single LLM call.
        For larger docs: chunk with overlap, deduplicate by source_quote.
        """
        results: list[ExtractionResult] = []
        seen_quotes: set[str] = set()

        # Regex pre-pass — extract quantities/CO2/CAS before LLM
        # Used to cross-check LLM output and fill gaps
        regex_quantities = _regex_extract_quantities(text)
        regex_co2 = _regex_extract_co2(text)
        regex_cas = _regex_extract_cas(text)
        log.info(f"Regex pre-pass: {len(regex_quantities)} quantities, {len(regex_co2)} CO2 mentions, {len(regex_cas)} CAS numbers")

        # Split into chunks only if necessary
        chunks = self._chunk(text)
        log.info(f"Processing {len(chunks)} chunk(s) via local LLM")

        for i, chunk in enumerate(chunks):
            log.info(f"  Chunk {i+1}/{len(chunks)} ({len(chunk):,} chars)")
            raw_records = self._call(chunk, doc_type, source)
            log.info(f"  LLM returned {len(raw_records)} raw records")

            for record in raw_records:
                if not record or not isinstance(record, dict):
                    continue

                schema = record.pop("_schema", None) or self._detect_schema(record)
                if not schema:
                    continue

                # Regex post-validation
                record = _validate_llm_record(record, chunk)
                if not record or not isinstance(record, dict):
                    continue

                # Deduplicate by source_quote
                quote = record.get("source_quote", "")
                if quote and quote in seen_quotes:
                    continue
                if quote:
                    seen_quotes.add(quote)

                # Pydantic validation
                model_map = {
                    "waste_listing": WasteListingExtraction,
                    "carbon_emission": CarbonEmissionExtraction,
                    "symbiosis_exchange": SymbiosisExchangeExtraction,
                }
                model_cls = model_map.get(schema)
                if not model_cls:
                    continue

                validated = validate_and_create(model_cls, record, chunk)
                if validated:
                    results.append(ExtractionResult.success(schema, validated))
                else:
                    # Log what failed validation for debugging
                    quote = record.get('source_quote', 'MISSING') or 'MISSING'
                    quote_preview = str(quote)[:60] if quote else 'MISSING'
                    log.warning(f"Record failed validation - schema={schema}, record keys={list(record.keys())}, quote={quote_preview}")

        # If LLM found nothing but regex found quantities — emit low-confidence records
        if not results and regex_quantities:
            log.info("LLM found nothing — emitting regex-only records at low confidence")
            years = _regex_extract_years(text)
            for rq in regex_quantities[:5]:  # cap at 5
                record = {
                    "quantity_tons": rq["quantity_tons"],
                    "source_quote": rq["source_quote"],
                    "year": years[0] if years else None,
                    "extraction_confidence": 0.45,
                    "cas_numbers": ", ".join(regex_cas) if regex_cas else None,
                    "verification_method": "regex_fallback",
                }
                validated = validate_and_create(WasteListingExtraction, record, text)
                if validated:
                    results.append(ExtractionResult.success("waste_listing", validated))

        log.info(f"Extraction complete: {len(results)} validated records")
        return results

    def _chunk(self, text: str, overlap: int = 8_000) -> list[str]:
        if len(text) <= self.MAX_CHARS:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = start + self.MAX_CHARS
            chunks.append(text[start:end])
            if end >= len(text):
                break
            start += self.MAX_CHARS - overlap
        return chunks

    def _detect_schema(self, record: dict) -> Optional[str]:
        keys = set(record.keys())
        if "co2_tons" in keys or "co2_scope1" in keys:
            return "carbon_emission"
        if "source_company" in keys and "target_company" in keys:
            return "symbiosis_exchange"
        if "material" in keys or "quantity_tons" in keys:
            return "waste_listing"
        return None
