"""
Symbio Data Engine - Pydantic Models
====================================
STRICT DATA INTEGRITY - Zero Hallucination Policy

All extracted data MUST pass through these models.
Any validation failure = record discarded.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================
# VALIDATION CONSTANTS
# ============================================
MIN_YEAR = 1970
MAX_YEAR = 2026
MIN_CONFIDENCE_FOR_AUTO_ACCEPT = 0.9
MIN_QUOTE_LENGTH = 10


# ============================================
# BASE MODEL WITH CITATION REQUIREMENT
# ============================================
class CitedRecord(BaseModel):
    """
    Base model enforcing the Citation Rule.
    
    Every extracted record MUST have a source_quote that
    can be verified against the original text.
    """
    source_quote: str = Field(
        ...,
        min_length=MIN_QUOTE_LENGTH,
        description="Exact quote from source text that supports this extraction"
    )
    extraction_confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="LLM confidence in extraction (0-1)"
    )
    requires_review: bool = Field(
        default=False,
        description="Flag for manual review if confidence < 0.9"
    )
    
    @field_validator("extraction_confidence")
    @classmethod
    def check_confidence_threshold(cls, v):
        """Auto-flag low confidence records for review."""
        return v
    
    @model_validator(mode="after")
    def flag_low_confidence(self):
        """Flag for review if confidence below threshold."""
        if self.extraction_confidence < MIN_CONFIDENCE_FOR_AUTO_ACCEPT:
            self.requires_review = True
        return self


# ============================================
# SYMBIOFLOWS: Waste Listings
# ============================================
class WasteListingExtraction(CitedRecord):
    """
    Validated waste listing extraction.
    
    All fields are strictly typed and bounded.
    """
    material: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Name of the waste material"
    )
    material_category: Optional[str] = Field(
        None,
        max_length=50,
        description="Category: metals, plastics, organics, etc."
    )
    quantity_tons: Optional[float] = Field(
        None,
        gt=0,
        le=100_000_000,  # 100 million tons max (reasonable upper bound)
        description="Quantity in metric tons"
    )
    original_quantity: Optional[float] = Field(
        None,
        description="Original quantity before unit conversion"
    )
    original_unit: Optional[str] = Field(
        None,
        description="Original unit before conversion"
    )
    price_per_ton: Optional[float] = Field(
        None,
        gt=0,
        le=1_000_000,  # $1M per ton max
        description="Price per metric ton"
    )
    currency: Optional[str] = Field(
        None,
        pattern=r"^[A-Z]{3}$",
        description="ISO 4217 currency code"
    )
    source_company: Optional[str] = Field(
        None,
        max_length=255,
        description="Company name"
    )
    source_location: Optional[str] = Field(
        None,
        max_length=255,
        description="Location/address"
    )
    quality_grade: Optional[str] = Field(
        None,
        pattern=r"^[A-C]$|^contaminated$",
        description="Quality grade: A, B, C, or contaminated"
    )
    year: Optional[int] = Field(
        None,
        ge=MIN_YEAR,
        le=MAX_YEAR,
        description="Year of listing"
    )
    
    @field_validator("material")
    @classmethod
    def normalize_material(cls, v):
        """Normalize material name."""
        return v.strip().lower() if v else v


# ============================================
# SYMBIOTRUST: Carbon Emissions
# ============================================
class CarbonEmissionExtraction(CitedRecord):
    """
    Validated carbon emission extraction.
    """
    company: str = Field(
        ...,
        min_length=2,
        max_length=255,
        description="Company name"
    )
    facility: Optional[str] = Field(
        None,
        max_length=255,
        description="Facility name"
    )
    year: int = Field(
        ...,
        ge=MIN_YEAR,
        le=MAX_YEAR,
        description="Reporting year"
    )
    co2_tons: Optional[float] = Field(
        None,
        gt=0,
        le=10_000_000_000,  # 10 billion tons max (global scale)
        description="Total CO2 emissions in metric tons"
    )
    co2_scope1: Optional[float] = Field(
        None,
        ge=0,
        description="Scope 1 (direct) emissions"
    )
    co2_scope2: Optional[float] = Field(
        None,
        ge=0,
        description="Scope 2 (indirect/energy) emissions"
    )
    co2_scope3: Optional[float] = Field(
        None,
        ge=0,
        description="Scope 3 (value chain) emissions"
    )
    co2_avoided_tons: Optional[float] = Field(
        None,
        ge=0,
        description="CO2 avoided through circular economy"
    )
    methodology: Optional[str] = Field(
        None,
        max_length=100,
        description="Reporting methodology (GHG Protocol, ISO 14064, etc.)"
    )
    
    @model_validator(mode="after")
    def validate_scope_totals(self):
        """Validate that scopes don't exceed total if all provided."""
        if self.co2_tons and self.co2_scope1 and self.co2_scope2:
            scope_sum = (self.co2_scope1 or 0) + (self.co2_scope2 or 0)
            # Allow 10% tolerance for rounding
            if scope_sum > self.co2_tons * 1.1:
                self.requires_review = True
        return self


# ============================================
# RESEARCH: Symbiosis Exchanges
# ============================================
class SymbiosisExchangeExtraction(CitedRecord):
    """
    Validated symbiosis exchange extraction.
    """
    eco_park: Optional[str] = Field(
        None,
        max_length=100,
        description="Name of eco-industrial park"
    )
    year: int = Field(
        ...,
        ge=MIN_YEAR,
        le=MAX_YEAR,
        description="Year of exchange"
    )
    source_company: str = Field(
        ...,
        min_length=2,
        max_length=255,
        description="Company providing waste/byproduct"
    )
    target_company: str = Field(
        ...,
        min_length=2,
        max_length=255,
        description="Company receiving material"
    )
    material: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Material being exchanged"
    )
    volume_tons: Optional[float] = Field(
        None,
        gt=0,
        le=100_000_000,
        description="Volume in metric tons"
    )
    exchange_type: Optional[str] = Field(
        None,
        pattern=r"^(waste|byproduct|energy|water|steam|heat)$",
        description="Type of exchange"
    )
    co2_savings_tons: Optional[float] = Field(
        None,
        ge=0,
        description="Estimated CO2 savings"
    )


# ============================================
# VALIDATION HELPERS
# ============================================
def validate_citation(record: CitedRecord, original_text: str) -> bool:
    """
    Validate that the source_quote exists in the original text.
    
    This is the core of the Zero Hallucination Policy:
    If the quote cannot be found, the record is REJECTED.
    
    Args:
        record: Extracted record with source_quote
        original_text: Original text the extraction came from
    
    Returns:
        True if quote is found, False otherwise
    """
    if not record.source_quote or not original_text:
        return False
    
    # Normalize both for comparison
    quote_normalized = record.source_quote.lower().strip()
    text_normalized = original_text.lower()
    
    # Check for substring match
    return quote_normalized in text_normalized


def validate_and_create(
    model_class: type[CitedRecord],
    data: dict,
    original_text: str,
) -> Optional[CitedRecord]:
    """
    Validate data and create record only if citation is valid.
    
    Args:
        model_class: Pydantic model class to use
        data: Extracted data dict
        original_text: Original text for citation validation
    
    Returns:
        Validated record or None if validation fails
    """
    try:
        record = model_class(**data)
        
        if validate_citation(record, original_text):
            return record
        else:
            # Citation not found - record is REJECTED
            return None
            
    except Exception:
        # Validation failed - record is REJECTED
        return None


# ============================================
# EXTRACTION RESULT CONTAINER
# ============================================
class ExtractionResult(BaseModel):
    """Container for extraction results with metadata."""
    
    record_type: str
    data: Optional[dict] = None
    is_valid: bool = False
    rejection_reason: Optional[str] = None
    requires_review: bool = False
    
    @classmethod
    def success(
        cls,
        record_type: str,
        record: CitedRecord,
    ) -> "ExtractionResult":
        return cls(
            record_type=record_type,
            data=record.model_dump(),
            is_valid=True,
            requires_review=record.requires_review,
        )
    
    @classmethod
    def failure(
        cls,
        record_type: str,
        reason: str,
    ) -> "ExtractionResult":
        return cls(
            record_type=record_type,
            is_valid=False,
            rejection_reason=reason,
        )
