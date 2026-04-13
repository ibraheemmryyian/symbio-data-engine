"""
Symbio Data Engine - Data Quality Validators
=============================================
Enforces Data Spec Contract (DQ-001 through DQ-005)
All data MUST pass through these validators before ingestion.
"""

from typing import Dict, List, Optional, Tuple, Any
import logging
import re

logger = logging.getLogger(__name__)

# ============================================================
# VALID VALUES (From Data Spec Contract)
# ============================================================

VALID_VERIFICATION_METHODS = [
    'E-PRTR Official (v15)',
    'CSR Verified Extraction (Regex v3.1)',
    'Government Portal',
    'Manual Verification',
    'EPA TRI Official',
]

VALID_INDUSTRY_KEYS = [
    'brewingAndDistilling', 'dairyProduction', 'constructionAndDemolition',
    'chemicalsAndPharmaceuticals', 'foodAndBeverage', 'textilesAndApparel',
    'metalAndManufacturing', 'generalManufacturing', 'energyAndPowerGeneration',
    'Food Processing', 'Chemical', 'Healthcare', 'retail'
]

# Materials that should NEVER appear in certain industries (DQ-004)
FORBIDDEN_MATERIALS_BY_INDUSTRY = {
    'constructionAndDemolition': [
        'aldrin', 'dioxin', 'heptachlor', 'chlordane', 'atrazine', 'carbaryl',
        'pesticide', 'herbicide', 'fungicide', 'phenol', 'ammonia', 'cumene',
        'trade secret', 'captan', 'diazinon', 'chloroform', 'dichlorvos',
        'chlorophenols', 'cyanide', 'formaldehyde', 'iron pentacarbonyl'
    ],
    'Construction': ['aldrin', 'dioxin', 'heptachlor', 'chlordane'],
}

# Precious metals allowed to exceed $5k/ton
PRECIOUS_MATERIALS = ['gold', 'platinum', 'palladium', 'rhodium', 'silver']


class ValidationResult:
    """Container for validation results."""
    
    def __init__(self, is_valid: bool, rule: str, message: str = "", severity: str = "INFO"):
        self.is_valid = is_valid
        self.rule = rule
        self.message = message
        self.severity = severity  # CRITICAL, HIGH, WARNING, INFO
    
    def __repr__(self):
        status = "✅ PASS" if self.is_valid else "❌ FAIL"
        return f"{status} [{self.rule}] {self.message}"


def validate_dq001(row: Dict[str, Any]) -> ValidationResult:
    """
    DQ-001: verification_method MUST NOT be NULL (CRITICAL)
    """
    verification_method = row.get('verification_method')
    
    if not verification_method or str(verification_method).strip() == '':
        return ValidationResult(
            is_valid=False,
            rule='DQ-001',
            message='verification_method is NULL or empty',
            severity='CRITICAL'
        )
    
    if verification_method not in VALID_VERIFICATION_METHODS:
        return ValidationResult(
            is_valid=False,
            rule='DQ-001',
            message=f'Invalid verification_method: {verification_method}',
            severity='WARNING'
        )
    
    return ValidationResult(is_valid=True, rule='DQ-001', message='OK')


def validate_dq002(row: Dict[str, Any]) -> ValidationResult:
    """
    DQ-002: material_defaults keys must be unique snake_case strings.
    For waste_listings, we check that material exists and is reasonable.
    """
    material = row.get('material')
    
    if not material or len(str(material).strip()) < 2:
        return ValidationResult(
            is_valid=False,
            rule='DQ-002',
            message='material is NULL or too short',
            severity='CRITICAL'
        )
    
    return ValidationResult(is_valid=True, rule='DQ-002', message='OK')


def validate_dq003(row: Dict[str, Any]) -> ValidationResult:
    """
    DQ-003: Bulk 50.0 values indicate corrupt/placeholder data (WARNING)
    Note: Individual 50.0 values are OK - this is for batch detection.
    """
    quantity_fields = ['quantity_onsite', 'quantity_tons', 'quantity', 'price_per_ton_usd']
    
    for field in quantity_fields:
        val = row.get(field)
        if val is not None:
            try:
                if float(val) == 50.0:
                    return ValidationResult(
                        is_valid=False,
                        rule='DQ-003',
                        message=f'{field} = 50.0 (potential placeholder)',
                        severity='WARNING'
                    )
            except (ValueError, TypeError):
                pass
    
    return ValidationResult(is_valid=True, rule='DQ-003', message='OK')


def validate_dq004(row: Dict[str, Any]) -> ValidationResult:
    """
    DQ-004: Materials should contextually match their industry.
    (e.g., "Aldrin" pesticide should NOT appear in "Construction")
    """
    material = str(row.get('material', '')).lower()
    industry = row.get('industry') or row.get('source_industry') or ''
    
    if not material or not industry:
        return ValidationResult(is_valid=True, rule='DQ-004', message='OK (no industry to check)')
    
    # Check all industry variants
    for ind_key, forbidden in FORBIDDEN_MATERIALS_BY_INDUSTRY.items():
        if ind_key.lower() in industry.lower():
            for forbidden_mat in forbidden:
                if forbidden_mat.lower() in material:
                    return ValidationResult(
                        is_valid=False,
                        rule='DQ-004',
                        message=f'Material "{material}" forbidden in {industry}',
                        severity='WARNING'
                    )
    
    return ValidationResult(is_valid=True, rule='DQ-004', message='OK')


def validate_billionaire_sludge(row: Dict[str, Any]) -> ValidationResult:
    """
    AMENDMENT 2: No waste stream (except precious metals) should be > $5,000/ton
    """
    price = row.get('price_per_ton_usd')
    estimated_value = row.get('estimated_value_usd')
    material = str(row.get('material', '')).lower()
    
    # Check if precious metal
    is_precious = any(p in material for p in PRECIOUS_MATERIALS)
    
    if price is not None:
        try:
            if float(price) > 5000 and not is_precious:
                return ValidationResult(
                    is_valid=False,
                    rule='BILLIONAIRE_SLUDGE',
                    message=f'price_per_ton_usd {price} exceeds $5,000',
                    severity='HIGH'
                )
        except (ValueError, TypeError):
            pass
    
    if estimated_value is not None:
        try:
            if float(estimated_value) > 100_000_000:  # $100M cap
                return ValidationResult(
                    is_valid=False,
                    rule='BILLIONAIRE_SLUDGE',
                    message=f'estimated_value_usd {estimated_value} exceeds $100M',
                    severity='HIGH'
                )
        except (ValueError, TypeError):
            pass
    
    return ValidationResult(is_valid=True, rule='BILLIONAIRE_SLUDGE', message='OK')


def validate_imperial_ghost(row: Dict[str, Any]) -> ValidationResult:
    """
    AMENDMENT 1: Detect lbs→kg double-conversion artifacts.
    Values close to multiples of 2.20462 are suspicious.
    """
    qty_fields = ['quantity_onsite', 'quantity_tons', 'quantity']
    
    for field in qty_fields:
        val = row.get(field)
        if val is not None:
            try:
                val = float(val)
                if val > 0:
                    ratio = val / 2.20462
                    # Check if ratio is close to an integer
                    if abs(ratio - round(ratio)) < 0.01 and round(ratio) > 10:
                        return ValidationResult(
                            is_valid=False,
                            rule='IMPERIAL_GHOST',
                            message=f'{field}={val} looks like unconverted lbs',
                            severity='WARNING'
                        )
            except (ValueError, TypeError):
                pass
    
    return ValidationResult(is_valid=True, rule='IMPERIAL_GHOST', message='OK')


def validate_row(row: Dict[str, Any], strict: bool = True) -> Tuple[bool, List[ValidationResult]]:
    """
    Run all validators on a single row.
    
    Args:
        row: Dictionary containing row data
        strict: If True, any CRITICAL violation fails the row
    
    Returns:
        Tuple of (is_valid, list of ValidationResults)
    """
    validators = [
        validate_dq001,
        validate_dq002,
        validate_dq003,
        validate_dq004,
        validate_billionaire_sludge,
        validate_imperial_ghost,
    ]
    
    results = [v(row) for v in validators]
    
    # In strict mode, any CRITICAL failure = row invalid
    if strict:
        critical_failures = [r for r in results if not r.is_valid and r.severity == 'CRITICAL']
        return len(critical_failures) == 0, results
    
    # In non-strict mode, only DQ-001 (verification_method) is enforced
    dq001_result = results[0]
    return dq001_result.is_valid, results


def validate_batch(rows: List[Dict[str, Any]], strict: bool = True) -> Dict[str, Any]:
    """
    Validate a batch of rows and return summary.
    
    Args:
        rows: List of row dictionaries
        strict: If True, apply strict validation
    
    Returns:
        Dictionary with validation summary and failed rows
    """
    valid_rows = []
    invalid_rows = []
    violation_counts = {}
    
    for i, row in enumerate(rows):
        is_valid, results = validate_row(row, strict=strict)
        
        if is_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append({
                'row_index': i,
                'row': row,
                'violations': [r for r in results if not r.is_valid]
            })
            
            for r in results:
                if not r.is_valid:
                    violation_counts[r.rule] = violation_counts.get(r.rule, 0) + 1
    
    return {
        'total_rows': len(rows),
        'valid_count': len(valid_rows),
        'invalid_count': len(invalid_rows),
        'valid_rows': valid_rows,
        'invalid_rows': invalid_rows[:100],  # Limit to first 100 for debugging
        'violation_counts': violation_counts
    }


def add_verification_method(row: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Add verification_method based on source type.
    Use this at the point of extraction.
    
    Args:
        row: Row dictionary
        source: Source type ('eprtr', 'csr', 'gov', 'manual')
    
    Returns:
        Row with verification_method added
    """
    source_mapping = {
        'eprtr': 'E-PRTR Official (v15)',
        'epa_tri': 'EPA TRI Official',
        'csr': 'CSR Verified Extraction (Regex v3.1)',
        'gov': 'Government Portal',
        'manual': 'Manual Verification',
        'government': 'Government Portal',
    }
    
    row['verification_method'] = source_mapping.get(source.lower(), source)
    return row


# ============================================================
# BATCH DQ-003 CHECK (for detecting bulk 50.0 corruption)
# ============================================================

def detect_bulk_50_corruption(rows: List[Dict[str, Any]], threshold: int = 100) -> Dict[str, Any]:
    """
    Detect if more than threshold rows have exactly 50.0 values.
    This is a batch-level check for DQ-003.
    """
    qty_fields = ['quantity_onsite', 'quantity_tons', 'quantity', 'price_per_ton_usd']
    
    counts = {}
    for field in qty_fields:
        counts[field] = sum(1 for row in rows if row.get(field) == 50.0)
    
    corrupted = {k: v for k, v in counts.items() if v > threshold}
    
    return {
        'is_corrupted': len(corrupted) > 0,
        'corrupted_fields': corrupted,
        'threshold': threshold,
        'total_rows': len(rows)
    }


# ============================================================
# REFERENTIAL INTEGRITY: Material must have pricing
# ============================================================

_pricing_cache = None  # Cache for pricing lookup

def load_pricing_definitions() -> dict:
    """
    Load all material pricing definitions from market_pricing table.
    Returns a dict of material_name -> price_info
    """
    global _pricing_cache
    
    if _pricing_cache is not None:
        return _pricing_cache
    
    try:
        import os
        from supabase import create_client
        from dotenv import load_dotenv
        load_dotenv()
        
        s = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
        mp = s.table('market_pricing').select('*').execute()
        
        pricing = {}
        for row in mp.data:
            md = row.get('material_defaults') or {}
            if isinstance(md, dict):
                for mat_name, price_data in md.items():
                    mat_lower = mat_name.lower().strip().replace('_', ' ')
                    pricing[mat_lower] = price_data
        
        _pricing_cache = pricing
        return pricing
    except Exception as e:
        logger.error(f"Failed to load pricing definitions: {e}")
        return {}


def validate_material_has_pricing(row: Dict[str, Any]) -> ValidationResult:
    """
    REFERENTIAL INTEGRITY: Material in waste_listings must have 
    a corresponding entry in market_pricing.material_defaults.
    
    If no pricing exists, the material cannot be valued in reports.
    """
    material = str(row.get('material', '')).lower().strip()
    
    if not material:
        return ValidationResult(
            is_valid=False,
            rule='MATERIAL_PRICING_REF',
            message='Material is empty',
            severity='CRITICAL'
        )
    
    pricing = load_pricing_definitions()
    
    if not pricing:
        return ValidationResult(
            is_valid=True,
            rule='MATERIAL_PRICING_REF',
            message='Could not load pricing (skipping check)',
            severity='INFO'
        )
    
    # Check for exact or fuzzy match
    matched = False
    
    # Exact match
    if material in pricing:
        matched = True
    else:
        # Fuzzy match - check if material contains any priced material
        for priced_mat in pricing.keys():
            if priced_mat in material or material in priced_mat:
                matched = True
                break
    
    if matched:
        return ValidationResult(
            is_valid=True,
            rule='MATERIAL_PRICING_REF',
            message='Pricing exists',
            severity='INFO'
        )
    else:
        return ValidationResult(
            is_valid=False,
            rule='MATERIAL_PRICING_REF',
            message=f'No pricing for material: {material}',
            severity='WARNING'
        )


def validate_row_with_pricing(row: Dict[str, Any], strict: bool = True) -> Tuple[bool, List[ValidationResult]]:
    """
    Run all validators INCLUDING material-pricing referential integrity.
    """
    validators = [
        validate_dq001,
        validate_dq002,
        validate_dq003,
        validate_dq004,
        validate_billionaire_sludge,
        validate_imperial_ghost,
        validate_material_has_pricing,  # NEW: Referential integrity
    ]
    
    results = [v(row) for v in validators]
    
    if strict:
        critical_failures = [r for r in results if not r.is_valid and r.severity == 'CRITICAL']
        return len(critical_failures) == 0, results
    
    dq001_result = results[0]
    return dq001_result.is_valid, results



    # Quick test
    test_row = {
        'material': 'Steel Slag',
        'verification_method': 'E-PRTR Official (v15)',
        'quantity_tons': 1500.5,
        'industry': 'metalAndManufacturing',
        'price_per_ton_usd': 45.0
    }
    
    is_valid, results = validate_row(test_row)
    print(f"Row valid: {is_valid}")
    for r in results:
        print(f"  {r}")
    
    # Test failure case
    bad_row = {
        'material': 'Aldrin',
        'industry': 'constructionAndDemolition',
        'quantity_tons': 50.0,
        'price_per_ton_usd': 14520000000
    }
    
    print("\nBad row test:")
    is_valid, results = validate_row(bad_row)
    print(f"Row valid: {is_valid}")
    for r in results:
        if not r.is_valid:
            print(f"  {r}")
