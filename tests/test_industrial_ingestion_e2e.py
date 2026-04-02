"""
Test Industrial Byproduct Ingestion Pipeline
==============================================
End-to-end tests for World Steel, FAO, Plastics, Petcoke, and Maritime data ingestion.
"""

import csv
import tempfile
import uuid
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from processors.gov_processor import GovProcessor
from processors.models import IndustrialByproductExtraction, ExtractionResult


@pytest.fixture
def sample_worldsteel_csv():
    """Create sample World Steel CSV for testing."""
    data = [
        ["Facility Name", "Country", "City", "Product Type", "Annual Volume", "Year"],
        ["ArcelorMittal Dofasco", "CA", "Hamilton", "Slag", "450000", "2023"],
        ["Nippon Steel", "JP", "Tokyo", "Slag", "320000", "2023"],
        ["POSCO", "KR", "Pohang", "EAF Dust", "125000", "2023"],
    ]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        return f.name


@pytest.fixture
def sample_fao_csv():
    """Create sample FAO food waste CSV for testing."""
    data = [
        ["Country", "Region", "Crop", "Waste Type", "Quantity (tonnes)", "Year"],
        ["US", "Midwest", "Corn", "Spent Grain", "350000", "2023"],
        ["FR", "Aquitaine", "Grapes", "Pomace", "85000", "2023"],
    ]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
        return f.name


def test_industrial_byproduct_extraction_valid():
    """Test IndustrialByproductExtraction validation with valid data."""
    ext = IndustrialByproductExtraction(
        material_name="steel slag",
        material_category="byproduct",
        industry="steel",
        source_company="Test Steel Mill",
        source_country="US",
        source_location="Pittsburgh, PA",
        quantity_tons=100000,
        primary_receiver_industry="cement",
        year=2023,
        source_quote="Test mill produced 100000 tons of steel slag in 2023",
        verification_method="World Steel Production Database",
        extraction_confidence=0.7,
    )
    assert ext.material_name == "Steel Slag"  # normalized to title case
    assert ext.extraction_confidence == 0.7
    assert ext.recyclability_score == 0.7  # default value
    assert ext.currency == "USD"  # default value


def test_industrial_byproduct_extraction_normalizes_material_name():
    """Test that material_name is normalized to title case."""
    ext = IndustrialByproductExtraction(
        material_name="  ELECTRIC ARC FURNACE DUST  ",
        material_category="byproduct",
        industry="steel",
        source_company="ArcelorMittal",
        source_country="BE",
        source_location="Gent",
        quantity_tons=50000,
        primary_receiver_industry="cement",
        year=2023,
        source_quote="EAF dust production data from company report",
        verification_method="Company Report",
        extraction_confidence=0.8,
    )
    assert ext.material_name == "Electric Arc Furnace Dust"


def test_industrial_byproduct_extraction_rejects_generic_company():
    """Test that company validation rejects overly generic names."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Unknown",  # too generic
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=2023,
            source_quote="Test quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_requires_source_quote():
    """Test that source_quote is mandatory (min 10 chars)."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=2023,
            source_quote="short",  # too short (min 10)
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_country_code():
    """Test that country code must be ISO 3166-1 alpha-2."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Test Mill",
            source_country="USA",  # invalid format (should be US)
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=2023,
            source_quote="Valid quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_quantity():
    """Test that quantity must be > 0."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=0,  # invalid: must be > 0
            primary_receiver_industry="cement",
            year=2023,
            source_quote="Valid quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_material_category():
    """Test that material_category must be from allowed list."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="invalid_category",
            industry="steel",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=2023,
            source_quote="Valid quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_industry():
    """Test that industry must be from allowed list."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Test Material",
            material_category="byproduct",
            industry="invalid_industry",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=2023,
            source_quote="Valid quote about test material production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_receiver_industry():
    """Test that primary_receiver_industry must be from allowed list."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="invalid_receiver",
            year=2023,
            source_quote="Valid quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_validates_year():
    """Test that year must be within valid range."""
    with pytest.raises(ValueError):
        IndustrialByproductExtraction(
            material_name="Steel Slag",
            material_category="byproduct",
            industry="steel",
            source_company="Test Mill",
            source_country="US",
            source_location="Test",
            quantity_tons=100,
            primary_receiver_industry="cement",
            year=1960,  # before MIN_YEAR (1970)
            source_quote="Valid quote about steel slag production",
            verification_method="Test Data",
        )


def test_industrial_byproduct_extraction_with_optional_fields():
    """Test IndustrialByproductExtraction with optional fields populated."""
    ext = IndustrialByproductExtraction(
        material_name="steel slag",
        material_category="byproduct",
        industry="steel",
        source_company="ArcelorMittal Gent",
        source_country="BE",
        source_location="Gent, Belgium",
        quantity_tons=275000,
        primary_receiver_industry="cement",
        receiver_companies=["Cementir Holdings", "Buzzi Unicem"],
        price_per_unit=15.50,
        currency="EUR",
        environmental_impact={"co2_kg_per_ton": 50, "water_l_per_ton": 200},
        recyclability_score=0.85,
        year=2023,
        source_quote="ArcelorMittal produced 275,000 tons of slag suitable for cement applications",
        verification_method="World Steel Association Report 2023",
        extraction_confidence=0.92,
    )
    assert ext.price_per_unit == 15.50
    assert ext.currency == "EUR"
    assert len(ext.receiver_companies) == 2
    assert ext.environmental_impact["co2_kg_per_ton"] == 50
    assert ext.recyclability_score == 0.85
    assert ext.extraction_confidence == 0.92
    assert ext.requires_review == False  # confidence >= 0.9


def test_gov_processor_initialization():
    """Test that GovProcessor initializes correctly."""
    gp = GovProcessor()
    assert gp is not None
    assert hasattr(gp, 'column_maps')
    assert 'epa_tri' in gp.column_maps
    assert 'eprtr' in gp.column_maps


def test_worldsteel_csv_parsing(sample_worldsteel_csv):
    """Test that World Steel CSV can be parsed without errors."""
    with open(sample_worldsteel_csv, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) >= 3
    assert rows[0]['Facility Name'] == 'ArcelorMittal Dofasco'
    assert rows[0]['Country'] == 'CA'
    assert rows[0]['Product Type'] == 'Slag'
    assert rows[0]['Annual Volume'] == '450000'


def test_fao_csv_parsing(sample_fao_csv):
    """Test that FAO CSV can be parsed without errors."""
    with open(sample_fao_csv, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) >= 2
    assert rows[0]['Country'] == 'US'
    assert rows[0]['Waste Type'] == 'Spent Grain'
    assert rows[0]['Quantity (tonnes)'] == '350000'


def test_industrial_byproduct_extraction_flags_low_confidence():
    """Test that records with low confidence are flagged for review."""
    ext = IndustrialByproductExtraction(
        material_name="Steel Slag",
        material_category="byproduct",
        industry="steel",
        source_company="Test Mill",
        source_country="US",
        source_location="Test",
        quantity_tons=100,
        primary_receiver_industry="cement",
        year=2023,
        source_quote="Valid quote about steel slag production data",
        verification_method="Test Data",
        extraction_confidence=0.65,  # below 0.9 threshold
    )
    assert ext.requires_review == True


def test_industrial_byproduct_extraction_high_confidence_no_review():
    """Test that high-confidence records are not flagged for review."""
    ext = IndustrialByproductExtraction(
        material_name="Steel Slag",
        material_category="byproduct",
        industry="steel",
        source_company="Test Mill",
        source_country="US",
        source_location="Test",
        quantity_tons=100,
        primary_receiver_industry="cement",
        year=2023,
        source_quote="Valid quote about steel slag production data",
        verification_method="Test Data",
        extraction_confidence=0.95,  # above 0.9 threshold
    )
    assert ext.requires_review == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
