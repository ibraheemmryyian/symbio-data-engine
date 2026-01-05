# SymbioTrust Exports

This directory contains exported data for **SymbioTrust carbon verification**.

## Data Format

Exports include carbon emissions and fraud detection data:

```jsonl
{"company": "ACME Corp", "year": 2023, "co2_tons": 125000, "anomaly_score": 0.12}
{"company": "Beta Industries", "year": 2023, "co2_tons": 85000, "anomaly_score": 0.85}
```

## Fields

### Carbon Emissions

| Field | Type | Description |
|-------|------|-------------|
| company | string | Company name |
| year | int | Reporting year |
| co2_tons | float | Total CO2 emissions |
| co2_scope1 | float | Scope 1 (direct) emissions |
| co2_scope2 | float | Scope 2 (indirect) emissions |
| co2_scope3 | float | Scope 3 (value chain) emissions |
| co2_avoided | float | CO2 avoided through circular economy |
| methodology | string | Reporting methodology |
| anomaly_score | float | Fraud detection score (0-1) |

### Fraud Flags

| Field | Type | Description |
|-------|------|-------------|
| entity_type | string | Type of entity flagged |
| flag_type | string | Type of fraud indicator |
| severity | string | low, medium, high, critical |
| description | string | Detailed explanation |

## Usage

```python
import json

# Load emissions data
with open("carbon_emissions.jsonl", "r") as f:
    for line in f:
        record = json.loads(line)
        if record.get("anomaly_score", 0) > 0.7:
            print(f"⚠️ High anomaly: {record['company']}")
```
