# SymbioFlows Exports

This directory contains exported data for the **SymbioFlows marketplace**.

## Data Format

Exports are generated in JSONL format for LLM training:

```jsonl
{"material": "copper scrap", "quantity_tons": 150.5, "price_per_ton": 8500, "source_company": "..."}
{"material": "HDPE plastic", "quantity_tons": 50.0, "price_per_ton": 450, "source_company": "..."}
```

## Fields

| Field | Type | Description |
|-------|------|-------------|
| material | string | Material name |
| material_category | string | Category (metals, plastics, etc.) |
| quantity_tons | float | Quantity in metric tons |
| price_per_ton | float | Price per metric ton |
| currency | string | Currency code (USD, EUR, etc.) |
| source_company | string | Company selling the material |
| source_location | string | Location |
| quality_grade | string | Quality grade (A, B, C) |
| year | int | Year of listing |

## Usage

```python
import json

with open("symbioflows_export.jsonl", "r") as f:
    for line in f:
        record = json.loads(line)
        print(f"{record['material']}: {record['quantity_tons']} tons")
```
