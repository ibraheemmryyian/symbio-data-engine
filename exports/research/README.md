# Research Exports

This directory contains exported data for **academic research** on industrial symbiosis.

## Data Format

Historical symbiosis exchange data from eco-industrial parks:

```jsonl
{"eco_park": "Kalundborg", "year": 2020, "source": "Asnæs Power", "target": "Gyproc", "material": "fly ash"}
{"eco_park": "Kalundborg", "year": 2020, "source": "Novo Nordisk", "target": "Novozymes", "material": "yeast slurry"}
```

## Fields

| Field | Type | Description |
|-------|------|-------------|
| eco_park | string | Name of eco-industrial park |
| year | int | Year of exchange |
| source_company | string | Company providing waste/byproduct |
| target_company | string | Company receiving material |
| material | string | Material being exchanged |
| volume_tons | float | Volume in metric tons |
| exchange_type | string | waste, byproduct, energy, water |
| co2_savings | float | Estimated CO2 savings |

## Covered Parks

1. **Kalundborg Symbiosis** (Denmark) - 1972-present
2. **Ulsan EIP** (South Korea) - 2005-present
3. **NISP** (UK) - 2003-present
4. **Tianjin TEDA** (China) - 1996-present

## Academic Use

This data is suitable for:
- Network analysis of material flows
- Economic modeling of industrial symbiosis
- Environmental impact assessment
- Policy research

## Citation

When using this data in academic work, please cite:
```
Symbio Data Engine (2026). Industrial Symbiosis Database.
https://github.com/symbio/data-engine
```
