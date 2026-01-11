"""
MSDS KNOWLEDGE BASE
===================
Mapping common waste descriptions to likely chemical compositions using
standard industrial safety data (MSDS/SDS).
"""

MSDS_MAP = {
    "sludge": {
        "composition": [
            {"component": "Water", "percent_range": "40-80%"},
            {"component": "Organic Solids", "percent_range": "10-40%"},
            {"component": "Inorganic Solids (Sand/Grit)", "percent_range": "5-20%"},
            {"component": "Heavy Metals (Zn, Cu, Pb)", "percent_range": "<1%"}
        ],
        "hazards": ["Biological", "Heavy Metal Toxicity"],
        "potential_uses": ["Biogas Generation", "Fertilizer", "Construction Filler"]
    },
    "non-hazardous waste": {
        "composition": [
            {"component": "Mixed Paper/Cardboard", "percent_range": "20-40%"},
            {"component": "Plastics (Mixed)", "percent_range": "10-30%"},
            {"component": "Organics/Food", "percent_range": "20-30%"},
            {"component": "Inert Glass/Metal", "percent_range": "10-20%"}
        ],
        "hazards": ["Low"],
        "potential_uses": ["RDF Fuel", "Recycling Sorting", "Compost"]
    },
    "hazardous waste": {
        "composition": [
            {"component": "Solvents", "percent_range": "10-50%"},
            {"component": "Heavy Metals (Hg, Pb, Cd)", "percent_range": "1-10%"},
            {"component": "Oily Sludge", "percent_range": "20-40%"},
            {"component": "Acids/Bases", "percent_range": "5-15%"}
        ],
        "hazards": ["Toxic", "Flammable", "Corrosive"],
        "potential_uses": ["Solvent Recovery", "Metal Extraction", "Incineration"]
    },
    "fly ash": {
        "composition": [
            {"component": "Silicon Dioxide (SiO2)", "percent_range": "40-60%"},
            {"component": "Aluminum Oxide (Al2O3)", "percent_range": "20-30%"},
            {"component": "Iron Oxide (Fe2O3)", "percent_range": "5-10%"},
            {"component": "Calcium Oxide (CaO)", "percent_range": "1-5%"}
        ],
        "hazards": ["Inhalation (Silica)", "Heavy Metal Leaching"],
        "potential_uses": ["Concrete Additive", "Geopolymer Cement", "Road Base"]
    },
    "spent catalyst": {
        "composition": [
            {"component": "Aluminum Oxide Support", "percent_range": "80-90%"},
            {"component": "Nickel/Cobalt/Molybdenum", "percent_range": "5-15%"},
            {"component": "Sulfur/Carbon Deposits", "percent_range": "1-5%"}
        ],
        "hazards": ["Flammability", "Carcinogenic Metals"],
        "potential_uses": ["Metal Recovery", "Abrasives"]
    },
    "red mud": {
        "composition": [
            {"component": "Iron Oxide (Fe2O3)", "percent_range": "30-60%"},
            {"component": "Aluminum Oxide (Al2O3)", "percent_range": "10-20%"},
            {"component": "Silicon Dioxide (SiO2)", "percent_range": "3-10%"},
            {"component": "Titanium Dioxide (TiO2)", "percent_range": "2-10%"},
            {"component": "Sodium Oxide (Na2O)", "percent_range": "2-10%"}
        ],
        "hazards": ["High Alkalinity (pH > 10)"],
        "potential_uses": ["Iron Recovery", "Building Materials", "Pigments"]
    },
    "drilling mud": {
        "composition": [
            {"component": "Barite", "percent_range": "30-50%"},
            {"component": "Bentonite Clay", "percent_range": "10-30%"},
            {"component": "Water/Oil Emulsion", "percent_range": "20-40%"},
            {"component": "Polymers", "percent_range": "1-5%"}
        ],
        "hazards": ["Hydrocarbon Contamination"],
        "potential_uses": ["Road Stabilization", "Landfill Cover"]
    },
    "tailings": {
        "composition": [
            {"component": "Silicates", "percent_range": "60-80%"},
            {"component": "Sulfides (Pyrite)", "percent_range": "5-20%"},
            {"component": "Trace Metals", "percent_range": "0.1-2%"}
        ],
        "hazards": ["Acid Mine Drainage", "Heavy Metal Leaching"],
        "potential_uses": ["Backfill", "Brick Manufacturing", "Glass Making"]
    }
}
