"""
VERIFY NEGATIVE PRICING
=======================
Goal: Confirm that liabilities (Hazardous Waste, Produced Water) have negative prices.
"""
import csv

def verify():
    print("Verifying Pricing Calibration...")
    
    targets = ["hazardous waste", "produced water"]
    found = {t: False for t in targets}
    
    with open("exports/symbio_data_engine_READY.csv", 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            desc = row.get('waste_description', '').lower()
            price = float(row.get('price_per_ton_usd', 0))
            
            for t in targets:
                if t in desc and not found[t]:
                    print(f"MATCH: '{desc}' => ${price}/ton")
                    if price < 0:
                        print("   (CORRECT: Negative Value)")
                    else:
                        print("   (FAILURE: Positive Value)")
                    found[t] = True
            
            if all(found.values()):
                break

if __name__ == "__main__":
    verify()
