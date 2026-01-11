import csv

INPUT_FILE = "exports/symbio_data_engine_READY.csv"

def check():
    print("Checking Hazardous Waste Pricing...")
    with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        count = 0
        failures = 0
        zeros = 0
        
        for row in reader:
            desc = row.get('waste_description', '').lower()
            if 'hazardous' in desc:
                try:
                    price = float(row['price_per_ton_usd'])
                    if price >= 0:
                        print(f"FAIL: {desc[:30]}... | Price: {price}")
                        failures += 1
                        if price == 0: zeros += 1
                    else:
                        if count < 3:
                            print(f"PASS: {desc[:30]}... | Price: {price}")
                except:
                    print(f"ERROR parsing price for: {desc}")
                
                count += 1
                if count > 20: break
    
    print(f"\nStats for first 20 'hazardous' items:")
    print(f"Failures (>=0): {failures}")
    print(f"Zeros: {zeros}")

if __name__ == "__main__":
    check()
