import csv

INPUT_FILE = "exports/symbio_data_engine_READY.csv"

def audit():
    print("AUDITING MASTER FILE COMPOSITION...")
    
    total = 0
    with_geo = 0
    alpha_verified = 0
    priced = 0
    negative_prices = 0
    
    with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            
            # Geo check
            try:
                lat = float(row.get('lat', 0))
                lon = float(row.get('lon', 0))
                if lat != 0 or lon != 0: with_geo += 1
            except: pass
            
            # Alpha check
            if row.get('is_alpha_verified') == 'True':
                alpha_verified += 1
                
            # Price check
            try:
                p = float(row.get('price_per_ton_usd', 0))
                if p != 0: priced += 1
                if p < 0: negative_prices += 1
            except: pass

    print(f"\nTOTAL ROWS: {total}")
    print(f"GEOSPATIAL (Lat/Lon): {with_geo} ({with_geo/total*100:.1f}%)")
    print(f"ALPHA VERIFIED (Facts): {alpha_verified} ({alpha_verified/total*100:.1f}%)")
    print(f"PRICED (Total): {priced}")
    print(f"LIABILITIES (Negative $): {negative_prices}")

if __name__ == "__main__":
    audit()
