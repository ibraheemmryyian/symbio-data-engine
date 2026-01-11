import ast

print('FINDING TABLES IN INVENTORY')
with open('zip_inventory.txt', 'r') as f:
    text = f.read()
    
blocks = text.split('\n\n')
fac_file = None
waste_file = None

for b in blocks:
    if not b.strip(): continue
    lines = b.split('\n')
    fname = lines[0].replace('FILE: ', '').strip()
    
    # Check cols
    try:
        col_line = next(l for l in lines if l.startswith('COLS: '))
        cols = ast.literal_eval(col_line.replace('COLS: ', ''))
        cols_lower = [c.lower() for c in cols]
        
        # Check Facility
        has_id = any('facilityid' in c or 'facilityreportid' in c for c in cols_lower)
        has_coord = any('lat' in c or 'long' in c or 'coord' in c for c in cols_lower)
        has_country = any('country' in c for c in cols_lower)
        
        if has_id and has_coord and has_country and 'facility' in fname.lower():
            print(f'MATCH FACILITY: {fname}')
            print(f'  Cols: {cols[:5]}...')
            fac_file = fname
            
        # Check Waste
        has_waste = any('wasteclass' in c or 'wastetreat' in c for c in cols_lower)
        has_qty = any('quantity' in c or 'amount' in c for c in cols_lower)
        
        if has_id and has_waste and has_qty:
            print(f'MATCH WASTE: {fname}')
            print(f'  Cols: {cols[:5]}...')
            waste_file = fname
            
    except Exception as e:
        pass
