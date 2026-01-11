import ast

keys = ['id', 'fac', 'lat', 'long', 'coord', 'country', 'waste', 'quant', 'amount', 'treat', 'class']

print('MAPPING COLUMNS')
with open('cols_debug.txt', 'r') as f:
    lines = f.readlines()
    
    current_file = None
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if line.startswith('FACILITY'):
            print(f'\n{line}')
            current_file = 'FAC'
        elif line.startswith('WASTE'):
            print(f'\n{line}')
            current_file = 'WASTE'
        elif line.startswith('[') and current_file:
            try:
                cols = ast.literal_eval(line)
                print(f'Total Columns: {len(cols)}')
                for c in cols:
                    # Check for keywords
                    if any(k in c.lower() for k in keys):
                        print(f'  MATCH: {c}')
                current_file = None
            except:
                pass
