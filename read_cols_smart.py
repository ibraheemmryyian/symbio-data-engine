import ast

print('PARSING COLUMNS')
with open('cols_debug.txt', 'r') as f:
    lines = f.readlines()
    
    current_file = None
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if line.startswith('FACILITY'):
            print('\n--- FACILITY COLUMNS ---')
            current_file = 'FAC'
        elif line.startswith('WASTE'):
            print('\n--- WASTE COLUMNS ---')
            current_file = 'WASTE'
        elif line.startswith('[') and current_file:
            try:
                cols = ast.literal_eval(line)
                for c in cols:
                    print(c)
                current_file = None # Only print header row
            except:
                pass
