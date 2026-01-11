import zipfile
import pandas as pd

print('DUMPING HEADERS')
try:
    with zipfile.ZipFile(r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip') as z:
        names = z.namelist()
        
        # Fuzzy find files
        fac_path = next((n for n in names if 'facilities' in n.lower() and n.lower().endswith('.csv')), None)
        waste_path = next((n for n in names if 'wastetransfers' in n.lower() and n.lower().endswith('.csv')), None)
        
        with open('raw_headers.txt', 'w') as out:
            if fac_path:
                with z.open(fac_path) as f:
                    head = pd.read_csv(f, nrows=1).columns.tolist()
                    out.write(f'--- FACILITY ({fac_path}) ---\n')
                    for c in head:
                        out.write(f'{c}\n')
                    out.write('\n')
            
            if waste_path:
                with z.open(waste_path) as f:
                    head = pd.read_csv(f, nrows=1).columns.tolist()
                    out.write(f'--- WASTE ({waste_path}) ---\n')
                    for c in head:
                        out.write(f'{c}\n')
                        
except Exception as e:
    print(f'Error: {e}')
