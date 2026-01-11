import zipfile
import pandas as pd

print('AUDITING ALL HEADERS')
try:
    with zipfile.ZipFile(r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip') as z:
        names = z.namelist()
        csvs = [n for n in names if n.lower().endswith('.csv')]
        
        with open('zip_inventory.txt', 'w') as out:
            for csv_file in csvs:
                try:
                    with z.open(csv_file) as f:
                        cols = pd.read_csv(f, nrows=1).columns.tolist()
                        out.write(f'FILE: {csv_file}\n')
                        out.write(f'COLS: {cols}\n\n')
                except Exception as e:
                    out.write(f'FILE: {csv_file} ERROR: {e}\n\n')
                    
    print(f'Audited {len(csvs)} files. Saved to zip_inventory.txt')

except Exception as e:
    print(f'Critical Error: {e}')
