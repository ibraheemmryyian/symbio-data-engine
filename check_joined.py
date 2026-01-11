import zipfile
import pandas as pd

print('CHECKING PRE-JOINED')
with zipfile.ZipFile(r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip') as z:
    names = z.namelist()
    joined = next((n for n in names if 'facility' in n.lower() and 'waste' in n.lower() and 'national' not in n.lower() and n.endswith('.csv')), None)
    
    if joined:
        print(f'FOUND JOINED FILE: {joined}')
        with z.open(joined) as f:
            head = pd.read_csv(f, nrows=1).columns.tolist()
            print(head)
    else:
        print('No pre-joined file found.')
