import zipfile
import io

print('EXTRACTING HEADERS')
try:
    with zipfile.ZipFile(r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip') as z:
        names = z.namelist()
        
        # Find files
        fac_file = next((f for f in names if '2_Facilities.csv' in f), None)
        waste_file = next((f for f in names if '2_WasteTransfers.csv' in f), None)
        
        print(f'\nFacility File: {fac_file}')
        if fac_file:
            with z.open(fac_file) as f:
                head = f.read(500).decode('utf-8', errors='ignore')
                print(f'Facility Columns: {head.splitlines()[0]}')
                
        print(f'\nWaste File: {waste_file}')
        if waste_file:
            with z.open(waste_file) as f:
                head = f.read(500).decode('utf-8', errors='ignore')
                print(f'Waste Columns: {head.splitlines()[0]}')

except Exception as e:
    print(f'Error: {e}')
