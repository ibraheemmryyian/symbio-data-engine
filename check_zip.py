import zipfile
z = zipfile.ZipFile(r'c:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eea_t_ied-eprtr_p_2007-2023_v15_r00.zip')
with open('zip_files.txt', 'w') as f:
    f.write('\n'.join(z.namelist()))
