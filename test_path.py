from pathlib import Path
import os

target = Path(r"C:\Users\Imrry\Desktop\symbio_data_engine\data\raw\eprtr\eea_t_ied-eprtr_p_2007-2023_v15_r00\User-friendly-CSV")

print(f"Path: {target}")
print(f"Exists: {target.exists()}")
print(f"Is Dir: {target.is_dir()}")

print("Listing:")
try:
    for f in target.iterdir():
        print(f" - {f.name}")
except Exception as e:
    print(f"Error: {e}")
