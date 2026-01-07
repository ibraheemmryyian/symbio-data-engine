import zipfile
import os
from pathlib import Path
from config import RAW_DIR

zip_path = RAW_DIR / "eea_t_ied-eprtr_p_2007-2023_v15_r00.zip"
extract_to = RAW_DIR / "eprtr"

print(f"📦 Unzipping {zip_path}...")
print(f"   Target: {extract_to}")

if not zip_path.exists():
    print("❌ ZIP file not found!")
    exit(1)

try:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print("✅ Unzip Complete.")
    
    # List extracted
    print("\n📂 Extracted Files:")
    for f in extract_to.glob("**/*"):
        if f.is_file():
            size_mb = f.stat().st_size / (1024*1024)
            print(f"   - {f.name} ({size_mb:.2f} MB)")
            
except Exception as e:
    print(f"Error: {e}")
