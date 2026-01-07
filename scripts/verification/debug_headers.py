import glob
import os

print("🔎 INSPECTING E-PRTR HEADERS...")
files = glob.glob(os.path.join("data", "raw", "eprtr", "*.csv"))

if not files:
    print("❌ No E-PRTR files found in data/raw/eprtr!")
else:
    target = files[0]
    print(f"📄 Reading: {target}")
    with open(target, 'r', encoding='utf-8', errors='ignore') as f:
        headers = f.readline().strip()
        print("\nHEADERS:")
        print(headers)
        
        # Check delimiters
        if ";" in headers and "," not in headers:
            print("\n⚠️ Detected SEMICOLON delimiter!")
        elif "\t" in headers:
            print("\n⚠️ Detected TAB delimiter!")
