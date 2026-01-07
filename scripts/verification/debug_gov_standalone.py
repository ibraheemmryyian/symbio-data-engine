import logging
from pathlib import Path
from processors.gov_processor import GovProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_gov():
    print("🧪 Testing GovProcessor Standalone...")
    
    file_path = Path("data/raw/eprtr/eea_t_ied-eprtr_p_2007-2023_v15_r00/User-friendly-CSV/F4_2_WasteTransfers_Facilities.csv")
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    proc = GovProcessor()
    
    # Force eprtr
    print(f"📂 Processing {file_path}")
    results = proc.process_csv(file_path, source_type="eprtr")
    
    print(f"📊 Results: {len(results)}")
    
    if len(results) > 0:
        print("✅ SUCCESS! Sample:")
        print(results[0].data)
    else:
        print("⚠️ NO RESULTS.")
        # Debug why
        # (The processor should have logged errors if any)

if __name__ == "__main__":
    test_gov()
