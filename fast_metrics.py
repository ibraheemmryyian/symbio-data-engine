import pandas as pd
import os

print('FAST METRICS REPORT')
print('===================')

# 1. TOKEN COUNT (File Size Proxy is consistently accurate for CSV training data)
# 1 byte ~= 0.25 to 0.3 tokens usually.
f_path = 'exports/waste_listings_granular.csv'
if os.path.exists(f_path):
    size_bytes = os.path.getsize(f_path)
    est_tokens = int(size_bytes / 3.5) # Conservative estimate for CSV structure
    print(f'File Size: {size_bytes / (1024*1024):.2f} MB')
    print(f'Est. Training Tokens: {est_tokens:,}')
else:
    print('File not found')

# 2. RISK AUDIT (Fast Sample)
try:
    # Read just needed columns for speed
    df = pd.read_csv(f_path, usecols=['source_company', 'region', 'material', 'chemical_profile'])
    
    # Hallucination Check: Company Consistency
    drift_count = 0
    if 'source_company' in df.columns:
        counts = df.groupby('source_company')['region'].nunique()
        drift_count = len(counts[counts > 1])
        print(f'Company Region Drift: {drift_count} companies (Risk < {drift_count/len(counts):.1%})')

    # Safety Check
    sample = df.sample(min(5000, len(df)))
    bad_matches = 0
    for _, row in sample.iterrows():
        m = str(row['material']).lower()
        p = str(row['chemical_profile'])
        if 'non-hazardous' in m and 'Toxic' in p:
            bad_matches += 1
            
    print(f'Safety Mismatch Rate (Sample {len(sample)}): {bad_matches} ({bad_matches/len(sample):.2%})')

except Exception as e:
    print(f'Audit Error: {e}')
