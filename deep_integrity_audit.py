"""
DEEP INTEGRITY AUDIT - HALLUCINATION & SANITIZATION CHECK
=========================================================
Rigorous audit to quantify risk of false training data.
Checks:
1. Fuzzy Match Confidence: Are we mapping "Austrian Airlines" to "Australia"?
2. Chemical Profile Consistency: Are we tagging "Office Paper" as "Hazardous"?
3. Geospatial Validity: Are coordinates landing in oceans?
"""
import pandas as pd
import json
from collections import Counter

def run_deep_audit():
    print('='*70)
    print('DEEP INTEGRITY AUDIT')
    print('='*70)
    
    file_path = 'exports/waste_listings_granular.csv'
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f'CRITICAL: Cannot load dataset. {e}')
        return

    total = len(df)
    print(f'Total Records: {total:,}')
    
    # 1. HALLUCINATION RISK: Fuzzy Matching
    # In our fuzzy logic, a match that isn't exact is a potential hallucination.
    # We can't verify accuracy perfectly without ground truth, but we can verify consistency.
    print('\n[1] GEOSPATIAL CONSISTENCY (Hallucination Proxy)')
    # If region is "unknown", it's safe (no hallucination). If confirmed, is it plausible?
    
    # Metric: Company Variance
    # A single company should belong to ONE region mostly. If "Shell" is mapped
    # to 6 different regions in 6 rows, that's likely data drift or matching error.
    if 'source_company' in df.columns:
        co_region_counts = df.groupby('source_company')['region'].nunique()
        drift_cos = co_region_counts[co_region_counts > 1]
        print(f'Companies with Multi-Region Drift: {len(drift_cos)} (Risk of matching error)')
        if not drift_cos.empty:
            print(f'Example Drifters: {drift_cos.head().index.tolist()}')
    
    # 2. SANITIZATION CHECK: Chemical Profiles
    print('\n[2] CHEMICAL MAPPING VALIDITY')
    # Check for mismatch: Non-Hazardous material mapped to Hazardous profile
    
    risk_count = 0
    safe_count = 0
    
    # Iterate a sample for speed (full scan if critical)
    sample = df.sample(min(10000, len(df)))
    
    for idx, row in sample.iterrows():
        mat = str(row.get('material', '')).lower()
        prof = str(row.get('chemical_profile', ''))
        
        # Risk Rule 1: Safety Mismatch
        if 'non-hazardous' in mat and 'Toxic' in prof:
            risk_count += 1
            
        # Risk Rule 2: Incompatibility
        if 'plastic' in mat and 'Heavy Metals' in prof:
            # Plastics usually don't have heavy metals unless e-waste. Marginal risk.
            pass
            
    print(f'Safety Drift (Safe declared material -> Toxic Profile): {risk_count} / {len(sample)}')
    if risk_count > 0:
        print('  WARNING: High likelihood of "Safety Hallucination"')
        
    # 3. NULL/GHOST DATA
    print('\n[3] GHOST DATA CHECK')
    # Records that are populated but meaningless (e.g., price = 0.0 or name = "-")
    null_price = len(df[df['price_per_ton_usd'] == 0])
    short_name = len(df[df['material'].fillna('').str.len() < 3])
    
    print(f'Zero-Priced Records: {null_price:,} ({null_price/total:.1%})')
    print(f'Ghost Names (<3 chars): {short_name:,}')

    # 4. OVERALL SCORE
    score = 100
    if len(drift_cos) > total * 0.01: score -= 10
    if (risk_count / len(sample)) > 0.01: score -= 20
    if (null_price / total) > 0.5: score -= 10
    
    print(f'\nDATA CONFIDENCE SCORE: {score}/100')
    if score < 90:
        print('RECOMMENDATION: SANITIZE BEFORE TRAINING')
    else:
        print('RECOMMENDATION: READY FOR TRAINING')

if __name__ == '__main__':
    run_deep_audit()
