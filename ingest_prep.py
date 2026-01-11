"""
INGEST PREP: SYMBIO DATA ENGINE
===============================
Goal: Master enrichment script to prepare the "Final Asset" for SymbioFlows.
Features:
1. Streaming Process (Handle 850k rows without RAM issues).
2. Two-Tier Enrichment:
   - TIER 1 (Everyone): Price Estimates + CO2 Factors + Default Volume (50t).
   - TIER 2 (Alpha): Process Context + Accurate CAS + Real Volume.
Output: symbio_data_engine_READY.csv
"""
import csv
import json
import pandas as pd
from pathlib import Path

# --- CONFIG ---
INPUT_FILE = "exports/waste_listings_granular.csv"
OUTPUT_FILE = "exports/symbio_data_engine_READY.csv"
PRICING_FILE = r"C:\Users\Imrry\.gemini\antigravity\brain\8396ab0b-4735-4ee7-aa47-4ce37f95cad0\industry_pricing_corrected.json"
CO2_FILE = "exports/co2_factors.json"
KNOWLEDGE_FILE = "exports/process_knowledge_v1.csv"

def load_references():
    print("Loading reference data...")
    
    # 1. Pricing
    try:
        with open(PRICING_FILE, 'r') as f:
            pricing_data = json.load(f)
            # Flatten if nested or just use raw if simple. 
            # Assuming structure: {"materials": {"steel": {...}}} or flat.
            # Adaptation based on previous `cat` output:
            if "materials" in pricing_data:
                prices = pricing_data["materials"]
            else:
                prices = pricing_data
    except:
        prices = {} # Fallback

    # 2. CO2
    try:
        with open(CO2_FILE, 'r') as f:
            co2_data = json.load(f)
    except:
        co2_data = {}

    # 3. Knowledge Base (Alpha)
    try:
        kb_df = pd.read_csv(KNOWLEDGE_FILE)
        # Create a dictionary for fast lookup: Company -> {context, volume, cas}
        kb_map = {}
        for _, row in kb_df.iterrows():
            comp = str(row['Company']).strip().lower()
            if comp not in kb_map:
                kb_map[comp] = {'context': [], 'volume': None, 'cas': []}
            
            # Add context
            if "Context_" in str(row['Keyword']):
                kb_map[comp]['context'].append(str(row['Context (Excerpt)'])[:100])
            
            # Capture Volume (First found)
            if "DATA_VOLUME" == row['Keyword'] and not kb_map[comp]['volume']:
                 # Extract number from string "25,000 tonnes..."
                 import re
                 nums = re.findall(r'[\d,]+', str(row['Context (Excerpt)']))
                 if nums:
                     kb_map[comp]['volume'] = nums[0].replace(',', '')

            # Capture CAS
            if "DATA_CAS_NUMBER" == row['Keyword']:
                 kb_map[comp]['cas'].append(str(row['Context (Excerpt)']).split('(')[0].strip())

        print(f"Loaded Knowledge Base: {len(kb_map)} companies.")
    except Exception as e:
        print(f"Error loading Knowledge Base: {e}")
        kb_map = {}

    return prices, co2_data, kb_map

def get_enrichment(text, prices, co2s):
    text = str(text).lower()
    price = 0
    co2 = 0
    
    # Simple Keyword Match (Optimize this for production)
    for key, data in prices.items():
        if key.replace('_', ' ') in text:
            # Handle if data is dict or scalar
            if isinstance(data, dict):
                price = data.get('price_low', 0)
            else:
                price = data
            break
            
    for key, val in co2s.items():
         if key.replace('_', ' ') in text:
             co2 = val
             break
             
    return price, co2

def run_pipeline():
    prices, co2s, kb_map = load_references()
    
    print("Starting Streaming Enrichment...")
    
    with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as infile, \
         open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + [
            'price_per_ton_usd', 'co2_factor', 'quantity_onsite', 
            'process_context', 'cas_numbers', 'is_alpha_verified'
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        alpha_count = 0
        
        for row in reader:
            # 1. Base Enrichment (Light)
            desc = row.get('waste_description', '')
            p, c = get_enrichment(desc, prices, co2s)
            
            row['price_per_ton_usd'] = p
            row['co2_factor'] = c
            row['quantity_onsite'] = 50 # Default
            row['is_alpha_verified'] = 'False'
            row['process_context'] = ''
            row['cas_numbers'] = ''

            # 2. Alpha Enrichment (Heavy)
            company = str(row.get('source_company', '')).strip().lower()
            if company in kb_map:
                k_data = kb_map[company]
                row['is_alpha_verified'] = 'True'
                row['process_context'] = " | ".join(k_data['context'][:2])
                row['cas_numbers'] = ", ".join(list(set(k_data['cas'])))
                
                # If we found real volume, use it
                if k_data['volume']:
                    try:
                        raw_vol = float(k_data['volume'])
                        # Unit Normalization Logic
                        context_lower = row['process_context'].lower()
                        if 'barrel' in context_lower or 'bbl' in context_lower:
                             # 1 Barrel of water/mud approx 0.159 tons (using 0.15 conservative)
                             row['quantity_onsite'] = raw_vol * 0.15
                        elif 'gallon' in context_lower:
                             # 1 Gallon water approx 0.00378 tons
                             row['quantity_onsite'] = raw_vol * 0.00378
                        else:
                             row['quantity_onsite'] = raw_vol
                    except:
                        pass
                alpha_count += 1

            writer.writerow(row)
            count += 1
            if count % 50000 == 0:
                print(f"Processed {count} rows... (Alpha: {alpha_count})")

    print(f"\nSUCCESS: Generated {OUTPUT_FILE}")
    print(f"Total Rows: {count}")
    print(f"Alpha Verified Rows: {alpha_count}")

if __name__ == "__main__":
    run_pipeline()
