"""
SYMBIOFLOWS VISUALIZATION SERVER
================================
Status: LIVE
Port: 5000
Data Source: exports/symbio_data_engine_READY.csv
"""
from flask import Flask, jsonify, request, send_file
import pandas as pd
import folium
from geopy.distance import geodesic
import os

app = Flask(__name__)

# CONFIG
DATA_FILE = "exports/symbio_data_engine_READY.csv"

# LOAD DATA (Optimization: Load once on startup)
print("Loading Symbio Data Engine...")
try:
    df = pd.read_csv(DATA_FILE)
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce').fillna(0)
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce').fillna(0)
    df['quantity_onsite'] = pd.to_numeric(df['quantity_onsite'], errors='coerce').fillna(50)
    df['price_per_ton_usd'] = pd.to_numeric(df['price_per_ton_usd'], errors='coerce').fillna(0)
    print(f"Data Loaded: {len(df)} records.")
except Exception as e:
    print(f"CRITICAL ERROR: Could not load data. {e}")
    df = pd.DataFrame()

@app.route('/')
def home():
    return "SymbioFlows Intelligence Server is Running."

@app.route('/api/viz/swarm')
def swarm_map():
    """Generates a Folium map of 1000 random industrial sites."""
    if df.empty: return "Error: No Data"
    
    # Sample 1000 points for performance
    sample = df.sample(min(1000, len(df)))
    
    # Center map on average or default (Paris/Europe center)
    center_lat = sample['lat'].mean()
    center_lon = sample['lon'].mean()
    if center_lat == 0: center_lat, center_lon = 48.85, 2.35
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="CartoDB dark_matter")
    
    for _, row in sample.iterrows():
        color = 'red' if row['price_per_ton_usd'] < 0 else 'green' # Red for cost, Green for revenue
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=3,
            color=color,
            fill=True,
            fill_opacity=0.6,
            popup=f"{row.get('source_company', 'Unknown')}: {row.get('waste_description', 'Waste')}"
        ).add_to(m)
        
    # Save map to temporary file
    map_path = "exports/swarm_map.html"
    m.save(map_path)
    return send_file(map_path)

@app.route('/api/analyze/revenue', methods=['POST'])
def analyze_revenue():
    """Calculates potential revenue/cost within radius of a point."""
    data = request.json
    lat = data.get('lat')
    lon = data.get('lon')
    radius = data.get('radius_km', 50)
    
    if not lat or not lon: return jsonify({"error": "Missing lat/lon"}), 400
    
    # Filter by Box first (speed)
    lat_min, lat_max = lat - 1, lat + 1
    lon_min, lon_max = lon - 1, lon + 1
    
    candidates = df[
        (df['lat'] >= lat_min) & (df['lat'] <= lat_max) &
        (df['lon'] >= lon_min) & (df['lon'] <= lon_max)
    ]
    
    # Precise Geodesic Filter
    matches = []
    total_val = 0
    total_vol = 0
    materials = {}
    
    center = (lat, lon)
    for _, row in candidates.iterrows():
        try:
            pt = (row['lat'], row['lon'])
            dist = geodesic(center, pt).km
            if dist <= radius:
                val = row['quantity_onsite'] * row['price_per_ton_usd']
                total_val += val
                total_vol += row['quantity_onsite']
                mat = row.get('waste_description', 'Unknown')
                materials[mat] = materials.get(mat, 0) + 1
        except:
            continue
            
    return jsonify({
        "total_volume_tons": total_vol,
        "recoverable_revenue_usd": total_val,
        "factories_found": len(matches) if 'matches' in locals() else 0, # Optimization
        "top_materials": dict(sorted(materials.items(), key=lambda x: x[1], reverse=True)[:5])
    })

@app.route('/api/data/download')
def download_data():
    """Allows SymbioFlows Agent to fetch the full calibrated dataset."""
    try:
        return send_file(DATA_FILE, as_attachment=True, download_name="symbio_data_v4.1.csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5000, debug=True)
