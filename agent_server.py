"""
Agent Communication Server
Simple Flask API that both agents can access to share data.
Run: python agent_server.py
Access: http://localhost:5000
"""
from flask import Flask, jsonify, request, render_template_string
import json
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

# Store messages between agents
MESSAGES = []

# HTML template for dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>SymbioFlows Agent Communication</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
               max-width: 1200px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }
        h1 { color: #58a6ff; }
        h2 { color: #8b949e; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
        .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin: 15px 0; }
        .success { color: #3fb950; }
        .warning { color: #d29922; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #30363d; }
        th { color: #58a6ff; }
        pre { background: #0d1117; padding: 15px; border-radius: 6px; overflow-x: auto; }
        .message { background: #21262d; padding: 15px; margin: 10px 0; border-radius: 8px; }
        .message-header { color: #8b949e; font-size: 12px; }
        .endpoint { background: #388bfd20; padding: 5px 10px; border-radius: 4px; font-family: monospace; }
    </style>
</head>
<body>
    <h1>🔄 SymbioFlows Agent Communication</h1>
    
    <div class="card">
        <h2>Data Status</h2>
        <table>
            <tr><th>Resource</th><th>Status</th><th>Details</th></tr>
            <tr>
                <td>industry_pricing.json</td>
                <td class="success">✅ Ready</td>
                <td>{{ pricing_stats }}</td>
            </tr>
            <tr>
                <td>Parent Categories</td>
                <td class="success">✅ Ready</td>
                <td>{{ parent_count }} categories</td>
            </tr>
            <tr>
                <td>Sub-Industries</td>
                <td class="success">✅ Ready</td>
                <td>{{ sub_count }} industries</td>
            </tr>
        </table>
    </div>
    
    <div class="card">
        <h2>API Endpoints</h2>
        <table>
            <tr><th>Endpoint</th><th>Description</th></tr>
            <tr><td><span class="endpoint">GET /api/pricing</span></td><td>Full pricing data JSON</td></tr>
            <tr><td><span class="endpoint">GET /api/industries</span></td><td>List of all industries</td></tr>
            <tr><td><span class="endpoint">GET /api/industry/{name}</span></td><td>Specific industry data</td></tr>
            <tr><td><span class="endpoint">GET /api/messages</span></td><td>Messages between agents</td></tr>
            <tr><td><span class="endpoint">POST /api/message</span></td><td>Send message to other agent</td></tr>
        </table>
    </div>
    
    <div class="card">
        <h2>Messages</h2>
        {% if messages %}
            {% for msg in messages %}
            <div class="message">
                <div class="message-header">{{ msg.from }} → {{ msg.to }} | {{ msg.time }}</div>
                <div>{{ msg.content }}</div>
            </div>
            {% endfor %}
        {% else %}
            <p>No messages yet. Agents can POST to /api/message</p>
        {% endif %}
    </div>
    
    <div class="card">
        <h2>Parent Categories (Dropdown Tier 1)</h2>
        <table>
            <tr><th>Category</th><th>Sub-Industries</th></tr>
            {% for cat, data in parent_categories.items() %}
            <tr>
                <td><strong>{{ cat }}</strong></td>
                <td>{{ data.sub_industries | join(', ') }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
    
    <script>
        // Auto-refresh messages every 5 seconds
        setInterval(() => {
            fetch('/api/messages')
                .then(r => r.json())
                .then(data => {
                    if (data.messages.length > {{ messages|length }}) {
                        location.reload();
                    }
                });
        }, 5000);
    </script>
</body>
</html>
"""

def load_pricing():
    """Load pricing data."""
    try:
        with open("exports/industry_pricing.json") as f:
            return json.load(f)
    except:
        return {}

@app.route('/')
def dashboard():
    pricing = load_pricing()
    parent_categories = pricing.get("parent_categories", {})
    sub_industries = pricing.get("sub_industries", {})
    
    return render_template_string(DASHBOARD_HTML,
        pricing_stats=f"v{pricing.get('version', '?')}, {len(pricing.get('materials', {}))} materials",
        parent_count=len(parent_categories),
        sub_count=len(sub_industries),
        parent_categories=parent_categories,
        messages=MESSAGES
    )

@app.route('/api/pricing')
def api_pricing():
    """Full pricing data."""
    return jsonify(load_pricing())

@app.route('/api/industries')
def api_industries():
    """List all industries."""
    pricing = load_pricing()
    return jsonify({
        "parent_categories": list(pricing.get("parent_categories", {}).keys()),
        "sub_industries": list(pricing.get("sub_industries", {}).keys()),
        "volume_tiers": list(pricing.get("volume_tiers", {}).keys()),
        "regions": list(pricing.get("regional_modifiers", {}).keys())
    })

@app.route('/api/industry/<name>')
def api_industry(name):
    """Get specific industry data."""
    pricing = load_pricing()
    
    # Check parent categories
    if name in pricing.get("parent_categories", {}):
        return jsonify({
            "type": "parent_category",
            "name": name,
            "data": pricing["parent_categories"][name]
        })
    
    # Check sub-industries
    if name in pricing.get("sub_industries", {}):
        return jsonify({
            "type": "sub_industry", 
            "name": name,
            "data": pricing["sub_industries"][name]
        })
    
    return jsonify({"error": f"Industry '{name}' not found"}), 404

@app.route('/api/messages')
def api_messages():
    """Get all messages."""
    return jsonify({"messages": MESSAGES})

@app.route('/api/message', methods=['POST'])
def api_send_message():
    """Send a message to other agent."""
    data = request.get_json() or {}
    msg = {
        "from": data.get("from", "unknown"),
        "to": data.get("to", "all"),
        "content": data.get("content", ""),
        "time": datetime.now().isoformat()
    }
    MESSAGES.append(msg)
    return jsonify({"status": "sent", "message": msg})

@app.route('/api/calculate', methods=['POST'])
def api_calculate():
    """Calculate values for an industry/volume/region combo."""
    data = request.get_json() or {}
    pricing = load_pricing()
    
    industry = data.get("industry")
    parent = data.get("parent_category")
    volume_tier = data.get("volume_tier", "medium")
    region = data.get("region", "north_america")
    
    # Get industry data
    if industry and industry in pricing.get("sub_industries", {}):
        ind_data = pricing["sub_industries"][industry]
    elif parent and parent in pricing.get("parent_categories", {}):
        ind_data = pricing["parent_categories"][parent].get("default", {})
    else:
        return jsonify({"error": "Industry or parent category required"}), 400
    
    # Get multipliers
    volume_mult = pricing["volume_tiers"].get(volume_tier, {}).get("multiplier", 5000)
    region_mod = pricing["regional_modifiers"].get(region, {}).get("modifier", 1.0)
    
    # Calculate
    total_value = 0
    total_co2 = 0
    materials = pricing.get("materials", {})
    
    for item in ind_data.get("waste_profile", []):
        mat_key = item["material"]
        percent = item["percent"] / 100
        volume = volume_mult * percent
        
        if mat_key in materials:
            mat = materials[mat_key]
            avg_price = (mat["price_low"] + mat["price_high"]) / 2 * region_mod
            value = volume * avg_price
            co2 = volume * mat["co2_factor"]
            total_value += value
            total_co2 += co2
    
    return jsonify({
        "industry": industry or f"{parent} (default)",
        "volume_tier": volume_tier,
        "region": region,
        "base_volume_tons": volume_mult,
        "annual_value_usd": round(total_value, 2),
        "co2_reduction_tons": round(total_co2, 2),
        "baseline_diversion": ind_data.get("baseline_diversion_rate") or ind_data.get("baseline_diversion"),
        "max_diversion": ind_data.get("max_diversion_rate") or ind_data.get("max_diversion")
    })


if __name__ == '__main__':
    print("="*60)
    print("AGENT COMMUNICATION SERVER")
    print("="*60)
    print("\n  Dashboard: http://localhost:5000")
    print("  API:       http://localhost:5000/api/pricing")
    print("\n  Other agent can access:")
    print("    GET  /api/industries     - list all industries")
    print("    GET  /api/industry/{name} - get industry data")
    print("    POST /api/calculate      - calculate report values")
    print("    POST /api/message        - send message to this agent")
    print("\n" + "="*60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)

# -------------------------------------------------------------------------
# SYMBIOFLOWS VISUALIZATION EXTENSIONS
# -------------------------------------------------------------------------
try:
    import pandas as pd
    import numpy as np
    import folium
    
    # GLOBAL DATA LOADER
    DATA_PATH = 'exports/symbio_data_engine_v1.csv'
    DATA_ENGINE_DF = None
    
    try:
        if Path(DATA_PATH).exists():
            DATA_ENGINE_DF = pd.read_csv(DATA_PATH)
            # Fill missing quantity with default for demo
            if 'quantity' not in DATA_ENGINE_DF.columns:
                DATA_ENGINE_DF['quantity'] = 50.0
            print(f" [SymbioFlows] Loaded Data Engine: {len(DATA_ENGINE_DF)} records")
        else:
            print(f" [SymbioFlows] Warning: {DATA_PATH} not found.")
    except Exception as e:
        print(f" [SymbioFlows] Data Load Error: {e}")

    @app.route('/api/viz/swarm')
    def viz_swarm():
        """Generates and serves the Swarm Map HTML."""
        if DATA_ENGINE_DF is None: 
            return "<h3>Error: Data Engine CSV not loaded. Check server logs.</h3>", 500
        
        # Filter for key industries to show clustering
        # Sample max 1000 points for performance
        relevant = DATA_ENGINE_DF[DATA_ENGINE_DF['industry'].isin(['Chemicals', 'Food/Ag', 'Metals & Mining'])]
        if relevant.empty: relevant = DATA_ENGINE_DF # Fallback if specific industries missing
        
        sample = relevant.sample(min(1000, len(relevant)))
        
        # Visual Center
        center_lat = sample['lat'].mean()
        center_lon = sample['lon'].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles='CartoDB dark_matter')
        
        colors = {'Chemicals': '#FF4136', 'Food/Ag': '#2ECC40', 'Metals & Mining': '#0074D9'}
        
        for _, row in sample.iterrows():
            ind = str(row.get('industry', 'Unknown'))
            # Simple color match
            color = '#888888'
            for k, v in colors.items():
                if k in ind: color = v
            
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=3, color=color, fill=True, fill_color=color,
                popup=folium.Popup(f"<b>{row.get('source_company')}</b><br>{ind}<br>${row.get('price_per_ton_usd',0)}/ton", max_width=200)
            ).add_to(m)
            
        return m.get_root().render()

    @app.route('/api/analyze/revenue', methods=['POST'])
    def analyze_revenue():
        """Calculates revenue potential in radius."""
        if DATA_ENGINE_DF is None: return jsonify({"error": "Data not loaded"}), 500
        
        data = request.json or {}
        try:
            lat = float(data.get('lat')) if data.get('lat') else None
            lon = float(data.get('lon')) if data.get('lon') else None
        except:
            lat, lon = None, None
            
        radius_km = float(data.get('radius_km', 5))
        
        if lat is None or lon is None:
            # Default to a random chemical factory if no coords provided
            target = DATA_ENGINE_DF[DATA_ENGINE_DF['industry'] == 'Chemicals']
            if not target.empty:
                target = target.iloc[0]
                lat, lon = target['lat'], target['lon']
            else:
                lat, lon = DATA_ENGINE_DF['lat'].mean(), DATA_ENGINE_DF['lon'].mean()
        
        # Vectorized Haversine
        R = 6371
        dlat = np.radians(DATA_ENGINE_DF['lat'] - lat)
        dlon = np.radians(DATA_ENGINE_DF['lon'] - lon)
        a = np.sin(dlat/2)**2 + np.cos(np.radians(lat)) * np.cos(np.radians(DATA_ENGINE_DF['lat'])) * np.sin(dlon/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        dist_km = R * c
        
        # Filter
        cluster = DATA_ENGINE_DF[dist_km <= radius_km].copy()
        
        # Revenue = Price * Quantity
        # Ensure numeric
        price = pd.to_numeric(cluster['price_per_ton_usd'], errors='coerce').fillna(0)
        qty = pd.to_numeric(cluster['quantity'], errors='coerce').fillna(0)
        revenue = price * qty
        
        return jsonify({
            "center": {"lat": lat, "lon": lon},
            "radius_km": radius_km,
            "factories_found": int(len(cluster)),
            "total_volume_tons": float(qty.sum()),
            "recoverable_revenue_usd": float(revenue.sum()),
            "top_materials": cluster['material'].value_counts().head(3).to_dict()
        })

except ImportError as e:
    print(f" [SymbioFlows] Visualization extras skipped: Missing dependency ({e})")
