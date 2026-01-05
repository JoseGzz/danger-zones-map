import os
from flask import Flask, jsonify, render_template
from databricks import sql
import pandas as pd

app = Flask(__name__)

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# -------------------------------------------------------------------------
# DATA ACCESS LAYER
# -------------------------------------------------------------------------
def get_danger_data():
    """
    Fetches latest points and calculates summary statistics for the frontend.
    Returns a dict with 'points' (all coords) and 'top_zones' (summary).
    """
    if not DATABRICKS_SERVER_HOSTNAME or not DATABRICKS_HTTP_PATH:
        print("Missing Databricks Configuration")
        return None

    try:
        connection = sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        cursor = connection.cursor()
        
        # 1. Fetch Raw Points from the latest run
        query = """
        SELECT zone_id, center_lat, center_lon
        FROM workspace.default.gold_danger_zones
        WHERE run_timestamp = (
            SELECT MAX(run_timestamp) 
            FROM workspace.default.gold_danger_zones
        )
        """
        cursor.execute(query)
        result = cursor.fetchall()
        
        if not result:
            return None

        # Convert to list of dicts, ensuring coords are Floats 
        columns = [desc[0] for desc in cursor.description]
        raw_data = []
        for row in result:
            row_dict = dict(zip(columns, row))
            row_dict['center_lat'] = float(row_dict['center_lat'])
            row_dict['center_lon'] = float(row_dict['center_lon'])
            raw_data.append(row_dict)

        # 2. Process with Pandas to find Top 5 Largest Zones
        df = pd.DataFrame(raw_data)
        
        if df.empty:
            return {"points": [], "top_zones": []}

        # Group by zone_id to find center and count
        summary = df.groupby('zone_id').agg(
            center_lat=('center_lat', 'mean'),
            center_lon=('center_lon', 'mean'),
            report_count=('zone_id', 'count')
        ).reset_index()

        # Sort by count (descending) and take top 5
        top_zones_df = summary.sort_values('report_count', ascending=False).head(5)
        top_zones = top_zones_df.to_dict(orient='records')

        connection.close()
        
        return {
            "points": raw_data,
            "top_zones": top_zones
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# -------------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------------

@app.route('/api/danger-zones')
def api_danger_zones():
    data = get_danger_data()
    if data is None:
        return jsonify({"error": "Failed to fetch data (Check logs/config)"}), 500
    return jsonify(data)

@app.route('/')
def index():
    # Looks for 'index.html' inside the 'templates' folder
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port)