"""
Amtrak Real-Time Data Collector for Railway.app
================================================
Collects train data from Amtraker API every 30 seconds
Stores historical KPI snapshots for dashboard retrieval

Deploy this to Railway.app for 24/7 data collection.
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AmtrakCollector")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Configuration
AMTRAKER_API = "https://api-v3.amtraker.com/v3"
COLLECTION_INTERVAL = 30  # seconds
MAX_SNAPSHOTS = 2880  # 24 hours of data at 30-second intervals

# In-memory storage (for Railway's free tier without persistent storage)
# For production, use PostgreSQL or Redis
data_store = {
    "latest": None,
    "snapshots": deque(maxlen=MAX_SNAPSHOTS),
    "stats": {
        "total_records": 0,
        "collection_started": None,
        "last_collection": None
    }
}

def calculate_kpi(trains):
    """Calculate KPI metrics from train data."""
    if not trains:
        return None
    
    speeds = [t.get("speed", 0) or 0 for t in trains]
    delays = [t.get("delay", 0) or 0 for t in trains]
    
    avg_speed = sum(speeds) / len(speeds) if speeds else 0
    max_speed = max(speeds) if speeds else 0
    moving = sum(1 for s in speeds if s > 5)
    stopped = len(trains) - moving
    delayed = sum(1 for d in delays if d > 0)
    
    # OEE calculation (simplified)
    target_speed = 55  # mph
    oee = min((avg_speed / target_speed) * 100, 100) if avg_speed > 0 else 0
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "total_trains": len(trains),
        "moving": moving,
        "stopped": stopped,
        "delayed": delayed,
        "avg_speed": round(avg_speed, 2),
        "max_speed": round(max_speed, 2),
        "oee": round(oee, 2),
        "on_time_pct": round((len(trains) - delayed) / len(trains) * 100, 2) if trains else 100
    }

def fetch_trains():
    """Fetch train data from Amtraker API."""
    try:
        response = requests.get(f"{AMTRAKER_API}/trains", timeout=10)
        if response.status_code != 200:
            logger.error(f"Amtraker API error: {response.status_code}")
            return []
        
        data = response.json()
        trains = []
        
        for train_num, train_array in data.items():
            if isinstance(train_array, list):
                for train in train_array:
                    if train.get("lat") and train.get("lon"):
                        delay = 0
                        if train.get("trainTimely"):
                            try:
                                delay = int(train["trainTimely"])
                            except (ValueError, TypeError):
                                delay = 0
                        
                        trains.append({
                            "id": train.get("trainID", train_num),
                            "trainNum": train.get("trainNum", train_num),
                            "latitude": train["lat"],
                            "longitude": train["lon"],
                            "speed": train.get("velocity", 0) or 0,
                            "routeName": train.get("routeName", "Unknown"),
                            "status": train.get("trainState", "Active"),
                            "origin": train.get("origName", ""),
                            "destination": train.get("destName", ""),
                            "delay": delay,
                            "heading": train.get("heading", 0)
                        })
        
        return trains
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return []

def collect_data():
    """Background thread to collect data periodically."""
    data_store["stats"]["collection_started"] = datetime.utcnow().isoformat()
    
    while True:
        try:
            trains = fetch_trains()
            
            if trains:
                # Store latest train positions
                data_store["latest"] = {
                    "trains": trains,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Calculate and store KPI snapshot
                kpi = calculate_kpi(trains)
                if kpi:
                    data_store["snapshots"].append(kpi)
                    data_store["stats"]["total_records"] += 1
                
                data_store["stats"]["last_collection"] = datetime.utcnow().isoformat()
                
                logger.info(f"Collected {len(trains)} trains, {len(data_store['snapshots'])} snapshots stored")
        
        except Exception as e:
            logger.error(f"Collection error: {e}")
        
        time.sleep(COLLECTION_INTERVAL)

# Start background collection thread
collector_thread = threading.Thread(target=collect_data, daemon=True)
collector_thread.start()

# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route("/")
def home():
    """Health check endpoint."""
    return jsonify({
        "status": "running",
        "service": "Amtrak Data Collector",
        "version": "2.0",
        "endpoints": [
            "/api/latest",
            "/api/kpi/snapshots",
            "/api/kpi/latest",
            "/api/stats"
        ]
    })

@app.route("/api/latest")
def get_latest():
    """Get latest train positions."""
    if data_store["latest"]:
        return jsonify(data_store["latest"])
    return jsonify({"trains": [], "timestamp": None})

@app.route("/api/kpi/latest")
def get_latest_kpi():
    """Get latest KPI snapshot."""
    if data_store["snapshots"]:
        return jsonify(data_store["snapshots"][-1])
    return jsonify({})

@app.route("/api/kpi/snapshots")
def get_snapshots():
    """Get historical KPI snapshots."""
    limit = request.args.get("limit", 100, type=int)
    hours = request.args.get("hours", None, type=int)
    
    snapshots = list(data_store["snapshots"])
    
    # Filter by hours if specified
    if hours:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        snapshots = [
            s for s in snapshots 
            if datetime.fromisoformat(s["timestamp"]) > cutoff
        ]
    
    # Return most recent snapshots up to limit
    return jsonify({
        "snapshots": snapshots[-limit:],
        "total": len(snapshots),
        "collection_interval": COLLECTION_INTERVAL
    })

@app.route("/api/stats")
def get_stats():
    """Get collection statistics."""
    return jsonify({
        **data_store["stats"],
        "snapshots_stored": len(data_store["snapshots"]),
        "max_snapshots": MAX_SNAPSHOTS,
        "collection_interval": COLLECTION_INTERVAL
    })

@app.route("/api/routes")
def get_routes():
    """Get unique routes from current trains."""
    if not data_store["latest"]:
        return jsonify([])
    
    routes = {}
    for train in data_store["latest"]["trains"]:
        name = train.get("routeName", "Unknown")
        if name not in routes:
            routes[name] = {"name": name, "count": 0, "speeds": []}
        routes[name]["count"] += 1
        routes[name]["speeds"].append(train.get("speed", 0))
    
    # Calculate average speeds
    for route in routes.values():
        route["avg_speed"] = round(sum(route["speeds"]) / len(route["speeds"]), 2) if route["speeds"] else 0
        del route["speeds"]
    
    return jsonify(list(routes.values()))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
