"""
Amtrak Real-Time Data Collector with PostgreSQL
================================================
Collects train data from Amtraker API every 30 seconds
Stores historical KPI snapshots in PostgreSQL for months of data

Uses psycopg3 for Python 3.13 compatibility.
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
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AmtrakCollector")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Configuration
AMTRAKER_API = "https://api-v3.amtraker.com/v3"
COLLECTION_INTERVAL = 30  # seconds

# Database URL from Railway
DATABASE_URL = os.environ.get("DATABASE_URL")

# In-memory cache for latest data (fast access)
latest_cache = {
    "trains": [],
    "timestamp": None,
    "kpi": None
}

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

@contextmanager
def get_db_connection():
    """Get database connection with automatic cleanup."""
    conn = None
    try:
        conn = psycopg.connect(DATABASE_URL)
        yield conn
    finally:
        if conn:
            conn.close()

def init_database():
    """Initialize PostgreSQL tables."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return False
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # KPI Snapshots table - stores metrics every 30 seconds
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kpi_snapshots (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT NOW(),
                        total_trains INTEGER,
                        moving INTEGER,
                        stopped INTEGER,
                        delayed INTEGER,
                        avg_speed FLOAT,
                        max_speed FLOAT,
                        oee FLOAT,
                        on_time_pct FLOAT
                    )
                """)
                
                # Train positions table - stores individual train data
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS train_positions (
                        id SERIAL PRIMARY KEY,
                        snapshot_id INTEGER REFERENCES kpi_snapshots(id),
                        train_num VARCHAR(20),
                        route_name VARCHAR(100),
                        latitude FLOAT,
                        longitude FLOAT,
                        speed FLOAT,
                        delay INTEGER,
                        origin VARCHAR(100),
                        destination VARCHAR(100),
                        status VARCHAR(50)
                    )
                """)
                
                # Index for faster queries
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_kpi_timestamp 
                    ON kpi_snapshots(timestamp DESC)
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_train_snapshot 
                    ON train_positions(snapshot_id)
                """)
                
                conn.commit()
                logger.info("Database initialized successfully")
                return True
    except Exception as e:
        logger.error(f"Database init error: {e}")
        return False

def save_snapshot(kpi, trains):
    """Save KPI snapshot and train positions to database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Insert KPI snapshot
                cur.execute("""
                    INSERT INTO kpi_snapshots 
                    (total_trains, moving, stopped, delayed, avg_speed, max_speed, oee, on_time_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    kpi["total_trains"],
                    kpi["moving"],
                    kpi["stopped"],
                    kpi["delayed"],
                    kpi["avg_speed"],
                    kpi["max_speed"],
                    kpi["oee"],
                    kpi["on_time_pct"]
                ))
                snapshot_id = cur.fetchone()[0]
                
                # Insert train positions (sample every 10th train to save space)
                for i, train in enumerate(trains):
                    if i % 10 == 0:  # Store 10% of trains to save DB space
                        cur.execute("""
                            INSERT INTO train_positions
                            (snapshot_id, train_num, route_name, latitude, longitude, speed, delay, origin, destination, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            snapshot_id,
                            train.get("trainNum", ""),
                            train.get("routeName", ""),
                            train.get("latitude", 0),
                            train.get("longitude", 0),
                            train.get("speed", 0),
                            train.get("delay", 0),
                            train.get("origin", ""),
                            train.get("destination", ""),
                            train.get("status", "")
                        ))
                
                conn.commit()
                return snapshot_id
    except Exception as e:
        logger.error(f"Save snapshot error: {e}")
        return None

def get_snapshots(limit=100, hours=None, days=None):
    """Retrieve historical snapshots from database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if days:
                    cur.execute("""
                        SELECT * FROM kpi_snapshots 
                        WHERE timestamp > NOW() - INTERVAL '%s days'
                        ORDER BY timestamp ASC
                    """, (days,))
                elif hours:
                    cur.execute("""
                        SELECT * FROM kpi_snapshots 
                        WHERE timestamp > NOW() - INTERVAL '%s hours'
                        ORDER BY timestamp ASC
                    """, (hours,))
                else:
                    cur.execute("""
                        SELECT * FROM kpi_snapshots 
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (limit,))
                
                rows = cur.fetchall()
                return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Get snapshots error: {e}")
        return []

def get_stats():
    """Get database statistics."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT COUNT(*) as total FROM kpi_snapshots")
                total = cur.fetchone()["total"]
                
                cur.execute("SELECT MIN(timestamp) as first, MAX(timestamp) as last FROM kpi_snapshots")
                times = cur.fetchone()
                
                cur.execute("""
                    SELECT COUNT(*) as today FROM kpi_snapshots 
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                """)
                today = cur.fetchone()["today"]
                
                return {
                    "total_snapshots": total,
                    "snapshots_today": today,
                    "first_record": times["first"].isoformat() if times["first"] else None,
                    "last_record": times["last"].isoformat() if times["last"] else None,
                    "collection_interval": COLLECTION_INTERVAL,
                    "database": "PostgreSQL"
                }
    except Exception as e:
        logger.error(f"Get stats error: {e}")
        return {"error": str(e)}

# =============================================================================
# DATA COLLECTION
# =============================================================================

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
    
    target_speed = 55
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
    # Wait for database to be ready
    time.sleep(5)
    init_database()
    
    while True:
        try:
            trains = fetch_trains()
            
            if trains:
                kpi = calculate_kpi(trains)
                
                # Update in-memory cache
                latest_cache["trains"] = trains
                latest_cache["timestamp"] = datetime.utcnow().isoformat()
                latest_cache["kpi"] = kpi
                
                # Save to PostgreSQL
                if kpi and DATABASE_URL:
                    snapshot_id = save_snapshot(kpi, trains)
                    if snapshot_id:
                        logger.info(f"Saved snapshot #{snapshot_id}: {len(trains)} trains, OEE={kpi['oee']:.1f}%")
                    else:
                        logger.warning("Failed to save snapshot to database")
                else:
                    logger.info(f"Collected {len(trains)} trains (no database)")
        
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
        "version": "3.0 (PostgreSQL + psycopg3)",
        "database": "connected" if DATABASE_URL else "not configured",
        "endpoints": [
            "GET /api/latest - Current train positions",
            "GET /api/kpi/latest - Latest KPI metrics",
            "GET /api/kpi/snapshots?limit=100 - Historical snapshots",
            "GET /api/kpi/snapshots?hours=24 - Last 24 hours",
            "GET /api/kpi/snapshots?days=7 - Last 7 days",
            "GET /api/stats - Collection statistics",
            "GET /api/export/csv - Export all data as CSV"
        ]
    })

@app.route("/api/latest")
def get_latest():
    """Get latest train positions."""
    return jsonify({
        "trains": latest_cache["trains"],
        "timestamp": latest_cache["timestamp"]
    })

@app.route("/api/kpi/latest")
def get_latest_kpi():
    """Get latest KPI snapshot."""
    return jsonify(latest_cache["kpi"] or {})

@app.route("/api/kpi/snapshots")
def api_get_snapshots():
    """Get historical KPI snapshots."""
    limit = request.args.get("limit", 100, type=int)
    hours = request.args.get("hours", None, type=int)
    days = request.args.get("days", None, type=int)
    
    snapshots = get_snapshots(limit=limit, hours=hours, days=days)
    
    # Convert timestamps to ISO format strings
    for s in snapshots:
        if s.get("timestamp"):
            s["timestamp"] = s["timestamp"].isoformat()
    
    return jsonify({
        "snapshots": snapshots,
        "total": len(snapshots),
        "collection_interval": COLLECTION_INTERVAL
    })

@app.route("/api/stats")
def api_get_stats():
    """Get collection statistics."""
    return jsonify(get_stats())

@app.route("/api/routes")
def get_routes():
    """Get unique routes from current trains."""
    routes = {}
    for train in latest_cache["trains"]:
        name = train.get("routeName", "Unknown")
        if name not in routes:
            routes[name] = {"name": name, "count": 0, "speeds": [], "delays": []}
        routes[name]["count"] += 1
        routes[name]["speeds"].append(train.get("speed", 0))
        routes[name]["delays"].append(train.get("delay", 0))
    
    for route in routes.values():
        route["avg_speed"] = round(sum(route["speeds"]) / len(route["speeds"]), 2) if route["speeds"] else 0
        route["avg_delay"] = round(sum(route["delays"]) / len(route["delays"]), 2) if route["delays"] else 0
        del route["speeds"]
        del route["delays"]
    
    return jsonify(list(routes.values()))

@app.route("/api/export/csv")
def export_csv():
    """Export all KPI data as CSV."""
    days = request.args.get("days", 30, type=int)
    snapshots = get_snapshots(days=days)
    
    if not snapshots:
        return "No data available", 404
    
    # Build CSV
    headers = ["timestamp", "total_trains", "moving", "stopped", "delayed", "avg_speed", "max_speed", "oee", "on_time_pct"]
    lines = [",".join(headers)]
    
    for s in snapshots:
        row = [str(s.get(h, "")) for h in headers]
        lines.append(",".join(row))
    
    csv_content = "\n".join(lines)
    
    return csv_content, 200, {
        "Content-Type": "text/csv",
        "Content-Disposition": f"attachment; filename=amtrak_kpi_{days}days.csv"
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
