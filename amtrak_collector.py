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
                
                # Add missing columns if table already existed
                columns_to_add = [
                    ("moving", "INTEGER"),
                    ("stopped", "INTEGER"),
                    ("delayed", "INTEGER"),
                    ("avg_speed", "FLOAT"),
                    ("max_speed", "FLOAT"),
                    ("oee", "FLOAT"),
                    ("on_time_pct", "FLOAT")
                ]
                
                for col_name, col_type in columns_to_add:
                    try:
                        cur.execute(f"ALTER TABLE kpi_snapshots ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                    except Exception:
                        pass  # Column might already exist
                
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
    app.run(host="0.0.0.0", port=port, debug=False)"""
Amtrak Real-Time Data Collector - Production Grade
===================================================
Enhanced collector with:
- Hourly/Daily rollup tables for fast queries
- Route-level KPIs (OTP, headway, performance)
- Smart data retention (7 days raw, forever aggregated)
- Health monitoring
- Optimized API endpoints for trend analysis

Deploy to Railway.app with PostgreSQL.
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
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AmtrakCollector")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Configuration
AMTRAKER_API = "https://api-v3.amtraker.com/v3"
COLLECTION_INTERVAL = 30  # seconds
RAW_RETENTION_DAYS = 7    # Keep raw snapshots for 7 days
AGGREGATION_INTERVAL = 3600  # Run aggregation every hour

# Database URL from Railway
DATABASE_URL = os.environ.get("DATABASE_URL")

# In-memory state
latest_cache = {
    "trains": [],
    "timestamp": None,
    "kpi": None,
    "routes": {},
    "last_positions": {}
}

collection_stats = {
    "started_at": None,
    "total_collections": 0,
    "successful_saves": 0,
    "failed_saves": 0,
    "last_error": None,
    "last_aggregation": None
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
    """Initialize all PostgreSQL tables."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return False
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # RAW DATA TABLES (7-day retention)
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
                
                # Add missing columns if table existed
                for col, ctype in [("moving", "INTEGER"), ("stopped", "INTEGER"), 
                                   ("delayed", "INTEGER"), ("avg_speed", "FLOAT"),
                                   ("max_speed", "FLOAT"), ("oee", "FLOAT"), 
                                   ("on_time_pct", "FLOAT")]:
                    try:
                        cur.execute(f"ALTER TABLE kpi_snapshots ADD COLUMN IF NOT EXISTS {col} {ctype}")
                    except:
                        pass
                
                # Route snapshots - per-route metrics every 30 seconds
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS route_snapshots (
                        id SERIAL PRIMARY KEY,
                        snapshot_id INTEGER REFERENCES kpi_snapshots(id) ON DELETE CASCADE,
                        route_name VARCHAR(100),
                        train_count INTEGER,
                        avg_speed FLOAT,
                        max_speed FLOAT,
                        delayed_count INTEGER,
                        on_time_pct FLOAT,
                        avg_delay FLOAT,
                        headway_minutes FLOAT
                    )
                """)
                
                # AGGREGATED TABLES (kept forever)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kpi_hourly (
                        id SERIAL PRIMARY KEY,
                        hour_start TIMESTAMPTZ UNIQUE,
                        sample_count INTEGER,
                        avg_trains FLOAT,
                        min_trains INTEGER,
                        max_trains INTEGER,
                        avg_moving FLOAT,
                        avg_stopped FLOAT,
                        avg_delayed FLOAT,
                        avg_speed FLOAT,
                        min_speed FLOAT,
                        max_speed FLOAT,
                        avg_oee FLOAT,
                        min_oee FLOAT,
                        max_oee FLOAT,
                        avg_on_time_pct FLOAT
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kpi_daily (
                        id SERIAL PRIMARY KEY,
                        date DATE UNIQUE,
                        sample_count INTEGER,
                        avg_trains FLOAT,
                        min_trains INTEGER,
                        max_trains INTEGER,
                        avg_speed FLOAT,
                        avg_oee FLOAT,
                        min_oee FLOAT,
                        max_oee FLOAT,
                        avg_on_time_pct FLOAT,
                        peak_hour INTEGER,
                        peak_trains INTEGER
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS route_daily (
                        id SERIAL PRIMARY KEY,
                        date DATE,
                        route_name VARCHAR(100),
                        sample_count INTEGER,
                        avg_trains FLOAT,
                        avg_speed FLOAT,
                        avg_delay FLOAT,
                        on_time_pct FLOAT,
                        avg_headway FLOAT,
                        reliability_score FLOAT,
                        UNIQUE(date, route_name)
                    )
                """)
                
                # INDEXES
                cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_timestamp ON kpi_snapshots(timestamp DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_route_snapshot ON route_snapshots(snapshot_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_route_name ON route_snapshots(route_name)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_hourly_time ON kpi_hourly(hour_start DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON kpi_daily(date DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_route_daily ON route_daily(date DESC, route_name)")
                
                conn.commit()
                logger.info("Database initialized successfully with all tables")
                return True
    except Exception as e:
        logger.error(f"Database init error: {e}")
        return False

def save_snapshot(kpi, trains, route_metrics):
    """Save KPI snapshot with route-level data."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kpi_snapshots 
                    (total_trains, moving, stopped, delayed, avg_speed, max_speed, oee, on_time_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    kpi["total_trains"], kpi["moving"], kpi["stopped"], kpi["delayed"],
                    kpi["avg_speed"], kpi["max_speed"], kpi["oee"], kpi["on_time_pct"]
                ))
                snapshot_id = cur.fetchone()[0]
                
                for route_name, metrics in route_metrics.items():
                    cur.execute("""
                        INSERT INTO route_snapshots
                        (snapshot_id, route_name, train_count, avg_speed, max_speed, 
                         delayed_count, on_time_pct, avg_delay, headway_minutes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        snapshot_id, route_name, metrics["count"], metrics["avg_speed"],
                        metrics["max_speed"], metrics["delayed"], metrics["on_time_pct"],
                        metrics["avg_delay"], metrics.get("headway")
                    ))
                
                conn.commit()
                collection_stats["successful_saves"] += 1
                return snapshot_id
    except Exception as e:
        logger.error(f"Save snapshot error: {e}")
        collection_stats["failed_saves"] += 1
        collection_stats["last_error"] = str(e)
        return None

def run_hourly_aggregation():
    """Aggregate raw data into hourly summaries."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO kpi_hourly (
                        hour_start, sample_count, avg_trains, min_trains, max_trains,
                        avg_moving, avg_stopped, avg_delayed, avg_speed, min_speed, max_speed,
                        avg_oee, min_oee, max_oee, avg_on_time_pct
                    )
                    SELECT 
                        date_trunc('hour', timestamp) as hour_start,
                        COUNT(*) as sample_count,
                        AVG(total_trains), MIN(total_trains), MAX(total_trains),
                        AVG(moving), AVG(stopped), AVG(delayed),
                        AVG(avg_speed), MIN(avg_speed), MAX(avg_speed),
                        AVG(oee), MIN(oee), MAX(oee), AVG(on_time_pct)
                    FROM kpi_snapshots
                    WHERE timestamp >= NOW() - INTERVAL '2 hours'
                      AND timestamp < date_trunc('hour', NOW())
                    GROUP BY date_trunc('hour', timestamp)
                    ON CONFLICT (hour_start) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        avg_trains = EXCLUDED.avg_trains,
                        avg_oee = EXCLUDED.avg_oee,
                        avg_on_time_pct = EXCLUDED.avg_on_time_pct
                """)
                
                cur.execute("""
                    INSERT INTO kpi_daily (
                        date, sample_count, avg_trains, min_trains, max_trains,
                        avg_speed, avg_oee, min_oee, max_oee, avg_on_time_pct,
                        peak_hour, peak_trains
                    )
                    SELECT 
                        date_trunc('day', hour_start)::date as date,
                        SUM(sample_count), AVG(avg_trains), MIN(min_trains), MAX(max_trains),
                        AVG(avg_speed), AVG(avg_oee), MIN(min_oee), MAX(max_oee),
                        AVG(avg_on_time_pct), 0, MAX(max_trains)
                    FROM kpi_hourly
                    WHERE hour_start >= NOW() - INTERVAL '2 days'
                      AND hour_start < date_trunc('day', NOW())
                    GROUP BY date_trunc('day', hour_start)::date
                    ON CONFLICT (date) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        avg_oee = EXCLUDED.avg_oee,
                        avg_on_time_pct = EXCLUDED.avg_on_time_pct
                """)
                
                cur.execute("""
                    INSERT INTO route_daily (
                        date, route_name, sample_count, avg_trains, avg_speed,
                        avg_delay, on_time_pct, avg_headway, reliability_score
                    )
                    SELECT 
                        date_trunc('day', k.timestamp)::date as date,
                        r.route_name, COUNT(*), AVG(r.train_count), AVG(r.avg_speed),
                        AVG(r.avg_delay), AVG(r.on_time_pct), AVG(r.headway_minutes),
                        (AVG(r.on_time_pct) * 0.4 + (100 - LEAST(AVG(ABS(r.avg_delay)), 30) * 3.33) * 0.3 + 
                         LEAST(AVG(r.avg_speed) / 60 * 100, 100) * 0.3)
                    FROM route_snapshots r
                    JOIN kpi_snapshots k ON r.snapshot_id = k.id
                    WHERE k.timestamp >= NOW() - INTERVAL '2 days'
                      AND k.timestamp < date_trunc('day', NOW())
                    GROUP BY date_trunc('day', k.timestamp)::date, r.route_name
                    ON CONFLICT (date, route_name) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        on_time_pct = EXCLUDED.on_time_pct,
                        reliability_score = EXCLUDED.reliability_score
                """)
                
                conn.commit()
                collection_stats["last_aggregation"] = datetime.utcnow().isoformat()
                logger.info("Hourly aggregation completed")
    except Exception as e:
        logger.error(f"Aggregation error: {e}")

def cleanup_old_data():
    """Remove raw data older than retention period."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM route_snapshots 
                    WHERE snapshot_id IN (
                        SELECT id FROM kpi_snapshots 
                        WHERE timestamp < NOW() - INTERVAL '%s days'
                    )
                """, (RAW_RETENTION_DAYS,))
                
                cur.execute("""
                    DELETE FROM kpi_snapshots 
                    WHERE timestamp < NOW() - INTERVAL '%s days'
                """, (RAW_RETENTION_DAYS,))
                
                deleted = cur.rowcount
                conn.commit()
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} old snapshots")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

# =============================================================================
# DATA COLLECTION
# =============================================================================

def calculate_route_metrics(trains):
    """Calculate per-route KPIs."""
    routes = defaultdict(lambda: {"trains": [], "delays": [], "speeds": []})
    
    for train in trains:
        route = train.get("routeName", "Unknown")
        routes[route]["trains"].append(train)
        routes[route]["delays"].append(train.get("delay", 0) or 0)
        routes[route]["speeds"].append(train.get("speed", 0) or 0)
    
    route_metrics = {}
    for route_name, data in routes.items():
        trains_list = data["trains"]
        delays = data["delays"]
        speeds = data["speeds"]
        
        delayed_count = sum(1 for d in delays if d > 0)
        on_time_pct = ((len(trains_list) - delayed_count) / len(trains_list) * 100) if trains_list else 100
        headway = 60 / len(trains_list) if len(trains_list) >= 2 else None
        
        route_metrics[route_name] = {
            "count": len(trains_list),
            "avg_speed": round(sum(speeds) / len(speeds), 2) if speeds else 0,
            "max_speed": max(speeds) if speeds else 0,
            "delayed": delayed_count,
            "on_time_pct": round(on_time_pct, 2),
            "avg_delay": round(sum(delays) / len(delays), 2) if delays else 0,
            "headway": round(headway, 2) if headway else None
        }
    
    return route_metrics

def calculate_kpi(trains):
    """Calculate overall KPI metrics."""
    if not trains:
        return None
    
    speeds = [t.get("speed", 0) or 0 for t in trains]
    delays = [t.get("delay", 0) or 0 for t in trains]
    
    avg_speed = sum(speeds) / len(speeds) if speeds else 0
    moving = sum(1 for s in speeds if s > 5)
    delayed = sum(1 for d in delays if d > 0)
    oee = min((avg_speed / 55) * 100, 100) if avg_speed > 0 else 0
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "total_trains": len(trains),
        "moving": moving,
        "stopped": len(trains) - moving,
        "delayed": delayed,
        "avg_speed": round(avg_speed, 2),
        "max_speed": round(max(speeds), 2) if speeds else 0,
        "oee": round(oee, 2),
        "on_time_pct": round(((len(trains) - delayed) / len(trains) * 100), 2) if trains else 100
    }

def fetch_trains():
    """Fetch train data from Amtraker API."""
    try:
        response = requests.get(f"{AMTRAKER_API}/trains", timeout=15)
        if response.status_code != 200:
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
                            except:
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
    """Main data collection loop."""
    time.sleep(5)
    init_database()
    collection_stats["started_at"] = datetime.utcnow().isoformat()
    
    last_aggregation = time.time()
    last_cleanup = time.time()
    
    while True:
        try:
            trains = fetch_trains()
            collection_stats["total_collections"] += 1
            
            if trains:
                kpi = calculate_kpi(trains)
                route_metrics = calculate_route_metrics(trains)
                
                latest_cache["trains"] = trains
                latest_cache["timestamp"] = datetime.utcnow().isoformat()
                latest_cache["kpi"] = kpi
                latest_cache["routes"] = route_metrics
                
                if kpi and DATABASE_URL:
                    snapshot_id = save_snapshot(kpi, trains, route_metrics)
                    if snapshot_id:
                        logger.info(f"Snapshot #{snapshot_id}: {len(trains)} trains, OEE={kpi['oee']:.1f}%, Routes={len(route_metrics)}")
            
            if time.time() - last_aggregation > AGGREGATION_INTERVAL:
                run_hourly_aggregation()
                last_aggregation = time.time()
            
            if time.time() - last_cleanup > 86400:
                cleanup_old_data()
                last_cleanup = time.time()
                
        except Exception as e:
            logger.error(f"Collection error: {e}")
            collection_stats["last_error"] = str(e)
        
        time.sleep(COLLECTION_INTERVAL)

collector_thread = threading.Thread(target=collect_data, daemon=True)
collector_thread.start()

# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "service": "Amtrak Data Collector - Production",
        "version": "4.0",
        "features": ["hourly_rollups", "route_kpis", "smart_retention", "health_monitoring"],
        "database": "connected" if DATABASE_URL else "not configured"
    })

@app.route("/api/health")
def health():
    """Health check endpoint."""
    now = datetime.utcnow()
    last_ts = latest_cache.get("timestamp")
    healthy = False
    age_seconds = None
    
    if last_ts:
        last_update = datetime.fromisoformat(last_ts.replace('Z', ''))
        age_seconds = (now - last_update).total_seconds()
        healthy = age_seconds < 120
    
    return jsonify({
        "healthy": healthy,
        "last_update_seconds_ago": age_seconds,
        "trains_count": len(latest_cache.get("trains", [])),
        "collection_stats": collection_stats
    }), 200 if healthy else 503

@app.route("/api/latest")
def get_latest():
    return jsonify({"trains": latest_cache["trains"], "timestamp": latest_cache["timestamp"]})

@app.route("/api/kpi/latest")
def get_latest_kpi():
    return jsonify(latest_cache["kpi"] or {})

@app.route("/api/routes")
def get_routes():
    return jsonify(latest_cache.get("routes", {}))

@app.route("/api/kpi/snapshots")
def get_snapshots():
    """Get raw KPI snapshots (last 7 days max)."""
    limit = min(request.args.get("limit", 100, type=int), 1000)
    hours = request.args.get("hours", None, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if hours:
                    hours = min(hours, RAW_RETENTION_DAYS * 24)
                    cur.execute("SELECT * FROM kpi_snapshots WHERE timestamp > NOW() - INTERVAL '%s hours' ORDER BY timestamp ASC", (hours,))
                else:
                    cur.execute("SELECT * FROM kpi_snapshots ORDER BY timestamp DESC LIMIT %s", (limit,))
                
                rows = cur.fetchall()
                for r in rows:
                    if r.get("timestamp"):
                        r["timestamp"] = r["timestamp"].isoformat()
                
                return jsonify({"snapshots": rows, "total": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/kpi/hourly")
def get_hourly():
    """Get hourly aggregated data (kept forever)."""
    days = request.args.get("days", 7, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM kpi_hourly WHERE hour_start > NOW() - INTERVAL '%s days' ORDER BY hour_start ASC", (days,))
                rows = cur.fetchall()
                for r in rows:
                    if r.get("hour_start"):
                        r["hour_start"] = r["hour_start"].isoformat()
                return jsonify({"hourly": rows, "total": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/kpi/daily")
def get_daily():
    """Get daily aggregated data (kept forever)."""
    days = request.args.get("days", 60, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM kpi_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date ASC", (days,))
                rows = cur.fetchall()
                for r in rows:
                    if r.get("date"):
                        r["date"] = r["date"].isoformat()
                return jsonify({"daily": rows, "total": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/routes/daily")
def get_routes_daily():
    """Get daily route performance."""
    days = request.args.get("days", 30, type=int)
    route = request.args.get("route", None)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if route:
                    cur.execute("SELECT * FROM route_daily WHERE date > NOW() - INTERVAL '%s days' AND route_name = %s ORDER BY date ASC", (days, route))
                else:
                    cur.execute("SELECT * FROM route_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date ASC, route_name", (days,))
                
                rows = cur.fetchall()
                for r in rows:
                    if r.get("date"):
                        r["date"] = r["date"].isoformat()
                return jsonify({"routes": rows, "total": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/routes/performance")
def get_route_performance():
    """Get route performance rankings."""
    days = request.args.get("days", 7, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT route_name, COUNT(*) as days_active,
                        ROUND(AVG(avg_trains)::numeric, 2) as avg_trains,
                        ROUND(AVG(avg_speed)::numeric, 2) as avg_speed,
                        ROUND(AVG(on_time_pct)::numeric, 2) as on_time_pct,
                        ROUND(AVG(avg_delay)::numeric, 2) as avg_delay,
                        ROUND(AVG(reliability_score)::numeric, 2) as reliability_score
                    FROM route_daily WHERE date > NOW() - INTERVAL '%s days'
                    GROUP BY route_name ORDER BY reliability_score DESC NULLS LAST
                """, (days,))
                return jsonify({"routes": cur.fetchall(), "period_days": days})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/stats")
def get_stats():
    """Get detailed collection statistics."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT COUNT(*) as count, MIN(timestamp) as oldest, MAX(timestamp) as newest FROM kpi_snapshots")
                raw = cur.fetchone()
                cur.execute("SELECT COUNT(*) as count, MIN(hour_start) as oldest FROM kpi_hourly")
                hourly = cur.fetchone()
                cur.execute("SELECT COUNT(*) as count, MIN(date) as oldest FROM kpi_daily")
                daily = cur.fetchone()
                cur.execute("SELECT COUNT(DISTINCT route_name) as count FROM route_daily")
                routes = cur.fetchone()
                
                return jsonify({
                    "raw_snapshots": raw["count"],
                    "raw_oldest": raw["oldest"].isoformat() if raw["oldest"] else None,
                    "raw_newest": raw["newest"].isoformat() if raw["newest"] else None,
                    "hourly_records": hourly["count"],
                    "daily_records": daily["count"],
                    "routes_tracked": routes["count"],
                    "retention_days": RAW_RETENTION_DAYS,
                    "collection_stats": collection_stats
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/export/csv")
def export_csv():
    """Export data as CSV."""
    data_type = request.args.get("type", "daily")
    days = request.args.get("days", 30, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if data_type == "hourly":
                    cur.execute("SELECT * FROM kpi_hourly WHERE hour_start > NOW() - INTERVAL '%s days' ORDER BY hour_start", (days,))
                elif data_type == "routes":
                    cur.execute("SELECT * FROM route_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date, route_name", (days,))
                else:
                    cur.execute("SELECT * FROM kpi_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date", (days,))
                
                rows = cur.fetchall()
                if not rows:
                    return "No data available", 404
                
                headers = list(rows[0].keys())
                lines = [",".join(headers)]
                for r in rows:
                    lines.append(",".join(str(r.get(h, "")) for h in headers))
                
                return "\n".join(lines), 200, {
                    "Content-Type": "text/csv",
                    "Content-Disposition": f"attachment; filename=amtrak_{data_type}_{days}days.csv"
                }
    except Exception as e:
        return str(e), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
