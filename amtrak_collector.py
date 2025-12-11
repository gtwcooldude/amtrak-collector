"""
Amtrak Real-Time Data Collector for Railway.app
================================================
Uses psycopg v3 (works with Python 3.13)
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import threading

# Database - psycopg v3 (NOT psycopg2)
import psycopg
from psycopg.rows import dict_row

# GTFS-RT parsing
from google.transit import gtfs_realtime_pb2

# Health check server
from flask import Flask, jsonify

# =============================================================================
# CONFIGURATION
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "")
COLLECTION_INTERVAL = 30  # seconds
KPI_INTERVAL = 300  # 5 minutes

# Amtrak GTFS-RT endpoints
VEHICLE_POSITIONS_URL = "https://asm-backend.transitdocs.com/gtfs/amtrak/vehiclePositions"
TRIP_UPDATES_URL = "https://asm-backend.transitdocs.com/gtfs/amtrak/tripUpdates"

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AmtrakCollector")

# =============================================================================
# DATABASE SETUP
# =============================================================================
def get_db_connection():
    """Get database connection."""
    return psycopg.connect(DATABASE_URL)

def init_database():
    """Initialize database tables."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_positions (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            trip_id VARCHAR(50),
            route_id VARCHAR(20),
            vehicle_id VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            bearing DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            current_status VARCHAR(30),
            stop_id VARCHAR(50)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_timestamp ON vehicle_positions(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_trip ON vehicle_positions(trip_id)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trip_updates (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            trip_id VARCHAR(50),
            route_id VARCHAR(20),
            delay_seconds INTEGER,
            stop_id VARCHAR(50)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tu_timestamp ON trip_updates(timestamp)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kpi_snapshots (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            total_trains INTEGER,
            moving_trains INTEGER,
            stopped_trains INTEGER,
            avg_speed_mph DOUBLE PRECISION,
            max_speed_mph DOUBLE PRECISION,
            on_time_count INTEGER,
            delayed_count INTEGER,
            on_time_percentage DOUBLE PRECISION,
            avg_delay_seconds DOUBLE PRECISION,
            availability DOUBLE PRECISION,
            performance DOUBLE PRECISION,
            quality DOUBLE PRECISION,
            oee_score DOUBLE PRECISION
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kpi_timestamp ON kpi_snapshots(timestamp)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_aggregates (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            avg_oee DOUBLE PRECISION,
            avg_on_time_pct DOUBLE PRECISION,
            avg_speed_mph DOUBLE PRECISION,
            total_snapshots INTEGER,
            peak_trains INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_patterns (
            id SERIAL PRIMARY KEY,
            day_of_week INTEGER,
            hour_of_day INTEGER,
            sample_count INTEGER DEFAULT 0,
            avg_trains DOUBLE PRECISION DEFAULT 0,
            avg_speed DOUBLE PRECISION DEFAULT 0,
            avg_on_time_pct DOUBLE PRECISION DEFAULT 0,
            avg_oee DOUBLE PRECISION DEFAULT 0,
            UNIQUE(day_of_week, hour_of_day)
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("✅ Database tables initialized")

# =============================================================================
# DATA FETCHING
# =============================================================================
def fetch_gtfs_feed(url):
    """Fetch GTFS-RT feed."""
    try:
        request = Request(url, headers={'User-Agent': 'AmtrakCollector/1.0'})
        with urlopen(request, timeout=30) as response:
            return response.read()
    except (URLError, HTTPError) as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None

def fetch_vehicle_positions():
    """Fetch and parse vehicle positions."""
    positions = []
    data = fetch_gtfs_feed(VEHICLE_POSITIONS_URL)
    
    if not data:
        return positions
    
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        
        for entity in feed.entity:
            if entity.HasField('vehicle'):
                vp = entity.vehicle
                positions.append({
                    'trip_id': vp.trip.trip_id if vp.HasField('trip') else None,
                    'route_id': vp.trip.route_id if vp.HasField('trip') else None,
                    'vehicle_id': vp.vehicle.id if vp.HasField('vehicle') else None,
                    'latitude': vp.position.latitude if vp.HasField('position') else None,
                    'longitude': vp.position.longitude if vp.HasField('position') else None,
                    'bearing': vp.position.bearing if vp.HasField('position') else None,
                    'speed': vp.position.speed if vp.HasField('position') else None,
                    'current_status': str(vp.current_status) if vp.current_status else None,
                    'stop_id': vp.stop_id if vp.stop_id else None,
                })
        
        logger.info(f"📍 Fetched {len(positions)} vehicle positions")
    except Exception as e:
        logger.error(f"Error parsing vehicle positions: {e}")
    
    return positions

def fetch_trip_updates():
    """Fetch and parse trip updates."""
    updates = []
    data = fetch_gtfs_feed(TRIP_UPDATES_URL)
    
    if not data:
        return updates
    
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                tu = entity.trip_update
                delay = 0
                stop_id = None
                
                if tu.stop_time_update:
                    stu = tu.stop_time_update[0]
                    stop_id = stu.stop_id
                    if stu.HasField('arrival'):
                        delay = stu.arrival.delay
                    elif stu.HasField('departure'):
                        delay = stu.departure.delay
                
                updates.append({
                    'trip_id': tu.trip.trip_id if tu.HasField('trip') else None,
                    'route_id': tu.trip.route_id if tu.HasField('trip') else None,
                    'delay_seconds': delay,
                    'stop_id': stop_id,
                })
        
        logger.info(f"⏱️ Fetched {len(updates)} trip updates")
    except Exception as e:
        logger.error(f"Error parsing trip updates: {e}")
    
    return updates

# =============================================================================
# DATABASE STORAGE
# =============================================================================
def store_vehicle_positions(positions):
    """Store vehicle positions to database."""
    if not positions:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for p in positions:
        cursor.execute("""
            INSERT INTO vehicle_positions 
            (trip_id, route_id, vehicle_id, latitude, longitude, bearing, speed, current_status, stop_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (p['trip_id'], p['route_id'], p['vehicle_id'], p['latitude'], 
              p['longitude'], p['bearing'], p['speed'], p['current_status'], p['stop_id']))
    
    conn.commit()
    conn.close()

def store_trip_updates(updates):
    """Store trip updates to database."""
    if not updates:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for u in updates:
        cursor.execute("""
            INSERT INTO trip_updates (trip_id, route_id, delay_seconds, stop_id)
            VALUES (%s, %s, %s, %s)
        """, (u['trip_id'], u['route_id'], u['delay_seconds'], u['stop_id']))
    
    conn.commit()
    conn.close()

# =============================================================================
# KPI CALCULATION
# =============================================================================
def calculate_and_store_kpis(positions, updates):
    """Calculate KPIs and store snapshot."""
    if not positions:
        return {}
    
    total_trains = len(positions)
    speeds_mph = [(p['speed'] or 0) * 2.237 for p in positions]
    
    avg_speed = sum(speeds_mph) / len(speeds_mph) if speeds_mph else 0
    max_speed = max(speeds_mph) if speeds_mph else 0
    moving = sum(1 for s in speeds_mph if s > 5)
    stopped = total_trains - moving
    
    delays = [u['delay_seconds'] for u in updates] if updates else []
    on_time = sum(1 for d in delays if d < 300) if delays else total_trains
    delayed = len(delays) - on_time if delays else 0
    on_time_pct = (on_time / len(delays) * 100) if delays else 100
    avg_delay = sum(delays) / len(delays) if delays else 0
    
    expected_trains = max(total_trains, 40)
    availability = min(100, (total_trains / expected_trains) * 100)
    performance = min(100, (avg_speed / 45) * 100) if avg_speed > 0 else 100
    quality = on_time_pct
    oee = (availability / 100) * (performance / 100) * (quality / 100) * 100
    
    kpis = {
        'total_trains': total_trains,
        'moving_trains': moving,
        'stopped_trains': stopped,
        'avg_speed_mph': avg_speed,
        'max_speed_mph': max_speed,
        'on_time_count': on_time,
        'delayed_count': delayed,
        'on_time_percentage': on_time_pct,
        'avg_delay_seconds': avg_delay,
        'availability': availability,
        'performance': performance,
        'quality': quality,
        'oee_score': oee
    }
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO kpi_snapshots 
        (total_trains, moving_trains, stopped_trains, avg_speed_mph, max_speed_mph,
         on_time_count, delayed_count, on_time_percentage, avg_delay_seconds,
         availability, performance, quality, oee_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        kpis['total_trains'], kpis['moving_trains'], kpis['stopped_trains'],
        kpis['avg_speed_mph'], kpis['max_speed_mph'], kpis['on_time_count'],
        kpis['delayed_count'], kpis['on_time_percentage'], kpis['avg_delay_seconds'],
        kpis['availability'], kpis['performance'], kpis['quality'], kpis['oee_score']
    ))
    
    now = datetime.now()
    cursor.execute("""
        INSERT INTO hourly_patterns (day_of_week, hour_of_day, sample_count, avg_trains, avg_speed, avg_on_time_pct, avg_oee)
        VALUES (%s, %s, 1, %s, %s, %s, %s)
        ON CONFLICT (day_of_week, hour_of_day) 
        DO UPDATE SET
            sample_count = hourly_patterns.sample_count + 1,
            avg_trains = (hourly_patterns.avg_trains * hourly_patterns.sample_count + EXCLUDED.avg_trains) / (hourly_patterns.sample_count + 1),
            avg_speed = (hourly_patterns.avg_speed * hourly_patterns.sample_count + EXCLUDED.avg_speed) / (hourly_patterns.sample_count + 1),
            avg_on_time_pct = (hourly_patterns.avg_on_time_pct * hourly_patterns.sample_count + EXCLUDED.avg_on_time_pct) / (hourly_patterns.sample_count + 1),
            avg_oee = (hourly_patterns.avg_oee * hourly_patterns.sample_count + EXCLUDED.avg_oee) / (hourly_patterns.sample_count + 1)
    """, (now.weekday(), now.hour, total_trains, avg_speed, on_time_pct, oee))
    
    conn.commit()
    conn.close()
    
    logger.info(f"📊 KPI Snapshot - OEE: {oee:.1f}% | Trains: {total_trains} | On-Time: {on_time_pct:.1f}%")
    
    return kpis

def cleanup_old_data():
    """Remove raw data older than 7 days."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM vehicle_positions WHERE timestamp < NOW() - INTERVAL '7 days'")
    cursor.execute("DELETE FROM trip_updates WHERE timestamp < NOW() - INTERVAL '7 days'")
    conn.commit()
    conn.close()
    logger.info("🧹 Cleaned up old data")

# =============================================================================
# COLLECTION LOOPS
# =============================================================================
last_positions = []
last_updates = []
last_kpis = {}

def collection_loop():
    global last_positions, last_updates
    while True:
        try:
            positions = fetch_vehicle_positions()
            updates = fetch_trip_updates()
            if positions:
                last_positions = positions
                store_vehicle_positions(positions)
            if updates:
                last_updates = updates
                store_trip_updates(updates)
        except Exception as e:
            logger.error(f"Collection error: {e}")
        time.sleep(COLLECTION_INTERVAL)

def kpi_loop():
    global last_kpis
    while True:
        try:
            if last_positions:
                last_kpis = calculate_and_store_kpis(last_positions, last_updates)
        except Exception as e:
            logger.error(f"KPI calculation error: {e}")
        time.sleep(KPI_INTERVAL)

def cleanup_loop():
    while True:
        time.sleep(3600)
        try:
            cleanup_old_data()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# =============================================================================
# HEALTH CHECK SERVER
# =============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        'service': 'Amtrak Data Collector',
        'status': 'running',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'last_kpis': last_kpis,
        'trains_tracked': len(last_positions),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/stats')
def stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        stats = {}
        for table in ['vehicle_positions', 'trip_updates', 'kpi_snapshots']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        conn.close()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)})

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║         🚄 AMTRAK DATA COLLECTOR SERVICE 🚄                ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not set!")
        sys.exit(1)
    
    print(f"✅ Database URL configured")
    print(f"✅ Using psycopg v3 (Python 3.13 compatible)")
    print(f"✅ Collection interval: {COLLECTION_INTERVAL}s")
    print()
    
    print("📦 Initializing database...")
    init_database()
    
    print("🚀 Starting collection threads...")
    for t in [
        threading.Thread(target=collection_loop, daemon=True),
        threading.Thread(target=kpi_loop, daemon=True),
        threading.Thread(target=cleanup_loop, daemon=True),
    ]:
        t.start()
    
    print("✅ All threads started!")
    print("🌐 Starting web server...")
    
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    main()
