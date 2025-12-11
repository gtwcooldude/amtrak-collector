"""
Amtrak Real-Time Data Collector for Railway.app
================================================
Uses Amtraker API (api-v3.amtraker.com)
"""

import os
import sys
import json
import time
import logging
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import threading

# Database - psycopg v3
import psycopg

# Health check server
from flask import Flask, jsonify

# =============================================================================
# CONFIGURATION
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "")
COLLECTION_INTERVAL = 30
KPI_INTERVAL = 300

# Heading to degrees mapping
HEADING_MAP = {
    'N': 0, 'NE': 45, 'E': 90, 'SE': 135,
    'S': 180, 'SW': 225, 'W': 270, 'NW': 315
}

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
# DATABASE
# =============================================================================
def get_db_connection():
    return psycopg.connect(DATABASE_URL)

def init_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop old tables
    for table in ['vehicle_positions', 'trip_updates', 'kpi_snapshots', 'hourly_patterns']:
        cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    
    cursor.execute("""
        CREATE TABLE vehicle_positions (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            trip_id VARCHAR(50),
            route_name VARCHAR(100),
            train_number VARCHAR(20),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            bearing DOUBLE PRECISION,
            speed_mph DOUBLE PRECISION,
            train_state VARCHAR(50),
            origin_code VARCHAR(10),
            dest_code VARCHAR(10)
        )
    """)
    cursor.execute("CREATE INDEX idx_vp_ts ON vehicle_positions(timestamp)")
    
    cursor.execute("""
        CREATE TABLE trip_updates (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            trip_id VARCHAR(50),
            route_name VARCHAR(100),
            train_number VARCHAR(20),
            delay_minutes INTEGER,
            train_state VARCHAR(50)
        )
    """)
    cursor.execute("CREATE INDEX idx_tu_ts ON trip_updates(timestamp)")
    
    cursor.execute("""
        CREATE TABLE kpi_snapshots (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            total_trains INTEGER,
            moving_trains INTEGER,
            stopped_trains INTEGER,
            avg_speed_mph DOUBLE PRECISION,
            max_speed_mph DOUBLE PRECISION,
            on_time_count INTEGER,
            delayed_count INTEGER,
            on_time_pct DOUBLE PRECISION,
            avg_delay_min DOUBLE PRECISION,
            availability DOUBLE PRECISION,
            performance DOUBLE PRECISION,
            quality DOUBLE PRECISION,
            oee DOUBLE PRECISION
        )
    """)
    cursor.execute("CREATE INDEX idx_kpi_ts ON kpi_snapshots(timestamp)")
    
    cursor.execute("""
        CREATE TABLE hourly_patterns (
            id SERIAL PRIMARY KEY,
            day_of_week INTEGER,
            hour_of_day INTEGER,
            samples INTEGER DEFAULT 0,
            avg_trains DOUBLE PRECISION DEFAULT 0,
            avg_speed DOUBLE PRECISION DEFAULT 0,
            avg_on_time DOUBLE PRECISION DEFAULT 0,
            avg_oee DOUBLE PRECISION DEFAULT 0,
            UNIQUE(day_of_week, hour_of_day)
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("✅ Database tables initialized")

# =============================================================================
# FETCH TRAINS FROM AMTRAKER API
# =============================================================================
def fetch_trains():
    positions = []
    updates = []
    
    try:
        url = "https://api-v3.amtraker.com/v3/trains"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        request = Request(url, headers=headers)
        
        with urlopen(request, timeout=30) as response:
            raw = response.read().decode('utf-8')
            logger.info(f"📥 API response length: {len(raw)} bytes")
            
            if not raw or raw == '{}':
                logger.warning("⚠️ Empty response from API")
                return positions, updates
            
            data = json.loads(raw)
            
            if not data:
                logger.warning("⚠️ No data in response")
                return positions, updates
            
            train_count = 0
            for train_num, train_list in data.items():
                if not isinstance(train_list, list):
                    continue
                
                for train in train_list:
                    try:
                        lat = train.get('lat')
                        lon = train.get('lon')
                        
                        if lat is None or lon is None:
                            continue
                        
                        # Get speed (velocity is in MPH)
                        speed = float(train.get('velocity', 0) or 0)
                        
                        # Convert heading string to degrees
                        heading_str = train.get('heading', 'N')
                        bearing = HEADING_MAP.get(heading_str, 0)
                        
                        train_number = str(train.get('trainNum', train_num))
                        route = train.get('routeName', '')
                        state = train.get('trainState', 'Unknown')
                        orig = train.get('origCode', '')
                        dest = train.get('destCode', '')
                        train_id = train.get('trainID', f"{train_number}-{datetime.now().day}")
                        
                        # Calculate delay from stations
                        delay_mins = 0
                        stations = train.get('stations', [])
                        for stn in stations:
                            if stn.get('code') == train.get('eventCode'):
                                # Check arrival and departure delays
                                arr_d = stn.get('arrDelay')
                                dep_d = stn.get('depDelay')
                                if arr_d is not None and arr_d > delay_mins:
                                    delay_mins = arr_d
                                if dep_d is not None and dep_d > delay_mins:
                                    delay_mins = dep_d
                                break
                        
                        positions.append({
                            'trip_id': train_id,
                            'route_name': route,
                            'train_number': train_number,
                            'latitude': float(lat),
                            'longitude': float(lon),
                            'bearing': bearing,
                            'speed_mph': speed,
                            'train_state': state,
                            'origin_code': orig,
                            'dest_code': dest
                        })
                        
                        updates.append({
                            'trip_id': train_id,
                            'route_name': route,
                            'train_number': train_number,
                            'delay_minutes': delay_mins,
                            'train_state': state
                        })
                        
                        train_count += 1
                        
                    except Exception as e:
                        logger.debug(f"Error parsing train: {e}")
                        continue
            
            logger.info(f"📍 Fetched {train_count} trains from Amtraker")
            
    except HTTPError as e:
        logger.error(f"HTTP Error: {e.code} {e.reason}")
    except URLError as e:
        logger.error(f"URL Error: {e.reason}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
    except Exception as e:
        logger.error(f"Fetch error: {e}")
    
    return positions, updates

# =============================================================================
# DATABASE STORAGE
# =============================================================================
def store_positions(positions):
    if not positions:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    for p in positions:
        cursor.execute("""
            INSERT INTO vehicle_positions 
            (trip_id, route_name, train_number, latitude, longitude, bearing, speed_mph, train_state, origin_code, dest_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (p['trip_id'], p['route_name'], p['train_number'], p['latitude'], 
              p['longitude'], p['bearing'], p['speed_mph'], p['train_state'],
              p['origin_code'], p['dest_code']))
    conn.commit()
    conn.close()
    logger.info(f"💾 Stored {len(positions)} positions")

def store_updates(updates):
    if not updates:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    for u in updates:
        cursor.execute("""
            INSERT INTO trip_updates (trip_id, route_name, train_number, delay_minutes, train_state)
            VALUES (%s, %s, %s, %s, %s)
        """, (u['trip_id'], u['route_name'], u['train_number'], u['delay_minutes'], u['train_state']))
    conn.commit()
    conn.close()

# =============================================================================
# KPI CALCULATION
# =============================================================================
def calculate_kpis(positions, updates):
    if not positions:
        return {}
    
    total = len(positions)
    speeds = [p['speed_mph'] for p in positions]
    avg_speed = sum(speeds) / len(speeds) if speeds else 0
    max_speed = max(speeds) if speeds else 0
    moving = sum(1 for s in speeds if s > 5)
    stopped = total - moving
    
    delays = [u['delay_minutes'] for u in updates] if updates else []
    on_time = sum(1 for d in delays if d < 10) if delays else total
    delayed = len(delays) - on_time if delays else 0
    on_time_pct = (on_time / len(delays) * 100) if delays else 100
    avg_delay = sum(delays) / len(delays) if delays else 0
    
    expected = max(total, 50)
    availability = min(100, (total / expected) * 100)
    performance = min(100, (avg_speed / 55) * 100) if avg_speed > 0 else 100
    quality = on_time_pct
    oee = (availability / 100) * (performance / 100) * (quality / 100) * 100
    
    kpis = {
        'total_trains': total, 'moving_trains': moving, 'stopped_trains': stopped,
        'avg_speed_mph': avg_speed, 'max_speed_mph': max_speed,
        'on_time_count': on_time, 'delayed_count': delayed,
        'on_time_pct': on_time_pct, 'avg_delay_min': avg_delay,
        'availability': availability, 'performance': performance,
        'quality': quality, 'oee': oee
    }
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO kpi_snapshots 
        (total_trains, moving_trains, stopped_trains, avg_speed_mph, max_speed_mph,
         on_time_count, delayed_count, on_time_pct, avg_delay_min,
         availability, performance, quality, oee)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (total, moving, stopped, avg_speed, max_speed, on_time, delayed,
          on_time_pct, avg_delay, availability, performance, quality, oee))
    
    now = datetime.now()
    cursor.execute("""
        INSERT INTO hourly_patterns (day_of_week, hour_of_day, samples, avg_trains, avg_speed, avg_on_time, avg_oee)
        VALUES (%s, %s, 1, %s, %s, %s, %s)
        ON CONFLICT (day_of_week, hour_of_day) 
        DO UPDATE SET
            samples = hourly_patterns.samples + 1,
            avg_trains = (hourly_patterns.avg_trains * hourly_patterns.samples + EXCLUDED.avg_trains) / (hourly_patterns.samples + 1),
            avg_speed = (hourly_patterns.avg_speed * hourly_patterns.samples + EXCLUDED.avg_speed) / (hourly_patterns.samples + 1),
            avg_on_time = (hourly_patterns.avg_on_time * hourly_patterns.samples + EXCLUDED.avg_on_time) / (hourly_patterns.samples + 1),
            avg_oee = (hourly_patterns.avg_oee * hourly_patterns.samples + EXCLUDED.avg_oee) / (hourly_patterns.samples + 1)
    """, (now.weekday(), now.hour, total, avg_speed, on_time_pct, oee))
    
    conn.commit()
    conn.close()
    
    logger.info(f"📊 KPI: OEE={oee:.1f}% | Trains={total} | OnTime={on_time_pct:.1f}% | AvgSpeed={avg_speed:.1f}mph")
    return kpis

def cleanup_old_data():
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
            positions, updates = fetch_trains()
            if positions:
                last_positions = positions
                store_positions(positions)
            if updates:
                last_updates = updates
                store_updates(updates)
        except Exception as e:
            logger.error(f"Collection error: {e}")
        time.sleep(COLLECTION_INTERVAL)

def kpi_loop():
    global last_kpis
    while True:
        try:
            if last_positions:
                last_kpis = calculate_kpis(last_positions, last_updates)
        except Exception as e:
            logger.error(f"KPI error: {e}")
        time.sleep(KPI_INTERVAL)

def cleanup_loop():
    while True:
        time.sleep(3600)
        try:
            cleanup_old_data()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# =============================================================================
# FLASK SERVER
# =============================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        'service': 'Amtrak Data Collector',
        'status': 'running',
        'trains': len(last_positions),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'kpis': last_kpis,
        'trains': len(last_positions)
    })

@app.route('/trains')
def trains():
    return jsonify({'count': len(last_positions), 'data': last_positions[:20]})

@app.route('/stats')
def stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        result = {}
        for table in ['vehicle_positions', 'trip_updates', 'kpi_snapshots']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            result[table] = cursor.fetchone()[0]
        conn.close()
        return jsonify(result)
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
    
    print("✅ Database URL configured")
    print("✅ Using Amtraker API (api-v3.amtraker.com)")
    print(f"✅ Collection interval: {COLLECTION_INTERVAL}s")
    print()
    
    print("📦 Initializing database (fresh tables)...")
    init_database()
    
    print("🚀 Starting threads...")
    threading.Thread(target=collection_loop, daemon=True).start()
    threading.Thread(target=kpi_loop, daemon=True).start()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    
    print("✅ All threads started!")
    print("🌐 Starting web server...")
    
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    main()
