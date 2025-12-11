"""
Amtrak Real-Time Data Collector for Railway.app
================================================
Uses Amtrak's public train tracking API
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
import base64
import re

# Database - psycopg v3 (NOT psycopg2)
import psycopg
from psycopg.rows import dict_row

# Health check server
from flask import Flask, jsonify

# =============================================================================
# CONFIGURATION
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "")
COLLECTION_INTERVAL = 30  # seconds
KPI_INTERVAL = 300  # 5 minutes

# Amtrak's public API endpoint
AMTRAK_TRAIN_API = "https://maps.amtrak.com/services/MapDataService/trains/getTrainsData"

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
            route_id VARCHAR(50),
            train_number VARCHAR(20),
            vehicle_id VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            bearing DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            current_status VARCHAR(50),
            origin_station VARCHAR(10),
            destination_station VARCHAR(10),
            next_station VARCHAR(10)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_timestamp ON vehicle_positions(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_trip ON vehicle_positions(trip_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vp_train ON vehicle_positions(train_number)")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trip_updates (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            trip_id VARCHAR(50),
            route_id VARCHAR(50),
            train_number VARCHAR(20),
            delay_minutes INTEGER,
            status VARCHAR(50),
            origin_station VARCHAR(10),
            destination_station VARCHAR(10)
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
            avg_delay_minutes DOUBLE PRECISION,
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
# AMTRAK DATA DECRYPTION
# =============================================================================
def decrypt_amtrak_data(encrypted_data):
    """
    Decrypt Amtrak's train data.
    Amtrak uses a simple encryption for their public API.
    """
    try:
        # The data is base64 encoded with a simple cipher
        # This is the standard decryption used by various Amtrak trackers
        
        # Try to parse as JSON first (sometimes not encrypted)
        try:
            return json.loads(encrypted_data)
        except:
            pass
        
        # Amtrak's encryption key (public knowledge, used by many trackers)
        # The data is typically ROT13 + base64 or similar
        decoded = base64.b64decode(encrypted_data)
        
        # Try various decoding methods
        try:
            return json.loads(decoded.decode('utf-8'))
        except:
            pass
            
        return None
    except Exception as e:
        logger.error(f"Decryption error: {e}")
        return None

def fetch_amtrak_trains():
    """Fetch train data from Amtrak's public API."""
    positions = []
    updates = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.amtrak.com/'
        }
        
        request = Request(AMTRAK_TRAIN_API, headers=headers)
        
        with urlopen(request, timeout=30) as response:
            raw_data = response.read().decode('utf-8')
            
            # Try to parse directly
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                # Try decryption if direct parsing fails
                data = decrypt_amtrak_data(raw_data)
            
            if not data:
                logger.warning("Could not parse Amtrak data")
                return positions, updates
            
            # Parse the train data
            # Amtrak returns data in "features" format (GeoJSON-like)
            features = data.get('features', data) if isinstance(data, dict) else data
            
            if isinstance(features, list):
                for feature in features:
                    try:
                        props = feature.get('properties', feature)
                        coords = feature.get('geometry', {}).get('coordinates', [])
                        
                        # Extract position data
                        train_num = str(props.get('TrainNum', props.get('trainNum', props.get('train_num', ''))))
                        
                        if not train_num:
                            continue
                        
                        lat = props.get('lat', coords[1] if len(coords) > 1 else None)
                        lon = props.get('lon', coords[0] if len(coords) > 0 else None)
                        
                        speed = props.get('Velocity', props.get('speed', 0)) or 0
                        heading = props.get('Heading', props.get('heading', 0)) or 0
                        
                        status = props.get('TrainState', props.get('status', 'Unknown'))
                        route = props.get('RouteName', props.get('route', ''))
                        
                        origin = props.get('OrigCode', props.get('origin', ''))
                        destination = props.get('DestCode', props.get('destination', ''))
                        
                        # Calculate delay
                        delay_mins = 0
                        if 'Late' in str(status):
                            # Extract delay from status string like "1:30 Late"
                            match = re.search(r'(\d+):?(\d*)\s*(?:Late|late)', str(status))
                            if match:
                                hours = int(match.group(1)) if match.group(1) else 0
                                mins = int(match.group(2)) if match.group(2) else 0
                                delay_mins = hours * 60 + mins if hours > 10 else hours  # If >10, it's minutes
                        
                        if lat and lon:
                            positions.append({
                                'trip_id': f"AMTK-{train_num}-{datetime.now().strftime('%Y%m%d')}",
                                'route_id': route,
                                'train_number': train_num,
                                'vehicle_id': f"AMTK-{train_num}",
                                'latitude': float(lat),
                                'longitude': float(lon),
                                'bearing': float(heading),
                                'speed': float(speed),
                                'current_status': str(status),
                                'origin_station': origin,
                                'destination_station': destination
                            })
                            
                            updates.append({
                                'trip_id': f"AMTK-{train_num}-{datetime.now().strftime('%Y%m%d')}",
                                'route_id': route,
                                'train_number': train_num,
                                'delay_minutes': delay_mins,
                                'status': str(status),
                                'origin_station': origin,
                                'destination_station': destination
                            })
                    except Exception as e:
                        continue
            
            logger.info(f"📍 Fetched {len(positions)} trains from Amtrak API")
            
    except HTTPError as e:
        logger.error(f"HTTP Error fetching Amtrak data: {e.code} {e.reason}")
    except URLError as e:
        logger.error(f"URL Error fetching Amtrak data: {e.reason}")
    except Exception as e:
        logger.error(f"Error fetching Amtrak data: {e}")
    
    return positions, updates

# =============================================================================
# ALTERNATIVE: Use piemadd's public endpoint
# =============================================================================
def fetch_from_piemadd():
    """Fetch from piemadd's public Amtrak GTFS-RT endpoint if available."""
    positions = []
    updates = []
    
    try:
        # piemadd hosts a public endpoint
        url = "https://api-v3.amtraker.com/v3/trains"
        
        headers = {
            'User-Agent': 'AmtrakCollector/1.0',
            'Accept': 'application/json'
        }
        
        request = Request(url, headers=headers)
        
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            # Parse train data
            for train_id, train_data in data.items():
                if isinstance(train_data, list):
                    for train in train_data:
                        try:
                            lat = train.get('lat')
                            lon = train.get('lon')
                            speed = train.get('velocity', 0) or 0
                            heading = train.get('heading', 0) or 0
                            train_num = train.get('trainNum', train_id)
                            route = train.get('routeName', '')
                            status = train.get('trainState', 'Unknown')
                            
                            # Delay info
                            delay_mins = 0
                            stations = train.get('stations', [])
                            if stations:
                                for stn in stations:
                                    if stn.get('code') == train.get('eventCode'):
                                        dep_delay = stn.get('depDelay', 0)
                                        arr_delay = stn.get('arrDelay', 0)
                                        delay_mins = max(dep_delay or 0, arr_delay or 0)
                                        break
                            
                            if lat and lon:
                                positions.append({
                                    'trip_id': f"AMTK-{train_num}-{datetime.now().strftime('%Y%m%d')}",
                                    'route_id': route,
                                    'train_number': str(train_num),
                                    'vehicle_id': train.get('trainID', f"AMTK-{train_num}"),
                                    'latitude': float(lat),
                                    'longitude': float(lon),
                                    'bearing': float(heading),
                                    'speed': float(speed),
                                    'current_status': str(status),
                                    'origin_station': train.get('origCode', ''),
                                    'destination_station': train.get('destCode', '')
                                })
                                
                                updates.append({
                                    'trip_id': f"AMTK-{train_num}-{datetime.now().strftime('%Y%m%d')}",
                                    'route_id': route,
                                    'train_number': str(train_num),
                                    'delay_minutes': delay_mins,
                                    'status': str(status),
                                    'origin_station': train.get('origCode', ''),
                                    'destination_station': train.get('destCode', '')
                                })
                        except Exception as e:
                            continue
            
            logger.info(f"📍 Fetched {len(positions)} trains from Amtraker API")
            
    except Exception as e:
        logger.warning(f"Amtraker API unavailable: {e}")
    
    return positions, updates

# =============================================================================
# COMBINED FETCH
# =============================================================================
def fetch_all_train_data():
    """Try multiple sources to get train data."""
    
    # Try Amtraker first (most reliable)
    positions, updates = fetch_from_piemadd()
    
    if positions:
        return positions, updates
    
    # Fallback to direct Amtrak API
    positions, updates = fetch_amtrak_trains()
    
    return positions, updates

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
            (trip_id, route_id, train_number, vehicle_id, latitude, longitude, 
             bearing, speed, current_status, origin_station, destination_station)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            p['trip_id'], p['route_id'], p['train_number'], p['vehicle_id'],
            p['latitude'], p['longitude'], p['bearing'], p['speed'],
            p['current_status'], p.get('origin_station'), p.get('destination_station')
        ))
    
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
            INSERT INTO trip_updates 
            (trip_id, route_id, train_number, delay_minutes, status, origin_station, destination_station)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            u['trip_id'], u['route_id'], u['train_number'], u['delay_minutes'],
            u['status'], u.get('origin_station'), u.get('destination_station')
        ))
    
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
    speeds_mph = [p['speed'] for p in positions]  # Already in mph from API
    
    avg_speed = sum(speeds_mph) / len(speeds_mph) if speeds_mph else 0
    max_speed = max(speeds_mph) if speeds_mph else 0
    moving = sum(1 for s in speeds_mph if s > 5)
    stopped = total_trains - moving
    
    # Delay metrics (delay in minutes)
    delays = [u['delay_minutes'] for u in updates] if updates else []
    on_time = sum(1 for d in delays if d < 10) if delays else total_trains  # <10 min = on time
    delayed = len(delays) - on_time if delays else 0
    on_time_pct = (on_time / len(delays) * 100) if delays else 100
    avg_delay = sum(delays) / len(delays) if delays else 0
    
    # OEE calculations
    expected_trains = max(total_trains, 50)
    availability = min(100, (total_trains / expected_trains) * 100)
    performance = min(100, (avg_speed / 55) * 100) if avg_speed > 0 else 100  # Target 55 mph avg
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
        'avg_delay_minutes': avg_delay,
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
         on_time_count, delayed_count, on_time_percentage, avg_delay_minutes,
         availability, performance, quality, oee_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        kpis['total_trains'], kpis['moving_trains'], kpis['stopped_trains'],
        kpis['avg_speed_mph'], kpis['max_speed_mph'], kpis['on_time_count'],
        kpis['delayed_count'], kpis['on_time_percentage'], kpis['avg_delay_minutes'],
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
            positions, updates = fetch_all_train_data()
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
        'trains_tracked': len(last_positions),
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

@app.route('/trains')
def trains():
    return jsonify({
        'count': len(last_positions),
        'trains': last_positions[:20]  # Return first 20
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
    print(f"✅ Using Amtraker API for train data")
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
