"""
Amtrak Real-Time Data Collector v6.0
=====================================
Features:
- Time-of-day analysis (peak/off-peak, weekday/weekend)
- Weather correlation (OpenWeather API)
- Incident annotations
- Station-level metrics
- Hourly/daily rollups
- Route performance tracking
- NEW: Delay Cascade Predictor
- NEW: Track Segment Bottleneck Analysis
"""

import os
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AmtrakCollector")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Configuration
AMTRAKER_API = "https://api-v3.amtraker.com/v3"
COLLECTION_INTERVAL = 30
RAW_RETENTION_DAYS = 7
AGGREGATION_INTERVAL = 3600
WEATHER_INTERVAL = 900  # 15 minutes

DATABASE_URL = os.environ.get("DATABASE_URL")
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")

# Corridor definitions - routes that share track
CORRIDORS = {
    "NEC": {
        "name": "Northeast Corridor",
        "routes": ["Acela", "Northeast Regional", "Vermonter", "Carolinian", "Palmetto", 
                   "Silver Star", "Silver Meteor", "Pennsylvanian", "Keystone"],
        "segments": [
            {"name": "Boston-Providence", "lat_range": (41.5, 42.4), "lon_range": (-71.5, -70.8)},
            {"name": "Providence-New Haven", "lat_range": (41.2, 41.8), "lon_range": (-73.0, -71.4)},
            {"name": "New Haven-NYC", "lat_range": (40.7, 41.3), "lon_range": (-74.0, -72.9)},
            {"name": "NYC-Trenton", "lat_range": (40.2, 40.8), "lon_range": (-74.8, -73.9)},
            {"name": "Trenton-Philadelphia", "lat_range": (39.9, 40.3), "lon_range": (-75.2, -74.7)},
            {"name": "Philadelphia-Wilmington", "lat_range": (39.7, 40.0), "lon_range": (-75.6, -75.1)},
            {"name": "Wilmington-Baltimore", "lat_range": (39.2, 39.8), "lon_range": (-76.7, -75.5)},
            {"name": "Baltimore-DC", "lat_range": (38.8, 39.3), "lon_range": (-77.1, -76.5)},
        ]
    },
    "Empire": {
        "name": "Empire Corridor", 
        "routes": ["Empire Service", "Lake Shore Limited", "Maple Leaf", "Adirondack", "Ethan Allen Express"],
        "segments": [
            {"name": "NYC-Yonkers", "lat_range": (40.7, 41.0), "lon_range": (-74.0, -73.8)},
            {"name": "Yonkers-Poughkeepsie", "lat_range": (41.0, 41.8), "lon_range": (-74.1, -73.7)},
            {"name": "Poughkeepsie-Albany", "lat_range": (41.8, 42.7), "lon_range": (-74.0, -73.6)},
            {"name": "Albany-Syracuse", "lat_range": (42.6, 43.1), "lon_range": (-76.2, -73.7)},
            {"name": "Syracuse-Rochester", "lat_range": (43.0, 43.2), "lon_range": (-77.7, -76.1)},
            {"name": "Rochester-Buffalo", "lat_range": (42.8, 43.2), "lon_range": (-78.9, -77.5)},
        ]
    },
    "California": {
        "name": "California Corridor",
        "routes": ["Pacific Surfliner", "Coast Starlight", "Capitol Corridor", "San Joaquins"],
        "segments": [
            {"name": "San Diego-LA", "lat_range": (32.7, 34.1), "lon_range": (-118.5, -117.1)},
            {"name": "LA-Santa Barbara", "lat_range": (34.0, 34.5), "lon_range": (-120.0, -118.2)},
            {"name": "Santa Barbara-SLO", "lat_range": (34.4, 35.3), "lon_range": (-120.9, -119.7)},
            {"name": "Bay Area", "lat_range": (37.3, 38.0), "lon_range": (-122.5, -121.8)},
            {"name": "Sacramento-Oakland", "lat_range": (37.7, 38.6), "lon_range": (-122.4, -121.4)},
        ]
    },
    "Midwest": {
        "name": "Chicago Hub",
        "routes": ["Lincoln Service", "Illinois Zephyr", "Carl Sandburg", "Hiawatha", 
                   "Pere Marquette", "Wolverine", "Blue Water", "City of New Orleans",
                   "Texas Eagle", "Southwest Chief", "Empire Builder", "California Zephyr"],
        "segments": [
            {"name": "Chicago-Milwaukee", "lat_range": (41.8, 43.1), "lon_range": (-88.0, -87.5)},
            {"name": "Chicago-Springfield", "lat_range": (39.7, 41.9), "lon_range": (-89.8, -87.6)},
            {"name": "Chicago-Detroit", "lat_range": (41.8, 42.4), "lon_range": (-87.7, -83.0)},
        ]
    },
    "Southeast": {
        "name": "Southeast Corridor",
        "routes": ["Carolinian", "Piedmont", "Silver Star", "Silver Meteor", "Palmetto", "Auto Train"],
        "segments": [
            {"name": "DC-Richmond", "lat_range": (37.5, 38.9), "lon_range": (-77.5, -76.9)},
            {"name": "Richmond-Raleigh", "lat_range": (35.7, 37.6), "lon_range": (-79.0, -77.3)},
            {"name": "Raleigh-Charlotte", "lat_range": (35.2, 35.8), "lon_range": (-80.9, -78.6)},
        ]
    }
}

# Key cities for weather (covers major Amtrak corridors)
WEATHER_CITIES = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Washington DC", "lat": 38.9072, "lon": -77.0369},
    {"name": "Boston", "lat": 42.3601, "lon": -71.0589},
]

# In-memory cache
latest_cache = {
    "trains": [],
    "timestamp": None,
    "kpi": None,
    "routes": {},
    "weather": {},
    "stations": {},
    "cascade": {},
    "segments": []
}


# =============================================================================
# CASCADE & SEGMENT ANALYSIS FUNCTIONS
# =============================================================================

def get_train_corridor(route_name):
    """Determine which corridor a train belongs to."""
    for corridor_id, corridor in CORRIDORS.items():
        if any(r.lower() in route_name.lower() for r in corridor["routes"]):
            return corridor_id, corridor
    return None, None


def get_train_segment(lat, lon, corridor_id):
    """Determine which track segment a train is in."""
    if corridor_id not in CORRIDORS:
        return None
    
    for segment in CORRIDORS[corridor_id]["segments"]:
        lat_min, lat_max = segment["lat_range"]
        lon_min, lon_max = segment["lon_range"]
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return segment["name"]
    return None


def analyze_cascade_risk(trains):
    """Analyze delay cascade risks across corridors."""
    corridor_status = {}
    
    for corridor_id, corridor in CORRIDORS.items():
        corridor_trains = []
        for train in trains:
            train_corridor, _ = get_train_corridor(train.get("routeName", ""))
            if train_corridor == corridor_id:
                corridor_trains.append(train)
        
        if not corridor_trains:
            continue
        
        delayed_trains = [t for t in corridor_trains if (t.get("delay") or 0) > 10]
        severely_delayed = [t for t in corridor_trains if (t.get("delay") or 0) > 30]
        
        risk_score = 0
        risk_factors = []
        
        if len(severely_delayed) > 0:
            risk_score += 40
            risk_factors.append(f"{len(severely_delayed)} trains >30min late")
        
        if len(delayed_trains) >= 3:
            risk_score += 30
            risk_factors.append(f"{len(delayed_trains)} trains delayed")
        elif len(delayed_trains) >= 2:
            risk_score += 15
        
        segment_delays = {}
        for train in delayed_trains:
            seg = get_train_segment(train.get("latitude", 0), train.get("longitude", 0), corridor_id)
            if seg:
                segment_delays[seg] = segment_delays.get(seg, 0) + 1
        
        clustered_segments = [s for s, c in segment_delays.items() if c >= 2]
        if clustered_segments:
            risk_score += 20
            risk_factors.append(f"Clustering in: {', '.join(clustered_segments)}")
        
        affected = []
        for train in corridor_trains:
            if train not in delayed_trains and (train.get("delay") or 0) <= 5:
                seg = get_train_segment(train.get("latitude", 0), train.get("longitude", 0), corridor_id)
                if seg in clustered_segments:
                    affected.append({
                        "trainNum": train.get("trainNum"),
                        "route": train.get("routeName"),
                        "risk": "high",
                        "reason": f"Approaching congestion in {seg}"
                    })
        
        corridor_status[corridor_id] = {
            "name": corridor["name"],
            "total_trains": len(corridor_trains),
            "delayed": len(delayed_trains),
            "severely_delayed": len(severely_delayed),
            "risk_score": min(risk_score, 100),
            "risk_level": "critical" if risk_score >= 70 else "high" if risk_score >= 40 else "moderate" if risk_score >= 20 else "low",
            "risk_factors": risk_factors,
            "affected_trains": affected[:5],
            "segment_delays": segment_delays
        }
    
    return corridor_status


def analyze_segments(trains):
    """Analyze speed and performance by track segment."""
    segment_data = {}
    
    for train in trains:
        lat = train.get("latitude", 0)
        lon = train.get("longitude", 0)
        speed = train.get("speed", 0) or 0
        delay = train.get("delay", 0) or 0
        
        corridor_id, _ = get_train_corridor(train.get("routeName", ""))
        if not corridor_id:
            continue
            
        segment = get_train_segment(lat, lon, corridor_id)
        if not segment:
            continue
        
        key = f"{corridor_id}:{segment}"
        if key not in segment_data:
            segment_data[key] = {
                "corridor": corridor_id,
                "segment": segment,
                "speeds": [],
                "delays": [],
                "train_count": 0
            }
        
        segment_data[key]["speeds"].append(speed)
        segment_data[key]["delays"].append(delay)
        segment_data[key]["train_count"] += 1
    
    results = []
    for key, data in segment_data.items():
        if data["speeds"]:
            avg_speed = sum(data["speeds"]) / len(data["speeds"])
            avg_delay = sum(data["delays"]) / len(data["delays"])
            speed_score = min(avg_speed / 60 * 100, 100)
            
            results.append({
                "corridor": data["corridor"],
                "corridor_name": CORRIDORS[data["corridor"]]["name"],
                "segment": data["segment"],
                "train_count": data["train_count"],
                "avg_speed": round(avg_speed, 1),
                "avg_delay": round(avg_delay, 1),
                "speed_score": round(speed_score, 1),
                "is_bottleneck": speed_score < 50 and data["train_count"] >= 2
            })
    
    return sorted(results, key=lambda x: x["speed_score"])

collection_stats = {
    "started_at": None,
    "total_collections": 0,
    "successful_saves": 0,
    "failed_saves": 0,
    "last_error": None,
    "last_aggregation": None,
    "last_weather_fetch": None
}


@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg.connect(DATABASE_URL)
        yield conn
    finally:
        if conn:
            conn.close()


def init_database():
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return False
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Core KPI snapshots
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
                        on_time_pct FLOAT,
                        hour_of_day INTEGER,
                        day_of_week INTEGER,
                        is_weekend BOOLEAN
                    )
                """)
                
                # Add new columns if table exists
                new_cols = [
                    ("moving", "INTEGER"), ("stopped", "INTEGER"), 
                    ("delayed", "INTEGER"), ("avg_speed", "FLOAT"),
                    ("max_speed", "FLOAT"), ("oee", "FLOAT"), 
                    ("on_time_pct", "FLOAT"), ("hour_of_day", "INTEGER"),
                    ("day_of_week", "INTEGER"), ("is_weekend", "BOOLEAN")
                ]
                for col, ctype in new_cols:
                    try:
                        cur.execute(f"ALTER TABLE kpi_snapshots ADD COLUMN IF NOT EXISTS {col} {ctype}")
                    except Exception:
                        pass
                
                # Route snapshots
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
                
                # Station metrics
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS station_snapshots (
                        id SERIAL PRIMARY KEY,
                        snapshot_id INTEGER REFERENCES kpi_snapshots(id) ON DELETE CASCADE,
                        station_name VARCHAR(200),
                        station_type VARCHAR(20),
                        train_count INTEGER,
                        avg_delay FLOAT,
                        max_delay INTEGER,
                        on_time_count INTEGER
                    )
                """)
                
                # Weather data
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_snapshots (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT NOW(),
                        city VARCHAR(50),
                        temp_f FLOAT,
                        feels_like_f FLOAT,
                        humidity INTEGER,
                        wind_mph FLOAT,
                        conditions VARCHAR(100),
                        precipitation FLOAT,
                        snow FLOAT,
                        visibility FLOAT,
                        is_severe BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Incidents/Annotations
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS incidents (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT NOW(),
                        end_timestamp TIMESTAMPTZ,
                        title VARCHAR(200) NOT NULL,
                        description TEXT,
                        severity VARCHAR(20) DEFAULT 'info',
                        affected_routes TEXT[],
                        affected_stations TEXT[],
                        source VARCHAR(100),
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
                
                # Segment snapshots for bottleneck analysis
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS segment_snapshots (
                        id SERIAL PRIMARY KEY,
                        snapshot_id INTEGER REFERENCES kpi_snapshots(id) ON DELETE CASCADE,
                        corridor VARCHAR(50),
                        segment VARCHAR(100),
                        train_count INTEGER,
                        avg_speed FLOAT,
                        avg_delay FLOAT,
                        speed_score FLOAT,
                        is_bottleneck BOOLEAN
                    )
                """)
                
                # Cascade events log
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cascade_events (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMPTZ DEFAULT NOW(),
                        corridor VARCHAR(50),
                        corridor_name VARCHAR(100),
                        risk_level VARCHAR(20),
                        risk_score INTEGER,
                        total_trains INTEGER,
                        delayed_trains INTEGER,
                        affected_trains INTEGER,
                        risk_factors TEXT[]
                    )
                """)
                
                # Segment daily aggregation
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS segment_daily (
                        id SERIAL PRIMARY KEY,
                        date DATE,
                        corridor VARCHAR(50),
                        segment VARCHAR(100),
                        sample_count INTEGER,
                        avg_speed FLOAT,
                        min_speed FLOAT,
                        avg_delay FLOAT,
                        bottleneck_pct FLOAT,
                        UNIQUE(date, corridor, segment)
                    )
                """)
                
                # Hourly aggregations
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kpi_hourly (
                        id SERIAL PRIMARY KEY,
                        hour_start TIMESTAMPTZ UNIQUE,
                        hour_of_day INTEGER,
                        day_of_week INTEGER,
                        is_weekend BOOLEAN,
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
                        avg_on_time_pct FLOAT,
                        avg_temp_f FLOAT,
                        conditions VARCHAR(100)
                    )
                """)
                
                # Add new columns to hourly
                hourly_cols = [("hour_of_day", "INTEGER"), ("day_of_week", "INTEGER"), 
                              ("is_weekend", "BOOLEAN"), ("avg_temp_f", "FLOAT"), ("conditions", "VARCHAR(100)")]
                for col, ctype in hourly_cols:
                    try:
                        cur.execute(f"ALTER TABLE kpi_hourly ADD COLUMN IF NOT EXISTS {col} {ctype}")
                    except Exception:
                        pass
                
                # Daily aggregations
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kpi_daily (
                        id SERIAL PRIMARY KEY,
                        date DATE UNIQUE,
                        day_of_week INTEGER,
                        is_weekend BOOLEAN,
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
                        peak_trains INTEGER,
                        avg_temp_f FLOAT,
                        had_severe_weather BOOLEAN
                    )
                """)
                
                # Add new columns to daily
                daily_cols = [("day_of_week", "INTEGER"), ("is_weekend", "BOOLEAN"),
                             ("avg_temp_f", "FLOAT"), ("had_severe_weather", "BOOLEAN")]
                for col, ctype in daily_cols:
                    try:
                        cur.execute(f"ALTER TABLE kpi_daily ADD COLUMN IF NOT EXISTS {col} {ctype}")
                    except Exception:
                        pass
                
                # Route daily
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
                
                # Station daily
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS station_daily (
                        id SERIAL PRIMARY KEY,
                        date DATE,
                        station_name VARCHAR(200),
                        total_trains INTEGER,
                        avg_delay FLOAT,
                        max_delay INTEGER,
                        on_time_pct FLOAT,
                        delay_score FLOAT,
                        UNIQUE(date, station_name)
                    )
                """)
                
                # Time-of-day patterns (aggregated by hour across all days)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS time_patterns (
                        id SERIAL PRIMARY KEY,
                        hour_of_day INTEGER,
                        day_type VARCHAR(20),
                        sample_days INTEGER,
                        avg_trains FLOAT,
                        avg_oee FLOAT,
                        avg_on_time_pct FLOAT,
                        avg_speed FLOAT,
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(hour_of_day, day_type)
                    )
                """)
                
                # Indexes
                cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_timestamp ON kpi_snapshots(timestamp DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_hour ON kpi_snapshots(hour_of_day)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_dow ON kpi_snapshots(day_of_week)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_route_snapshot ON route_snapshots(snapshot_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_station_snapshot ON station_snapshots(snapshot_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_time ON weather_snapshots(timestamp DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_snapshots(city)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_time ON incidents(timestamp DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_incidents_active ON incidents(is_active)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_hourly_time ON kpi_hourly(hour_start DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON kpi_daily(date DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_route_daily ON route_daily(date DESC, route_name)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_station_daily ON station_daily(date DESC, station_name)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_segment_snap ON segment_snapshots(snapshot_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cascade_time ON cascade_events(timestamp DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_cascade_corridor ON cascade_events(corridor)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_segment_daily ON segment_daily(date DESC, corridor)")
                
                conn.commit()
                logger.info("Database initialized successfully")
                return True
    except Exception as e:
        logger.error(f"Database init error: {e}")
        return False


# =============================================================================
# WEATHER FUNCTIONS
# =============================================================================

def fetch_weather():
    """Fetch weather for key cities."""
    if not OPENWEATHER_API_KEY:
        return {}
    
    weather_data = {}
    for city in WEATHER_CITIES:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                "lat": city["lat"],
                "lon": city["lon"],
                "appid": OPENWEATHER_API_KEY,
                "units": "imperial"
            }
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                weather_data[city["name"]] = {
                    "temp_f": data["main"]["temp"],
                    "feels_like_f": data["main"]["feels_like"],
                    "humidity": data["main"]["humidity"],
                    "wind_mph": data["wind"]["speed"],
                    "conditions": data["weather"][0]["main"] if data.get("weather") else "Unknown",
                    "description": data["weather"][0]["description"] if data.get("weather") else "",
                    "precipitation": data.get("rain", {}).get("1h", 0),
                    "snow": data.get("snow", {}).get("1h", 0),
                    "visibility": data.get("visibility", 10000) / 1609.34,  # meters to miles
                    "is_severe": data["weather"][0]["main"] in ["Thunderstorm", "Snow", "Extreme"] if data.get("weather") else False
                }
        except Exception as e:
            logger.error(f"Weather fetch error for {city['name']}: {e}")
    
    return weather_data


def save_weather(weather_data):
    """Save weather data to database."""
    if not weather_data:
        return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for city, data in weather_data.items():
                    cur.execute("""
                        INSERT INTO weather_snapshots 
                        (city, temp_f, feels_like_f, humidity, wind_mph, conditions, 
                         precipitation, snow, visibility, is_severe)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        city, data["temp_f"], data["feels_like_f"], data["humidity"],
                        data["wind_mph"], data["conditions"], data["precipitation"],
                        data["snow"], data["visibility"], data["is_severe"]
                    ))
                conn.commit()
    except Exception as e:
        logger.error(f"Weather save error: {e}")


# =============================================================================
# DATA COLLECTION
# =============================================================================

def calculate_station_metrics(trains):
    """Calculate metrics by origin/destination station."""
    stations = defaultdict(lambda: {"origins": [], "destinations": [], "delays": []})
    
    for train in trains:
        origin = train.get("origin", "Unknown")
        dest = train.get("destination", "Unknown")
        delay = train.get("delay", 0) or 0
        
        if origin and origin != "Unknown":
            stations[origin]["origins"].append(train)
            stations[origin]["delays"].append(delay)
        if dest and dest != "Unknown":
            stations[dest]["destinations"].append(train)
            stations[dest]["delays"].append(delay)
    
    result = {}
    for station, data in stations.items():
        delays = data["delays"]
        total = len(data["origins"]) + len(data["destinations"])
        if total > 0:
            on_time = sum(1 for d in delays if d <= 0)
            result[station] = {
                "total_trains": total,
                "origins": len(data["origins"]),
                "destinations": len(data["destinations"]),
                "avg_delay": round(sum(delays) / len(delays), 2) if delays else 0,
                "max_delay": max(delays) if delays else 0,
                "on_time_count": on_time,
                "on_time_pct": round(on_time / len(delays) * 100, 2) if delays else 100
            }
    
    return result


def calculate_route_metrics(trains):
    """Calculate per-route KPIs."""
    routes = defaultdict(lambda: {"trains": [], "delays": [], "speeds": []})
    
    for train in trains:
        route = train.get("routeName", "Unknown")
        routes[route]["trains"].append(train)
        routes[route]["delays"].append(train.get("delay", 0) or 0)
        routes[route]["speeds"].append(train.get("speed", 0) or 0)
    
    result = {}
    for route_name, data in routes.items():
        trains_list = data["trains"]
        delays = data["delays"]
        speeds = data["speeds"]
        
        delayed_count = sum(1 for d in delays if d > 0)
        on_time = ((len(trains_list) - delayed_count) / len(trains_list) * 100) if trains_list else 100
        headway = 60 / len(trains_list) if len(trains_list) >= 2 else None
        
        result[route_name] = {
            "count": len(trains_list),
            "avg_speed": round(sum(speeds) / len(speeds), 2) if speeds else 0,
            "max_speed": max(speeds) if speeds else 0,
            "delayed": delayed_count,
            "on_time_pct": round(on_time, 2),
            "avg_delay": round(sum(delays) / len(delays), 2) if delays else 0,
            "headway": round(headway, 2) if headway else None
        }
    
    return result


def calculate_kpi(trains):
    """Calculate overall KPI metrics with time info."""
    if not trains:
        return None
    
    now = datetime.utcnow()
    speeds = [t.get("speed", 0) or 0 for t in trains]
    delays = [t.get("delay", 0) or 0 for t in trains]
    
    avg_speed = sum(speeds) / len(speeds) if speeds else 0
    moving = sum(1 for s in speeds if s > 5)
    delayed = sum(1 for d in delays if d > 0)
    oee = min((avg_speed / 55) * 100, 100) if avg_speed > 0 else 0
    
    return {
        "timestamp": now.isoformat(),
        "total_trains": len(trains),
        "moving": moving,
        "stopped": len(trains) - moving,
        "delayed": delayed,
        "avg_speed": round(avg_speed, 2),
        "max_speed": round(max(speeds), 2) if speeds else 0,
        "oee": round(oee, 2),
        "on_time_pct": round(((len(trains) - delayed) / len(trains) * 100), 2),
        "hour_of_day": now.hour,
        "day_of_week": now.weekday(),
        "is_weekend": now.weekday() >= 5
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


def save_snapshot(kpi, route_metrics, station_metrics):
    """Save all snapshot data."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # KPI snapshot
                cur.execute("""
                    INSERT INTO kpi_snapshots 
                    (total_trains, moving, stopped, delayed, avg_speed, max_speed, 
                     oee, on_time_pct, hour_of_day, day_of_week, is_weekend)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    kpi["total_trains"], kpi["moving"], kpi["stopped"], kpi["delayed"],
                    kpi["avg_speed"], kpi["max_speed"], kpi["oee"], kpi["on_time_pct"],
                    kpi["hour_of_day"], kpi["day_of_week"], kpi["is_weekend"]
                ))
                snapshot_id = cur.fetchone()[0]
                
                # Route snapshots
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
                
                # Station snapshots (top 50 by train count to save space)
                sorted_stations = sorted(station_metrics.items(), 
                                        key=lambda x: x[1]["total_trains"], reverse=True)[:50]
                for station_name, metrics in sorted_stations:
                    cur.execute("""
                        INSERT INTO station_snapshots
                        (snapshot_id, station_name, station_type, train_count, 
                         avg_delay, max_delay, on_time_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        snapshot_id, station_name, "hub",
                        metrics["total_trains"], metrics["avg_delay"],
                        metrics["max_delay"], metrics["on_time_count"]
                    ))
                
                conn.commit()
                collection_stats["successful_saves"] += 1
                return snapshot_id
    except Exception as e:
        logger.error(f"Save error: {e}")
        collection_stats["failed_saves"] += 1
        collection_stats["last_error"] = str(e)
        return None


def save_segment_data(snapshot_id, segments):
    """Save segment analysis data."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for seg in segments:
                    cur.execute("""
                        INSERT INTO segment_snapshots
                        (snapshot_id, corridor, segment, train_count, avg_speed, 
                         avg_delay, speed_score, is_bottleneck)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        snapshot_id, seg["corridor"], seg["segment"], seg["train_count"],
                        seg["avg_speed"], seg["avg_delay"], seg["speed_score"], seg["is_bottleneck"]
                    ))
                conn.commit()
    except Exception as e:
        logger.error(f"Segment save error: {e}")


def save_cascade_event(corridor_id, cascade_data):
    """Save significant cascade events for history."""
    if cascade_data["risk_score"] < 40:  # Only log moderate+ risk
        return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cascade_events
                    (corridor, corridor_name, risk_level, risk_score, total_trains,
                     delayed_trains, affected_trains, risk_factors)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    corridor_id, cascade_data["name"], cascade_data["risk_level"],
                    cascade_data["risk_score"], cascade_data["total_trains"],
                    cascade_data["delayed"], len(cascade_data["affected_trains"]),
                    cascade_data["risk_factors"]
                ))
                conn.commit()
    except Exception as e:
        logger.error(f"Cascade save error: {e}")


def run_hourly_aggregation():
    """Aggregate data into hourly/daily summaries."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Hourly KPI aggregation
                cur.execute("""
                    INSERT INTO kpi_hourly (
                        hour_start, hour_of_day, day_of_week, is_weekend,
                        sample_count, avg_trains, min_trains, max_trains,
                        avg_moving, avg_stopped, avg_delayed, avg_speed, min_speed, max_speed,
                        avg_oee, min_oee, max_oee, avg_on_time_pct
                    )
                    SELECT 
                        date_trunc('hour', timestamp),
                        EXTRACT(hour FROM date_trunc('hour', timestamp))::int,
                        EXTRACT(dow FROM date_trunc('hour', timestamp))::int,
                        EXTRACT(dow FROM date_trunc('hour', timestamp)) IN (0, 6),
                        COUNT(*), AVG(total_trains), MIN(total_trains), MAX(total_trains),
                        AVG(moving), AVG(stopped), AVG(delayed),
                        AVG(avg_speed), MIN(avg_speed), MAX(avg_speed),
                        AVG(oee), MIN(oee), MAX(oee), AVG(on_time_pct)
                    FROM kpi_snapshots
                    WHERE timestamp >= NOW() - INTERVAL '2 hours'
                      AND timestamp < date_trunc('hour', NOW())
                    GROUP BY date_trunc('hour', timestamp)
                    ON CONFLICT (hour_start) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        avg_oee = EXCLUDED.avg_oee,
                        avg_on_time_pct = EXCLUDED.avg_on_time_pct
                """)
                
                # Daily KPI aggregation
                cur.execute("""
                    INSERT INTO kpi_daily (
                        date, day_of_week, is_weekend,
                        sample_count, avg_trains, min_trains, max_trains,
                        avg_speed, avg_oee, min_oee, max_oee, avg_on_time_pct,
                        peak_hour, peak_trains
                    )
                    SELECT 
                        date_trunc('day', hour_start)::date,
                        EXTRACT(dow FROM date_trunc('day', hour_start))::int,
                        EXTRACT(dow FROM date_trunc('day', hour_start)) IN (0, 6),
                        SUM(sample_count), AVG(avg_trains), MIN(min_trains), MAX(max_trains),
                        AVG(avg_speed), AVG(avg_oee), MIN(min_oee), MAX(max_oee),
                        AVG(avg_on_time_pct), 0, MAX(max_trains)
                    FROM kpi_hourly
                    WHERE hour_start >= NOW() - INTERVAL '2 days'
                      AND hour_start < date_trunc('day', NOW())
                    GROUP BY date_trunc('day', hour_start)::date
                    ON CONFLICT (date) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        avg_oee = EXCLUDED.avg_oee
                """)
                
                # Route daily aggregation
                cur.execute("""
                    INSERT INTO route_daily (
                        date, route_name, sample_count, avg_trains, avg_speed,
                        avg_delay, on_time_pct, avg_headway, reliability_score
                    )
                    SELECT 
                        date_trunc('day', k.timestamp)::date,
                        r.route_name, COUNT(*), AVG(r.train_count), AVG(r.avg_speed),
                        AVG(r.avg_delay), AVG(r.on_time_pct), AVG(r.headway_minutes),
                        (AVG(r.on_time_pct) * 0.4 + 
                         (100 - LEAST(AVG(ABS(r.avg_delay)), 30) * 3.33) * 0.3 + 
                         LEAST(AVG(r.avg_speed) / 60 * 100, 100) * 0.3)
                    FROM route_snapshots r
                    JOIN kpi_snapshots k ON r.snapshot_id = k.id
                    WHERE k.timestamp >= NOW() - INTERVAL '2 days'
                      AND k.timestamp < date_trunc('day', NOW())
                    GROUP BY date_trunc('day', k.timestamp)::date, r.route_name
                    ON CONFLICT (date, route_name) DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        reliability_score = EXCLUDED.reliability_score
                """)
                
                # Station daily aggregation
                cur.execute("""
                    INSERT INTO station_daily (
                        date, station_name, total_trains, avg_delay, max_delay, 
                        on_time_pct, delay_score
                    )
                    SELECT 
                        date_trunc('day', k.timestamp)::date,
                        s.station_name, SUM(s.train_count), AVG(s.avg_delay), MAX(s.max_delay),
                        AVG(s.on_time_count::float / NULLIF(s.train_count, 0) * 100),
                        (100 - LEAST(AVG(s.avg_delay), 60) * 1.67)
                    FROM station_snapshots s
                    JOIN kpi_snapshots k ON s.snapshot_id = k.id
                    WHERE k.timestamp >= NOW() - INTERVAL '2 days'
                      AND k.timestamp < date_trunc('day', NOW())
                    GROUP BY date_trunc('day', k.timestamp)::date, s.station_name
                    ON CONFLICT (date, station_name) DO UPDATE SET
                        total_trains = EXCLUDED.total_trains,
                        delay_score = EXCLUDED.delay_score
                """)
                
                # Update time patterns
                cur.execute("""
                    INSERT INTO time_patterns (hour_of_day, day_type, sample_days, 
                                               avg_trains, avg_oee, avg_on_time_pct, avg_speed)
                    SELECT 
                        hour_of_day,
                        CASE WHEN is_weekend THEN 'weekend' ELSE 'weekday' END,
                        COUNT(DISTINCT date_trunc('day', hour_start)),
                        AVG(avg_trains), AVG(avg_oee), AVG(avg_on_time_pct), AVG(avg_speed)
                    FROM kpi_hourly
                    WHERE hour_start > NOW() - INTERVAL '30 days'
                    GROUP BY hour_of_day, is_weekend
                    ON CONFLICT (hour_of_day, day_type) DO UPDATE SET
                        sample_days = EXCLUDED.sample_days,
                        avg_trains = EXCLUDED.avg_trains,
                        avg_oee = EXCLUDED.avg_oee,
                        avg_on_time_pct = EXCLUDED.avg_on_time_pct,
                        avg_speed = EXCLUDED.avg_speed,
                        updated_at = NOW()
                """)
                
                conn.commit()
                collection_stats["last_aggregation"] = datetime.utcnow().isoformat()
                logger.info("Aggregation completed")
    except Exception as e:
        logger.error(f"Aggregation error: {e}")


def cleanup_old_data():
    """Remove raw data older than retention period."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM station_snapshots WHERE snapshot_id IN (SELECT id FROM kpi_snapshots WHERE timestamp < NOW() - INTERVAL '%s days')", (RAW_RETENTION_DAYS,))
                cur.execute("DELETE FROM route_snapshots WHERE snapshot_id IN (SELECT id FROM kpi_snapshots WHERE timestamp < NOW() - INTERVAL '%s days')", (RAW_RETENTION_DAYS,))
                cur.execute("DELETE FROM kpi_snapshots WHERE timestamp < NOW() - INTERVAL '%s days'", (RAW_RETENTION_DAYS,))
                cur.execute("DELETE FROM weather_snapshots WHERE timestamp < NOW() - INTERVAL '30 days'")
                deleted = cur.rowcount
                conn.commit()
                if deleted > 0:
                    logger.info(f"Cleaned {deleted} old records")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")


def collect_data():
    """Main data collection loop."""
    time.sleep(5)
    init_database()
    collection_stats["started_at"] = datetime.utcnow().isoformat()
    
    last_aggregation = time.time()
    last_cleanup = time.time()
    last_weather = 0
    last_cascade_log = 0
    
    while True:
        try:
            trains = fetch_trains()
            collection_stats["total_collections"] += 1
            
            if trains:
                kpi = calculate_kpi(trains)
                route_metrics = calculate_route_metrics(trains)
                station_metrics = calculate_station_metrics(trains)
                cascade_status = analyze_cascade_risk(trains)
                segment_analysis = analyze_segments(trains)
                
                latest_cache["trains"] = trains
                latest_cache["timestamp"] = datetime.utcnow().isoformat()
                latest_cache["kpi"] = kpi
                latest_cache["routes"] = route_metrics
                latest_cache["stations"] = station_metrics
                latest_cache["cascade"] = cascade_status
                latest_cache["segments"] = segment_analysis
                
                if kpi and DATABASE_URL:
                    snapshot_id = save_snapshot(kpi, route_metrics, station_metrics)
                    if snapshot_id:
                        # Save segment data
                        if segment_analysis:
                            save_segment_data(snapshot_id, segment_analysis)
                        
                        # Log cascade events every 5 minutes if significant
                        if time.time() - last_cascade_log > 300:
                            for corridor_id, cdata in cascade_status.items():
                                save_cascade_event(corridor_id, cdata)
                            last_cascade_log = time.time()
                        
                        logger.info(f"#{snapshot_id}: {len(trains)} trains, OEE={kpi['oee']:.1f}%")
            
            # Weather fetch (every 15 min)
            if time.time() - last_weather > WEATHER_INTERVAL:
                weather = fetch_weather()
                if weather:
                    latest_cache["weather"] = weather
                    save_weather(weather)
                    collection_stats["last_weather_fetch"] = datetime.utcnow().isoformat()
                last_weather = time.time()
            
            # Hourly aggregation
            if time.time() - last_aggregation > AGGREGATION_INTERVAL:
                run_hourly_aggregation()
                last_aggregation = time.time()
            
            # Daily cleanup
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
        "service": "Amtrak Collector v6.0",
        "features": ["time_analysis", "weather", "incidents", "stations", "routes", "cascade_predictor", "segment_analysis"],
        "database": "connected" if DATABASE_URL else "not configured",
        "weather_api": "configured" if OPENWEATHER_API_KEY else "not configured"
    })


@app.route("/api/health")
def health():
    now = datetime.utcnow()
    last_ts = latest_cache.get("timestamp")
    healthy = False
    age = None
    
    if last_ts:
        last = datetime.fromisoformat(last_ts.replace("Z", ""))
        age = (now - last).total_seconds()
        healthy = age < 120
    
    return jsonify({
        "healthy": healthy,
        "age_seconds": age,
        "trains": len(latest_cache.get("trains", [])),
        "stats": collection_stats
    }), 200 if healthy else 503


@app.route("/api/latest")
def get_latest():
    return jsonify({
        "trains": latest_cache["trains"],
        "timestamp": latest_cache["timestamp"]
    })


@app.route("/api/kpi/latest")
def get_latest_kpi():
    return jsonify(latest_cache["kpi"] or {})


@app.route("/api/routes")
def get_routes():
    return jsonify(latest_cache.get("routes", {}))


@app.route("/api/stations")
def get_stations():
    return jsonify(latest_cache.get("stations", {}))


@app.route("/api/weather/latest")
def get_weather():
    return jsonify(latest_cache.get("weather", {}))


# Time-of-day analysis
@app.route("/api/analysis/time-of-day")
def get_time_patterns():
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT hour_of_day, day_type, sample_days,
                           ROUND(avg_trains::numeric, 1) as avg_trains,
                           ROUND(avg_oee::numeric, 1) as avg_oee,
                           ROUND(avg_on_time_pct::numeric, 1) as avg_on_time_pct,
                           ROUND(avg_speed::numeric, 1) as avg_speed
                    FROM time_patterns
                    ORDER BY day_type, hour_of_day
                """)
                rows = cur.fetchall()
                
                # Organize by day type
                weekday = [r for r in rows if r["day_type"] == "weekday"]
                weekend = [r for r in rows if r["day_type"] == "weekend"]
                
                return jsonify({
                    "weekday": weekday,
                    "weekend": weekend,
                    "peak_hours": {
                        "weekday_morning": "6-9 AM",
                        "weekday_evening": "4-7 PM",
                        "weekend": "10 AM - 6 PM"
                    }
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analysis/hourly-heatmap")
def get_hourly_heatmap():
    """Get data for hour-by-day heatmap."""
    days = request.args.get("days", 14, type=int)
    metric = request.args.get("metric", "avg_oee")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f"""
                    SELECT 
                        date_trunc('day', hour_start)::date as date,
                        hour_of_day,
                        ROUND({metric}::numeric, 1) as value
                    FROM kpi_hourly
                    WHERE hour_start > NOW() - INTERVAL '%s days'
                    ORDER BY date, hour_of_day
                """, (days,))
                rows = cur.fetchall()
                
                # Convert to heatmap format
                for r in rows:
                    r["date"] = r["date"].isoformat()
                
                return jsonify({"data": rows, "metric": metric})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/analysis/weekday-comparison")
def get_weekday_comparison():
    """Compare metrics by day of week."""
    days = request.args.get("days", 30, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT 
                        day_of_week,
                        CASE day_of_week 
                            WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday'
                            WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
                            WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                            WHEN 6 THEN 'Saturday'
                        END as day_name,
                        COUNT(*) as sample_days,
                        ROUND(AVG(avg_trains)::numeric, 1) as avg_trains,
                        ROUND(AVG(avg_oee)::numeric, 1) as avg_oee,
                        ROUND(AVG(avg_on_time_pct)::numeric, 1) as avg_on_time_pct
                    FROM kpi_daily
                    WHERE date > NOW() - INTERVAL '%s days'
                    GROUP BY day_of_week
                    ORDER BY day_of_week
                """, (days,))
                return jsonify({"days": cur.fetchall()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Weather correlation
@app.route("/api/weather/correlation")
def get_weather_correlation():
    """Analyze weather impact on performance."""
    days = request.args.get("days", 30, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT 
                        w.conditions,
                        COUNT(*) as samples,
                        ROUND(AVG(k.avg_oee)::numeric, 1) as avg_oee,
                        ROUND(AVG(k.on_time_pct)::numeric, 1) as avg_on_time_pct,
                        ROUND(AVG(k.avg_speed)::numeric, 1) as avg_speed,
                        ROUND(AVG(k.delayed)::numeric, 0) as avg_delayed
                    FROM kpi_hourly k
                    JOIN weather_snapshots w ON date_trunc('hour', w.timestamp) = k.hour_start
                    WHERE k.hour_start > NOW() - INTERVAL '%s days'
                    GROUP BY w.conditions
                    HAVING COUNT(*) >= 5
                    ORDER BY avg_oee DESC
                """, (days,))
                return jsonify({"by_conditions": cur.fetchall()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/weather/history")
def get_weather_history():
    """Get weather history."""
    hours = request.args.get("hours", 24, type=int)
    city = request.args.get("city", None)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if city:
                    cur.execute("""
                        SELECT * FROM weather_snapshots 
                        WHERE timestamp > NOW() - INTERVAL '%s hours' AND city = %s
                        ORDER BY timestamp DESC
                    """, (hours, city))
                else:
                    cur.execute("""
                        SELECT * FROM weather_snapshots 
                        WHERE timestamp > NOW() - INTERVAL '%s hours'
                        ORDER BY timestamp DESC
                    """, (hours,))
                
                rows = cur.fetchall()
                for r in rows:
                    r["timestamp"] = r["timestamp"].isoformat()
                return jsonify({"weather": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Incidents
@app.route("/api/incidents", methods=["GET"])
def get_incidents():
    """Get incidents/annotations."""
    active_only = request.args.get("active", "false").lower() == "true"
    days = request.args.get("days", 30, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if active_only:
                    cur.execute("SELECT * FROM incidents WHERE is_active = true ORDER BY timestamp DESC")
                else:
                    cur.execute("""
                        SELECT * FROM incidents 
                        WHERE timestamp > NOW() - INTERVAL '%s days'
                        ORDER BY timestamp DESC
                    """, (days,))
                
                rows = cur.fetchall()
                for r in rows:
                    r["timestamp"] = r["timestamp"].isoformat()
                    if r.get("end_timestamp"):
                        r["end_timestamp"] = r["end_timestamp"].isoformat()
                return jsonify({"incidents": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/incidents", methods=["POST"])
def create_incident():
    """Create a new incident annotation."""
    data = request.json
    if not data or not data.get("title"):
        return jsonify({"error": "Title required"}), 400
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO incidents (title, description, severity, affected_routes, 
                                          affected_stations, source)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data["title"],
                    data.get("description"),
                    data.get("severity", "info"),
                    data.get("affected_routes"),
                    data.get("affected_stations"),
                    data.get("source", "manual")
                ))
                incident_id = cur.fetchone()[0]
                conn.commit()
                return jsonify({"id": incident_id, "status": "created"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/incidents/<int:incident_id>", methods=["PUT"])
def update_incident(incident_id):
    """Update an incident (e.g., mark resolved)."""
    data = request.json
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if data.get("resolve"):
                    cur.execute("""
                        UPDATE incidents SET is_active = false, end_timestamp = NOW()
                        WHERE id = %s
                    """, (incident_id,))
                else:
                    updates = []
                    values = []
                    for field in ["title", "description", "severity"]:
                        if field in data:
                            updates.append(f"{field} = %s")
                            values.append(data[field])
                    if updates:
                        values.append(incident_id)
                        cur.execute(f"UPDATE incidents SET {', '.join(updates)} WHERE id = %s", values)
                
                conn.commit()
                return jsonify({"status": "updated"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Station metrics
@app.route("/api/stations/rankings")
def get_station_rankings():
    """Get station delay rankings."""
    days = request.args.get("days", 7, type=int)
    limit = request.args.get("limit", 20, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Worst stations (most delays)
                cur.execute("""
                    SELECT 
                        station_name,
                        SUM(total_trains) as total_trains,
                        ROUND(AVG(avg_delay)::numeric, 1) as avg_delay,
                        MAX(max_delay) as max_delay,
                        ROUND(AVG(on_time_pct)::numeric, 1) as on_time_pct,
                        ROUND(AVG(delay_score)::numeric, 1) as delay_score
                    FROM station_daily
                    WHERE date > NOW() - INTERVAL '%s days'
                    GROUP BY station_name
                    HAVING SUM(total_trains) >= 10
                    ORDER BY avg_delay DESC
                    LIMIT %s
                """, (days, limit))
                worst = cur.fetchall()
                
                # Best stations
                cur.execute("""
                    SELECT 
                        station_name,
                        SUM(total_trains) as total_trains,
                        ROUND(AVG(avg_delay)::numeric, 1) as avg_delay,
                        ROUND(AVG(on_time_pct)::numeric, 1) as on_time_pct,
                        ROUND(AVG(delay_score)::numeric, 1) as delay_score
                    FROM station_daily
                    WHERE date > NOW() - INTERVAL '%s days'
                    GROUP BY station_name
                    HAVING SUM(total_trains) >= 10
                    ORDER BY on_time_pct DESC
                    LIMIT %s
                """, (days, limit))
                best = cur.fetchall()
                
                return jsonify({"worst_delays": worst, "best_on_time": best})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/daily")
def get_stations_daily():
    """Get daily station metrics."""
    days = request.args.get("days", 7, type=int)
    station = request.args.get("station", None)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if station:
                    cur.execute("""
                        SELECT * FROM station_daily 
                        WHERE date > NOW() - INTERVAL '%s days' AND station_name = %s
                        ORDER BY date
                    """, (days, station))
                else:
                    cur.execute("""
                        SELECT * FROM station_daily 
                        WHERE date > NOW() - INTERVAL '%s days'
                        ORDER BY date, station_name
                    """, (days,))
                
                rows = cur.fetchall()
                for r in rows:
                    r["date"] = r["date"].isoformat()
                return jsonify({"stations": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Existing endpoints
@app.route("/api/kpi/snapshots")
def get_snapshots():
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
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT COUNT(*) as count, MIN(timestamp) as oldest, MAX(timestamp) as newest FROM kpi_snapshots")
                raw = cur.fetchone()
                cur.execute("SELECT COUNT(*) as count FROM kpi_hourly")
                hourly = cur.fetchone()
                cur.execute("SELECT COUNT(*) as count FROM kpi_daily")
                daily = cur.fetchone()
                cur.execute("SELECT COUNT(DISTINCT route_name) as count FROM route_daily")
                routes = cur.fetchone()
                cur.execute("SELECT COUNT(DISTINCT station_name) as count FROM station_daily")
                stations = cur.fetchone()
                cur.execute("SELECT COUNT(*) as count FROM incidents WHERE is_active = true")
                incidents = cur.fetchone()
                
                return jsonify({
                    "raw_snapshots": raw["count"],
                    "raw_oldest": raw["oldest"].isoformat() if raw["oldest"] else None,
                    "raw_newest": raw["newest"].isoformat() if raw["newest"] else None,
                    "hourly_records": hourly["count"],
                    "daily_records": daily["count"],
                    "routes_tracked": routes["count"],
                    "stations_tracked": stations["count"],
                    "active_incidents": incidents["count"],
                    "retention_days": RAW_RETENTION_DAYS,
                    "collection_stats": collection_stats
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/csv")
def export_csv():
    data_type = request.args.get("type", "daily")
    days = request.args.get("days", 30, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if data_type == "hourly":
                    cur.execute("SELECT * FROM kpi_hourly WHERE hour_start > NOW() - INTERVAL '%s days' ORDER BY hour_start", (days,))
                elif data_type == "routes":
                    cur.execute("SELECT * FROM route_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date, route_name", (days,))
                elif data_type == "stations":
                    cur.execute("SELECT * FROM station_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date, station_name", (days,))
                else:
                    cur.execute("SELECT * FROM kpi_daily WHERE date > NOW() - INTERVAL '%s days' ORDER BY date", (days,))
                
                rows = cur.fetchall()
                if not rows:
                    return "No data", 404
                
                headers = list(rows[0].keys())
                lines = [",".join(headers)]
                for r in rows:
                    lines.append(",".join(str(r.get(h, "")) for h in headers))
                
                return "\n".join(lines), 200, {
                    "Content-Type": "text/csv",
                    "Content-Disposition": f"attachment; filename=amtrak_{data_type}_{days}d.csv"
                }
    except Exception as e:
        return str(e), 500


# =============================================================================
# CASCADE & SEGMENT API ENDPOINTS
# =============================================================================

@app.route("/api/cascade/status")
def get_cascade_status():
    """Get current cascade risk for all corridors."""
    cascade = latest_cache.get("cascade", {})
    if not cascade:
        trains = latest_cache.get("trains", [])
        if trains:
            cascade = analyze_cascade_risk(trains)
    
    total_risk = sum(c["risk_score"] for c in cascade.values()) / max(len(cascade), 1) if cascade else 0
    critical_corridors = [c["name"] for c in cascade.values() if c.get("risk_level") == "critical"]
    high_risk = [c["name"] for c in cascade.values() if c.get("risk_level") == "high"]
    
    return jsonify({
        "timestamp": latest_cache.get("timestamp"),
        "network_stress": round(total_risk, 1),
        "network_status": "critical" if total_risk >= 70 else "stressed" if total_risk >= 40 else "normal",
        "critical_corridors": critical_corridors,
        "high_risk_corridors": high_risk,
        "corridors": cascade
    })


@app.route("/api/cascade/history")
def get_cascade_history():
    """Get cascade event history."""
    hours = request.args.get("hours", 24, type=int)
    corridor = request.args.get("corridor", None)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if corridor:
                    cur.execute("""
                        SELECT * FROM cascade_events 
                        WHERE timestamp > NOW() - INTERVAL '%s hours' AND corridor = %s
                        ORDER BY timestamp DESC
                    """, (hours, corridor))
                else:
                    cur.execute("""
                        SELECT * FROM cascade_events 
                        WHERE timestamp > NOW() - INTERVAL '%s hours'
                        ORDER BY timestamp DESC
                    """, (hours,))
                
                rows = cur.fetchall()
                for r in rows:
                    r["timestamp"] = r["timestamp"].isoformat()
                return jsonify({"events": rows, "total": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/segments/current")
def get_segments_current():
    """Get current segment analysis."""
    segments = latest_cache.get("segments", [])
    if not segments:
        trains = latest_cache.get("trains", [])
        if trains:
            segments = analyze_segments(trains)
    
    bottlenecks = [s for s in segments if s.get("is_bottleneck")]
    
    return jsonify({
        "timestamp": latest_cache.get("timestamp"),
        "segments": segments,
        "bottlenecks": bottlenecks,
        "bottleneck_count": len(bottlenecks)
    })


@app.route("/api/segments/rankings")
def get_segment_rankings():
    """Get segment performance rankings over time."""
    days = request.args.get("days", 7, type=int)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT 
                        corridor, segment,
                        COUNT(*) as sample_count,
                        ROUND(AVG(avg_speed)::numeric, 1) as avg_speed,
                        ROUND(MIN(avg_speed)::numeric, 1) as min_speed,
                        ROUND(AVG(avg_delay)::numeric, 1) as avg_delay,
                        ROUND(AVG(speed_score)::numeric, 1) as avg_score,
                        ROUND(AVG(CASE WHEN is_bottleneck THEN 100 ELSE 0 END)::numeric, 1) as bottleneck_pct
                    FROM segment_snapshots s
                    JOIN kpi_snapshots k ON s.snapshot_id = k.id
                    WHERE k.timestamp > NOW() - INTERVAL '%s days'
                    GROUP BY corridor, segment
                    ORDER BY avg_speed ASC
                """, (days,))
                
                rows = cur.fetchall()
                
                for r in rows:
                    r["corridor_name"] = CORRIDORS.get(r["corridor"], {}).get("name", r["corridor"])
                
                worst = [r for r in rows if r["avg_speed"] and r["avg_speed"] < 40][:10]
                best = sorted([r for r in rows if r["avg_speed"]], key=lambda x: x["avg_speed"], reverse=True)[:10]
                
                return jsonify({
                    "all_segments": rows,
                    "worst_bottlenecks": worst,
                    "best_performing": best,
                    "period_days": days
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/segments/corridor/<corridor_id>")
def get_corridor_segments(corridor_id):
    """Get segment details for a specific corridor."""
    days = request.args.get("days", 7, type=int)
    
    if corridor_id not in CORRIDORS:
        return jsonify({"error": "Unknown corridor"}), 404
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                    SELECT 
                        segment,
                        date_trunc('hour', k.timestamp) as hour,
                        COUNT(*) as samples,
                        ROUND(AVG(avg_speed)::numeric, 1) as avg_speed,
                        ROUND(AVG(avg_delay)::numeric, 1) as avg_delay
                    FROM segment_snapshots s
                    JOIN kpi_snapshots k ON s.snapshot_id = k.id
                    WHERE corridor = %s AND k.timestamp > NOW() - INTERVAL '%s days'
                    GROUP BY segment, date_trunc('hour', k.timestamp)
                    ORDER BY segment, hour
                """, (corridor_id, days))
                
                rows = cur.fetchall()
                for r in rows:
                    r["hour"] = r["hour"].isoformat()
                
                return jsonify({
                    "corridor": corridor_id,
                    "corridor_name": CORRIDORS[corridor_id]["name"],
                    "segments": CORRIDORS[corridor_id]["segments"],
                    "data": rows
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/corridors")
def get_corridors():
    """Get list of all corridors and their segments."""
    result = {}
    for cid, cdata in CORRIDORS.items():
        result[cid] = {
            "name": cdata["name"],
            "routes": cdata["routes"],
            "segment_count": len(cdata["segments"]),
            "segments": [s["name"] for s in cdata["segments"]]
        }
    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
