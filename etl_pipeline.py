"""
DataFlow ETL Pipeline
=====================
A production-grade ETL (Extract, Transform, Load) data pipeline that:
- Extracts real-time weather and air quality data from public APIs
- Transforms and cleans raw data with validation and enrichment
- Loads processed data into SQLite database with schema management
- Supports scheduling, logging, and error handling

Author: Kumar Harsh
Tech Stack: Python, Pandas, SQLite, REST APIs, Logging
"""

import os
import json
import sqlite3
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

import requests
import pandas as pd

# ============================================================
# Configuration
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "dataflow.db")
LOG_PATH = os.path.join(BASE_DIR, "logs", "pipeline.log")
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

# Public APIs (no API key required)
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Indian cities with coordinates
CITIES = {
    "Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Patna": {"lat": 25.6093, "lon": 85.1376},
    "Bhubaneswar": {"lat": 20.2961, "lon": 85.8245},
}

# ============================================================
# Setup Logging
# ============================================================

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("DataFlow")


# ============================================================
# Data Models
# ============================================================

@dataclass
class WeatherRecord:
    """Structured weather data record."""
    city: str
    latitude: float
    longitude: float
    timestamp: str
    temperature_c: float
    humidity_pct: float
    wind_speed_kmh: float
    wind_direction_deg: int
    precipitation_mm: float
    weather_code: int
    weather_description: str
    feels_like_c: float
    heat_index: str
    ingestion_time: str
    record_hash: str


@dataclass
class AirQualityRecord:
    """Structured air quality data record."""
    city: str
    latitude: float
    longitude: float
    timestamp: str
    pm2_5: float
    pm10: float
    carbon_monoxide: float
    nitrogen_dioxide: float
    ozone: float
    aqi_category: str
    health_advisory: str
    ingestion_time: str
    record_hash: str


# ============================================================
# EXTRACT Layer
# ============================================================

class Extractor:
    """Handles data extraction from external APIs."""

    WMO_WEATHER_CODES = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
        55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snowfall", 73: "Moderate snowfall", 75: "Heavy snowfall",
        80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
        95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "DataFlow-ETL-Pipeline/1.0"})

    def extract_weather(self, city: str, lat: float, lon: float) -> Optional[Dict]:
        """Extract current weather data for a city."""
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,precipitation,weather_code,apparent_temperature",
                "timezone": "Asia/Kolkata",
            }
            response = self.session.get(WEATHER_API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Save raw data
            raw_path = os.path.join(RAW_DATA_DIR, f"weather_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"[EXTRACT] Weather data extracted for {city}")
            return {"city": city, "lat": lat, "lon": lon, "data": data}

        except requests.RequestException as e:
            logger.error(f"[EXTRACT] Failed to extract weather for {city}: {e}")
            return None

    def extract_air_quality(self, city: str, lat: float, lon: float) -> Optional[Dict]:
        """Extract current air quality data for a city."""
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,ozone",
                "timezone": "Asia/Kolkata",
            }
            response = self.session.get(AIR_QUALITY_API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Save raw data
            raw_path = os.path.join(RAW_DATA_DIR, f"airquality_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_path, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"[EXTRACT] Air quality data extracted for {city}")
            return {"city": city, "lat": lat, "lon": lon, "data": data}

        except requests.RequestException as e:
            logger.error(f"[EXTRACT] Failed to extract air quality for {city}: {e}")
            return None

    def extract_all(self) -> Dict[str, List]:
        """Extract all data from all sources for all cities."""
        logger.info(f"[EXTRACT] Starting extraction for {len(CITIES)} cities...")
        results = {"weather": [], "air_quality": []}

        for city, coords in CITIES.items():
            weather = self.extract_weather(city, coords["lat"], coords["lon"])
            if weather:
                results["weather"].append(weather)

            air_quality = self.extract_air_quality(city, coords["lat"], coords["lon"])
            if air_quality:
                results["air_quality"].append(air_quality)

        logger.info(f"[EXTRACT] Extraction complete: {len(results['weather'])} weather, {len(results['air_quality'])} air quality records")
        return results


# ============================================================
# TRANSFORM Layer
# ============================================================

class Transformer:
    """Handles data transformation, cleaning, and enrichment."""

    @staticmethod
    def _generate_hash(data: Dict) -> str:
        """Generate unique hash for deduplication."""
        raw = json.dumps(data, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    @staticmethod
    def _classify_heat_index(temp: float, humidity: float) -> str:
        """Classify heat index based on temperature and humidity."""
        if temp >= 40:
            return "Extreme"
        elif temp >= 35:
            return "Dangerous" if humidity > 60 else "High"
        elif temp >= 30:
            return "Moderate"
        elif temp >= 20:
            return "Comfortable"
        else:
            return "Cool"

    @staticmethod
    def _classify_aqi(pm2_5: float) -> str:
        """Classify AQI category based on PM2.5 levels (Indian standards)."""
        if pm2_5 <= 30:
            return "Good"
        elif pm2_5 <= 60:
            return "Satisfactory"
        elif pm2_5 <= 90:
            return "Moderate"
        elif pm2_5 <= 120:
            return "Poor"
        elif pm2_5 <= 250:
            return "Very Poor"
        else:
            return "Severe"

    @staticmethod
    def _get_health_advisory(aqi_category: str) -> str:
        """Generate health advisory based on AQI category."""
        advisories = {
            "Good": "Air quality is satisfactory. No health risk.",
            "Satisfactory": "Minor breathing discomfort to sensitive people.",
            "Moderate": "Breathing discomfort to people with lungs/heart disease.",
            "Poor": "Breathing discomfort to most people on prolonged exposure.",
            "Very Poor": "Respiratory illness on prolonged exposure. Limit outdoor activity.",
            "Severe": "Health impact even on light physical work. Avoid outdoor activity.",
        }
        return advisories.get(aqi_category, "No advisory available.")

    def transform_weather(self, raw_data: List[Dict]) -> List[WeatherRecord]:
        """Transform raw weather data into structured records."""
        records = []
        ingestion_time = datetime.now().isoformat()

        for entry in raw_data:
            try:
                city = entry["city"]
                current = entry["data"].get("current", {})

                temp = current.get("temperature_2m", 0.0)
                humidity = current.get("relative_humidity_2m", 0.0)
                weather_code = current.get("weather_code", 0)

                record = WeatherRecord(
                    city=city,
                    latitude=entry["lat"],
                    longitude=entry["lon"],
                    timestamp=current.get("time", ingestion_time),
                    temperature_c=round(temp, 1),
                    humidity_pct=round(humidity, 1),
                    wind_speed_kmh=round(current.get("wind_speed_10m", 0.0), 1),
                    wind_direction_deg=int(current.get("wind_direction_10m", 0)),
                    precipitation_mm=round(current.get("precipitation", 0.0), 2),
                    weather_code=weather_code,
                    weather_description=Extractor.WMO_WEATHER_CODES.get(weather_code, "Unknown"),
                    feels_like_c=round(current.get("apparent_temperature", temp), 1),
                    heat_index=self._classify_heat_index(temp, humidity),
                    ingestion_time=ingestion_time,
                    record_hash=self._generate_hash({"city": city, "time": current.get("time"), "type": "weather"}),
                )
                records.append(record)
                logger.info(f"[TRANSFORM] Weather record transformed for {city}: {temp}°C, {humidity}% humidity")

            except (KeyError, TypeError) as e:
                logger.error(f"[TRANSFORM] Failed to transform weather for {entry.get('city', 'unknown')}: {e}")

        return records

    def transform_air_quality(self, raw_data: List[Dict]) -> List[AirQualityRecord]:
        """Transform raw air quality data into structured records."""
        records = []
        ingestion_time = datetime.now().isoformat()

        for entry in raw_data:
            try:
                city = entry["city"]
                current = entry["data"].get("current", {})
                pm2_5 = current.get("pm2_5", 0.0)
                aqi_category = self._classify_aqi(pm2_5)

                record = AirQualityRecord(
                    city=city,
                    latitude=entry["lat"],
                    longitude=entry["lon"],
                    timestamp=current.get("time", ingestion_time),
                    pm2_5=round(pm2_5, 1),
                    pm10=round(current.get("pm10", 0.0), 1),
                    carbon_monoxide=round(current.get("carbon_monoxide", 0.0), 1),
                    nitrogen_dioxide=round(current.get("nitrogen_dioxide", 0.0), 1),
                    ozone=round(current.get("ozone", 0.0), 1),
                    aqi_category=aqi_category,
                    health_advisory=self._get_health_advisory(aqi_category),
                    ingestion_time=ingestion_time,
                    record_hash=self._generate_hash({"city": city, "time": current.get("time"), "type": "air_quality"}),
                )
                records.append(record)
                logger.info(f"[TRANSFORM] Air quality record transformed for {city}: PM2.5={pm2_5}, Category={aqi_category}")

            except (KeyError, TypeError) as e:
                logger.error(f"[TRANSFORM] Failed to transform air quality for {entry.get('city', 'unknown')}: {e}")

        return records

    def generate_analytics(self, weather_records: List[WeatherRecord], aq_records: List[AirQualityRecord]) -> Dict:
        """Generate analytics summary from processed data."""
        if not weather_records:
            return {}

        weather_df = pd.DataFrame([asdict(r) for r in weather_records])
        aq_df = pd.DataFrame([asdict(r) for r in aq_records]) if aq_records else pd.DataFrame()

        analytics = {
            "pipeline_run_time": datetime.now().isoformat(),
            "cities_processed": len(weather_records),
            "weather_summary": {
                "avg_temperature": round(weather_df["temperature_c"].mean(), 1),
                "max_temperature": {"value": round(weather_df["temperature_c"].max(), 1), "city": weather_df.loc[weather_df["temperature_c"].idxmax(), "city"]},
                "min_temperature": {"value": round(weather_df["temperature_c"].min(), 1), "city": weather_df.loc[weather_df["temperature_c"].idxmin(), "city"]},
                "avg_humidity": round(weather_df["humidity_pct"].mean(), 1),
                "avg_wind_speed": round(weather_df["wind_speed_kmh"].mean(), 1),
                "heat_index_distribution": weather_df["heat_index"].value_counts().to_dict(),
            },
        }

        if not aq_df.empty:
            analytics["air_quality_summary"] = {
                "avg_pm2_5": round(aq_df["pm2_5"].mean(), 1),
                "worst_city": {"city": aq_df.loc[aq_df["pm2_5"].idxmax(), "city"], "pm2_5": round(aq_df["pm2_5"].max(), 1)},
                "best_city": {"city": aq_df.loc[aq_df["pm2_5"].idxmin(), "city"], "pm2_5": round(aq_df["pm2_5"].min(), 1)},
                "aqi_distribution": aq_df["aqi_category"].value_counts().to_dict(),
            }

        # Save analytics report
        report_path = os.path.join(PROCESSED_DATA_DIR, f"analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, "w") as f:
            json.dump(analytics, f, indent=2)

        logger.info(f"[TRANSFORM] Analytics report generated: {report_path}")
        return analytics


# ============================================================
# LOAD Layer
# ============================================================

class Loader:
    """Handles loading processed data into SQLite database."""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self._init_schema()

    def _init_schema(self):
        """Initialize database schema with proper tables."""
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                timestamp TEXT NOT NULL,
                temperature_c REAL,
                humidity_pct REAL,
                wind_speed_kmh REAL,
                wind_direction_deg INTEGER,
                precipitation_mm REAL,
                weather_code INTEGER,
                weather_description TEXT,
                feels_like_c REAL,
                heat_index TEXT,
                ingestion_time TEXT NOT NULL,
                record_hash TEXT UNIQUE NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS air_quality_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                timestamp TEXT NOT NULL,
                pm2_5 REAL,
                pm10 REAL,
                carbon_monoxide REAL,
                nitrogen_dioxide REAL,
                ozone REAL,
                aqi_category TEXT,
                health_advisory TEXT,
                ingestion_time TEXT NOT NULL,
                record_hash TEXT UNIQUE NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time TEXT NOT NULL,
                weather_records_loaded INTEGER,
                air_quality_records_loaded INTEGER,
                status TEXT NOT NULL,
                duration_seconds REAL,
                error_message TEXT
            )
        """)

        # Create indexes for efficient querying
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aq_city ON air_quality_data(city)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_aq_timestamp ON air_quality_data(timestamp)")

        self.conn.commit()
        logger.info("[LOAD] Database schema initialized successfully")

    def load_weather(self, records: List[WeatherRecord]) -> int:
        """Load weather records into database with deduplication."""
        cursor = self.conn.cursor()
        loaded = 0

        for record in records:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO weather_data
                    (city, latitude, longitude, timestamp, temperature_c, humidity_pct,
                     wind_speed_kmh, wind_direction_deg, precipitation_mm, weather_code,
                     weather_description, feels_like_c, heat_index, ingestion_time, record_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.city, record.latitude, record.longitude, record.timestamp,
                    record.temperature_c, record.humidity_pct, record.wind_speed_kmh,
                    record.wind_direction_deg, record.precipitation_mm, record.weather_code,
                    record.weather_description, record.feels_like_c, record.heat_index,
                    record.ingestion_time, record.record_hash,
                ))
                if cursor.rowcount > 0:
                    loaded += 1
            except sqlite3.Error as e:
                logger.error(f"[LOAD] Failed to load weather record for {record.city}: {e}")

        self.conn.commit()
        logger.info(f"[LOAD] Loaded {loaded}/{len(records)} weather records (duplicates skipped)")
        return loaded

    def load_air_quality(self, records: List[AirQualityRecord]) -> int:
        """Load air quality records into database with deduplication."""
        cursor = self.conn.cursor()
        loaded = 0

        for record in records:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO air_quality_data
                    (city, latitude, longitude, timestamp, pm2_5, pm10,
                     carbon_monoxide, nitrogen_dioxide, ozone, aqi_category,
                     health_advisory, ingestion_time, record_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.city, record.latitude, record.longitude, record.timestamp,
                    record.pm2_5, record.pm10, record.carbon_monoxide,
                    record.nitrogen_dioxide, record.ozone, record.aqi_category,
                    record.health_advisory, record.ingestion_time, record.record_hash,
                ))
                if cursor.rowcount > 0:
                    loaded += 1
            except sqlite3.Error as e:
                logger.error(f"[LOAD] Failed to load air quality record for {record.city}: {e}")

        self.conn.commit()
        logger.info(f"[LOAD] Loaded {loaded}/{len(records)} air quality records (duplicates skipped)")
        return loaded

    def log_pipeline_run(self, weather_count: int, aq_count: int, status: str, duration: float, error: str = None):
        """Log pipeline run metadata for monitoring."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_runs (run_time, weather_records_loaded, air_quality_records_loaded, status, duration_seconds, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), weather_count, aq_count, status, round(duration, 2), error))
        self.conn.commit()

    def get_stats(self) -> Dict:
        """Get database statistics."""
        cursor = self.conn.cursor()
        stats = {}
        for table in ["weather_data", "air_quality_data", "pipeline_runs"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        return stats

    def close(self):
        """Close database connection."""
        self.conn.close()


# ============================================================
# Pipeline Orchestrator
# ============================================================

class DataFlowPipeline:
    """Main ETL pipeline orchestrator."""

    def __init__(self):
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()

    def run(self):
        """Execute the full ETL pipeline."""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("[PIPELINE] DataFlow ETL Pipeline started")
        logger.info("=" * 60)

        try:
            # ---- EXTRACT ----
            raw_data = self.extractor.extract_all()

            # ---- TRANSFORM ----
            weather_records = self.transformer.transform_weather(raw_data["weather"])
            aq_records = self.transformer.transform_air_quality(raw_data["air_quality"])

            # Generate analytics report
            analytics = self.transformer.generate_analytics(weather_records, aq_records)

            # ---- LOAD ----
            weather_loaded = self.loader.load_weather(weather_records)
            aq_loaded = self.loader.load_air_quality(aq_records)

            # Log pipeline run
            duration = (datetime.now() - start_time).total_seconds()
            self.loader.log_pipeline_run(weather_loaded, aq_loaded, "SUCCESS", duration)

            # Print summary
            db_stats = self.loader.get_stats()
            logger.info("=" * 60)
            logger.info("[PIPELINE] ✅ Pipeline completed successfully!")
            logger.info(f"[PIPELINE] Duration: {duration:.2f} seconds")
            logger.info(f"[PIPELINE] Weather records loaded: {weather_loaded}")
            logger.info(f"[PIPELINE] Air quality records loaded: {aq_loaded}")
            logger.info(f"[PIPELINE] Total DB records: {db_stats}")
            logger.info("=" * 60)

            # Print analytics highlights
            if analytics:
                print("\n📊 ANALYTICS HIGHLIGHTS")
                print("-" * 40)
                ws = analytics.get("weather_summary", {})
                print(f"🌡️  Avg Temperature: {ws.get('avg_temperature', 'N/A')}°C")
                print(f"🔥 Hottest City: {ws.get('max_temperature', {}).get('city', 'N/A')} ({ws.get('max_temperature', {}).get('value', 'N/A')}°C)")
                print(f"❄️  Coolest City: {ws.get('min_temperature', {}).get('city', 'N/A')} ({ws.get('min_temperature', {}).get('value', 'N/A')}°C)")
                print(f"💨 Avg Wind Speed: {ws.get('avg_wind_speed', 'N/A')} km/h")

                aqs = analytics.get("air_quality_summary", {})
                if aqs:
                    print(f"\n🏭 Worst Air Quality: {aqs.get('worst_city', {}).get('city', 'N/A')} (PM2.5: {aqs.get('worst_city', {}).get('pm2_5', 'N/A')})")
                    print(f"🌿 Best Air Quality: {aqs.get('best_city', {}).get('city', 'N/A')} (PM2.5: {aqs.get('best_city', {}).get('pm2_5', 'N/A')})")

            return True

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.loader.log_pipeline_run(0, 0, "FAILED", duration, str(e))
            logger.error(f"[PIPELINE] ❌ Pipeline failed: {e}")
            return False

        finally:
            self.loader.close()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    pipeline = DataFlowPipeline()
    success = pipeline.run()
    exit(0 if success else 1)
