# 🔄 DataFlow: Real-Time ETL Data Pipeline

A production-grade **ETL (Extract, Transform, Load) data pipeline** built with Python that collects, processes, and analyzes real-time weather and air quality data across 8 major Indian cities.

![Python](https://img.shields.io/badge/Python-3.8+-blue?style=flat-square&logo=python)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-green?style=flat-square&logo=pandas)
![SQLite](https://img.shields.io/badge/SQLite-Database-orange?style=flat-square&logo=sqlite)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

## 📋 Overview

DataFlow automates the collection and analysis of environmental data from public APIs, transforming raw JSON responses into structured, queryable datasets stored in a SQLite database with full schema management, deduplication, and pipeline monitoring.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DataFlow ETL Pipeline                     │
├──────────────┬──────────────────┬───────────────────────────┤
│   EXTRACT    │    TRANSFORM     │          LOAD             │
│              │                  │                           │
│ Open-Meteo   │ Data Validation  │ SQLite Database           │
│ Weather API  │ Type Casting     │ ├── weather_data          │
│              │ Null Handling    │ ├── air_quality_data       │
│ Air Quality  │ Enrichment:      │ └── pipeline_runs         │
│ API          │ ├── Heat Index   │                           │
│              │ ├── AQI Category │ Deduplication (MD5 Hash)  │
│ 8 Indian     │ ├── Health Alerts│ Indexed Queries           │
│ Cities       │ └── Analytics    │ Run Monitoring            │
└──────────────┴──────────────────┴───────────────────────────┘
```

## 🚀 Features

- **Multi-Source Extraction**: Parallel data collection from weather and air quality APIs
- **8 Indian Cities**: Delhi, Mumbai, Bengaluru, Kolkata, Chennai, Hyderabad, Patna, Bhubaneswar
- **Data Enrichment**: Heat index classification, AQI categorization (Indian standards), health advisories
- **Deduplication**: MD5-based record hashing prevents duplicate entries
- **Schema Management**: Auto-creates tables with proper indexing
- **Pipeline Monitoring**: Tracks run history, duration, and success/failure status
- **Analytics Engine**: Generates summary reports with city comparisons
- **Raw Data Archival**: Saves raw API responses for audit trails
- **Structured Logging**: Comprehensive logging with file and console output
- **Error Handling**: Graceful failure handling with detailed error reporting

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.8+ |
| Data Processing | Pandas |
| Database | SQLite3 |
| HTTP Client | Requests |
| Data Models | Python Dataclasses |
| APIs | Open-Meteo (Weather + Air Quality) |
| Hashing | MD5 (hashlib) |
| Logging | Python logging module |

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/kumarharsh21112003/dataflow-etl-pipeline.git
cd dataflow-etl-pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## ▶️ Usage

### Run the Pipeline

```bash
python etl_pipeline.py
```

### Sample Output

```
============================================================
[PIPELINE] DataFlow ETL Pipeline started
============================================================
[EXTRACT] Weather data extracted for Delhi
[EXTRACT] Air quality data extracted for Delhi
[EXTRACT] Weather data extracted for Mumbai
...
[TRANSFORM] Weather record transformed for Delhi: 32.5°C, 45% humidity
[TRANSFORM] Air quality record transformed for Delhi: PM2.5=85.3, Category=Moderate
...
[LOAD] Loaded 8/8 weather records (duplicates skipped)
[LOAD] Loaded 8/8 air quality records (duplicates skipped)
============================================================
[PIPELINE] ✅ Pipeline completed successfully!
[PIPELINE] Duration: 4.23 seconds
============================================================

📊 ANALYTICS HIGHLIGHTS
----------------------------------------
🌡️  Avg Temperature: 29.3°C
🔥 Hottest City: Delhi (35.2°C)
❄️  Coolest City: Bengaluru (24.1°C)
💨 Avg Wind Speed: 12.4 km/h

🏭 Worst Air Quality: Delhi (PM2.5: 85.3)
🌿 Best Air Quality: Bhubaneswar (PM2.5: 22.1)
```

## 📁 Project Structure

```
dataflow-etl-pipeline/
├── etl_pipeline.py          # Main ETL pipeline (Extract, Transform, Load)
├── requirements.txt         # Python dependencies
├── .gitignore              # Git ignore rules
├── README.md               # Documentation
├── data/                   # Auto-generated
│   ├── raw/                # Raw API responses (JSON)
│   ├── processed/          # Analytics reports (JSON)
│   └── dataflow.db         # SQLite database
└── logs/
    └── pipeline.log        # Pipeline execution logs
```

## 🗃️ Database Schema

### weather_data
| Column | Type | Description |
|--------|------|-------------|
| city | TEXT | City name |
| temperature_c | REAL | Temperature in Celsius |
| humidity_pct | REAL | Relative humidity % |
| wind_speed_kmh | REAL | Wind speed km/h |
| weather_description | TEXT | WMO weather description |
| heat_index | TEXT | Computed heat index category |
| record_hash | TEXT | MD5 hash for deduplication |

### air_quality_data
| Column | Type | Description |
|--------|------|-------------|
| city | TEXT | City name |
| pm2_5 | REAL | PM2.5 concentration |
| pm10 | REAL | PM10 concentration |
| aqi_category | TEXT | Indian AQI standard category |
| health_advisory | TEXT | Health recommendation |
| record_hash | TEXT | MD5 hash for deduplication |

### pipeline_runs
| Column | Type | Description |
|--------|------|-------------|
| run_time | TEXT | Pipeline execution time |
| status | TEXT | SUCCESS / FAILED |
| duration_seconds | REAL | Execution duration |

## 📊 Key Data Engineering Concepts Demonstrated

1. **ETL Architecture**: Clear separation of Extract, Transform, and Load layers
2. **Data Modeling**: Structured dataclasses for type-safe data handling
3. **Schema Management**: Automated database schema creation with indexing
4. **Data Quality**: Validation, null handling, and type casting
5. **Deduplication**: Hash-based record deduplication strategy
6. **Data Enrichment**: Derived metrics (heat index, AQI category, health advisories)
7. **Pipeline Monitoring**: Execution tracking with success/failure logging
8. **Raw Data Archival**: Preserving source data for lineage and debugging
9. **Idempotent Execution**: Safe to re-run without creating duplicates
10. **Error Resilience**: Graceful handling of API failures and data issues

## 🔮 Future Enhancements

- [ ] Apache Airflow integration for scheduled orchestration
- [ ] PostgreSQL support for production deployment
- [ ] Data visualization dashboard with Streamlit
- [ ] Historical trend analysis and forecasting
- [ ] Slack/Email alerting for pipeline failures
- [ ] Docker containerization

## 👤 Author

**Kumar Harsh**  
B.Tech CSE | KIIT University  
[GitHub](https://github.com/kumarharsh21112003) | [LinkedIn](https://linkedin.com/in/kumar-harsh-99b4982b1) | [Portfolio](https://kumar-harsh-portfolio.vercel.app)

## 📄 License

This project is licensed under the MIT License.
