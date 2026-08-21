# Weather Forecast Data Lake

A modern Data Engineering project that builds a Bronze–Silver data lake from hourly weather forecast data using Python, PySpark, and Docker.

**Status:** Work in Progress: the Bronze and Silver layers are implemented; the Gold analytical layer and orchestration are planned.

---

## Overview

This project implements an end-to-end weather data pipeline inspired by modern data lake architectures.

Every hour, weather forecasts for 31 European cities are collected from the **OpenWeather Forecast API**, stored as raw Bronze data, and transformed into a clean, analytical Silver layer using PySpark.

### Current architecture

```text
OpenWeather API
        │
        ▼
 Bronze Layer (JSON)
        │
        ▼
 Silver Layer (Parquet)
        │
        ▼
 Gold Layer (*planned*)
```

---

## Features

- Hourly ingestion of weather forecasts for 31 European cities
- Bronze data lake with partitioned raw JSON storage
- Silver layer built with PySpark
- Flattening of nested OpenWeather payloads into analytical tables
- Data validation and quality checks
- Hive-style partitioned Parquet datasets
- Fully containerized development environment using Docker
- Configurable command-line ETL scripts

---

## Technologies

| Category | Stack |
|----------|------|
| Language | Python 3.12 |
| Data Processing | PySpark |
| Storage | JSON, Parquet |
| API | OpenWeather Forecast API |
| Containerization | Docker |
| Logging | Python `logging` |

---

## Project structure

```text
.
├── data
│   ├── bronze
│   │   └── YYYY/MM/DD/HH/
│   └── silver
│       └── year=YYYY/month=MM/day=DD/hour=HH/
├── cities_files
│   └── cities.txt
├── ingest_weather.py
├── bronze_to_silver.py
├── Dockerfile
└── README.md
```

---

## Data model

### Bronze layer

The Bronze layer preserves the raw API payload while adding ingestion metadata.

Each hourly JSON file contains one record per city.

Example:

```json
{
  "metadata": {
    "city_name": "Vienna",
    "source": {
      "provider": "OpenWeather",
      "endpoint": "forecast"
    },
    "ingestion_timestamp": "...",
    "forecast_timestamp": "..."
  },
  "city": { ... },
  "payload": { ... }
}
```

### Silver layer

The Silver layer transforms nested JSON into a flat analytical table.

| Column | Description |
|---------|-------------|
| city_name | City |
| country | ISO country code |
| latitude / longitude | Geographic coordinates |
| forecast_timestamp | Forecasted observation time |
| ingestion_timestamp | Pipeline ingestion time |
| temp_celsius | Temperature |
| feels_like_celsius | Apparent temperature |
| humidity_percent | Relative humidity |
| pressure_hpa | Atmospheric pressure |
| wind_speed_m_s | Wind speed |
| wind_direction_deg | Wind direction |
| cloud_cover_percent | Cloud coverage |
| weather_main | Weather category |
| weather_description | Detailed description |

The Silver dataset is stored as **partitioned Parquet** for efficient analytical queries.

---

## Running the project

### Option 1 (recommended): Docker

Build the container:

```bash
docker build -t weather-data-lake .
```

Run it:

```bash
docker run -it \
  -v $(pwd):/workspace \
  weather-data-lake
```

Inside the container:

```bash
python ingest_weather.py --start-date DD-MM-YYYY --end-date DD-MM-YYYY

python bronze_to_silver.py
```

---

### Option 2: Local Python environment

Requirements:

- Python 3.12+
- Java (JDK 17 or compatible)
- PySpark
- Requests

Install dependencies:

```bash
pip install pyspark requests
```

Set your API key:

```bash
export OPENWEATHER_API_KEY="your_api_key"
```

---

## ETL pipeline

### Bronze ingestion

- Reads city list from a text file
- Downloads 5-day forecasts from OpenWeather (going from the current day, and up to 5 days from then)
- Generates hourly sampling
- Stores partitioned JSON files

### Bronze → Silver

- Reads all Bronze partitions recursively
- Flattens nested weather structures
- Validates numerical ranges
- Removes duplicate ingestion records
- Writes Hive-partitioned Parquet

---

## Example analytical use cases (planned Gold layer)

- Daily minimum, maximum, and average temperatures
- Warmest and coldest European cities ranking
- Wind speed and cloud cover comparisons
- Precipitation probability summaries
- Regional climate analysis
- Interactive geographic weather dashboards

---
