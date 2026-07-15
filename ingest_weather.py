import os
import sys
import json
import logging
from datetime import datetime
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = os.path.join("data", "raw")

def save_raw_data(city, data):
    """
    Saves the raw JSON data to the local filesystem under data/raw/
    with a timestamped filename.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Generate a safe filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_city_name = city.lower().replace(" ", "_")
        filename = f"{safe_city_name}_{timestamp}.json"
        filepath = os.path.join(DATA_DIR, filename)
        
        # Write JSON data to file
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Successfully saved raw data for {city} to {filepath}")
    except IOError as e:
        logger.error(f"Failed to save data for {city} to disk: {e}")

def fetch_weather(cities, api_key):
    """
    Fetches current weather data for a list of cities using the OpenWeather API
    and saves the raw JSON response locally.
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    for city in cities:
        logger.info(f"Fetching weather for {city}...")
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_