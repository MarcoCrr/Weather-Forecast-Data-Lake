import os
import sys
import json
import logging
import argparse
from datetime import datetime, timezone, timedelta
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
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DATA_DIR = os.path.join(PARENT_DATA_DIR, "bronze")
CITIES_DIR = os.path.join(SCRIPT_DIR, "cities_files")
DEFAULT_CITIES_FILE = os.path.join(CITIES_DIR, "cities.txt")


def load_cities_from_file(filepath):
    """
    Reads a text file containing city names, one per line.
    Ignores blank lines and comments starting with '#'.
    """
    if not os.path.exists(filepath):
        logger.error(f"Cities file not found: {filepath}")
        return []

    cities = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                cities.append(stripped)

    logger.info(f"Loaded {len(cities)} cities from {filepath}")
    return cities


def parse_date_string(date_str):
    """
    Parses a string in format 'dd-mm-yyyy' into a UTC datetime object.
    """
    dt = datetime.strptime(date_str, "%d-%m-%Y")
    return dt.replace(tzinfo=timezone.utc)


def parse_date_range(start_date_str, end_date_str):
    """
    Parses start and end date strings into timezone-aware datetime objects covering full days.
    """
    start_dt = parse_date_string(start_date_str)
    end_dt = parse_date_string(end_date_str).replace(hour=23, minute=59, second=59)
    if start_dt > end_dt:
        raise ValueError("Start date must be before or equal to end date.")
    return start_dt, end_dt


def generate_1hour_timestamps(start_dt, end_dt):
    """
    Generates a list of datetime objects spaced by 1-hour intervals within [start_dt, end_dt].
    """
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    timestamps = []
    while current <= end_dt:
        timestamps.append(current)
        current += timedelta(hours=1)
    return timestamps


def get_partitioned_dir(target_dt):
    """
    Constructs the directory path structured as year/month/day/hour.
    Example: data/bronze/2026/07/31/10
    """
    year = target_dt.strftime("%Y")
    month = target_dt.strftime("%m")
    day = target_dt.strftime("%d")
    hour = target_dt.strftime("%H")
    return os.path.join(DATA_DIR, year, month, day, hour)


def save_raw_data(combined_data, target_dt=None):
    """
    Saves the aggregated JSON data for all cities to the local filesystem
    organized by year/month/day/hour structure.
    """
    if target_dt is None:
        target_dt = datetime.now(timezone.utc)

    try:
        partition_dir = get_partitioned_dir(target_dt)
        os.makedirs(partition_dir, exist_ok=True)

        timestamp_str = target_dt.strftime("%Y%m%d_%H%M%S")
        filename = f"weather_data_{timestamp_str}.json"
        filepath = os.path.join(partition_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, indent=4, ensure_ascii=False)

        logger.info(f"Successfully saved raw data for all cities to {filepath}")
    except IOError as e:
        logger.error(f"Failed to save combined data to disk: {e}")


def fetch_weather(cities, api_key):
    """
    Fetches current weather data for a list of cities using the OpenWeather API
    and saves all city responses into a single raw JSON file locally.
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    all_weather_data = {}

    for city in cities:
        logger.info(f"Fetching weather for {city}...")
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            all_weather_data[city] = data

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred for {city}: {http_err}")
            if response and response.status_code == 401:
                logger.error("Please check if your OPENWEATHER_API_KEY is valid.")
        except Exception as err:
            logger.error(f"An error occurred for {city}: {err}")

    if all_weather_data:
        save_raw_data(all_weather_data, datetime.now(timezone.utc))
    else:
        logger.warning("No weather data was successfully fetched for any city.")


def fetch_forecast_weather(cities, api_key, start_dt, end_dt):
    """
    Fetches 5-day forecast weather data from OpenWeather forecast API for a list of cities
    and partitions data into 1-hour intervals saved under year/month/day/hour for every hour.
    """
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    city_forecasts = {}

    for city in cities:
        logger.info(f"Fetching forecast weather for {city}...")
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            forecast_json = response.json()

            items = []
            for item in forecast_json.get("list", []):
                dt_timestamp = item.get("dt")
                if dt_timestamp:
                    item_dt = datetime.fromtimestamp(dt_timestamp, tz=timezone.utc)
                    items.append((item_dt, item))

            if items:
                city_entry = dict(forecast_json.get("city", {}))
                city_forecasts[city] = {
                    "city": city_entry,
                    "items": items
                }

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred for {city}: {http_err}")
            if response and response.status_code == 401:
                logger.error("Please check if your OPENWEATHER_API_KEY is valid.")
        except Exception as err:
            logger.error(f"An error occurred for {city}: {err}")

    if not city_forecasts:
        logger.warning("No weather forecast data found within specified interval.")
        return

    hourly_timestamps = generate_1hour_timestamps(start_dt, end_dt)
    for target_dt in hourly_timestamps:
        hourly_city_data = {}
        for city, data in city_forecasts.items():
            items = data["items"]
            # Find the closest forecast entry for this hourly timestamp
            closest_item_dt, closest_item = min(
                items,
                key=lambda x: abs((x[0] - target_dt).total_seconds())
            )
            hourly_city_data[city] = {
                "city": data["city"],
                "weather": closest_item
            }

        save_raw_data(hourly_city_data, target_dt)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch weather data from OpenWeather API for a list of cities."
    )
    parser.add_argument(
        "cities",
        nargs="*",
        help="Optional list of city names to query."
    )
    parser.add_argument(
        "-f", "--file",
        dest="cities_file",
        default=None,
        help="Path to a text file containing city names (one per line)."
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        default=None,
        help="Start date in dd-mm-yyyy format."
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        default=None,
        help="End date in dd-mm-yyyy format."
    )

    args = parser.parse_args()

    # Retrieve API key from environment variable
    api_key = os.environ.get("OPENWEATHER_API_KEY")

    if not api_key:
        logger.error("OPENWEATHER_API_KEY environment variable not set.")
        logger.error("Please set it using: export OPENWEATHER_API_KEY='your_api_key'")
        sys.exit(1)

    # Determine target cities list based on CLI arguments or default cities file
    if args.cities:
        target_cities = args.cities
    elif args.cities_file:
        target_cities = load_cities_from_file(args.cities_file)
    elif os.path.exists(DEFAULT_CITIES_FILE):
        target_cities = load_cities_from_file(DEFAULT_CITIES_FILE)
    else:
        target_cities = ["London", "New York", "Tokyo"]

    if not target_cities:
        logger.error("No cities were specified or loaded from file.")
        sys.exit(1)

    if args.start_date and args.end_date:
        try:
            start_dt, end_dt = parse_date_range(args.start_date, args.end_date)
            fetch_forecast_weather(target_cities, api_key, start_dt, end_dt)
        except ValueError as err:
            logger.error(f"Invalid date range format or value: {err}")
            sys.exit(1)
    else:
        fetch_weather(target_cities, api_key)


if __name__ == "__main__":
    main()
