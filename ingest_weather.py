import os
import sys
import json
import logging
import argparse
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
# Resolves the directory relative to this script's location to keep all data inside the project folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DATA_DIR = os.path.join(PARENT_DATA_DIR, "bronze")
DEFAULT_CITIES_FILE = os.path.join(SCRIPT_DIR, "cities.txt")


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


def save_raw_data(combined_data):
    """
    Saves the aggregated JSON data for all cities to the local filesystem
    under the project folder with a single timestamped filename.
    """
    try:
        # Ensure the output directory exists (this will also create PARENT_DATA_DIR if not present)
        os.makedirs(DATA_DIR, exist_ok=True)

        # Generate a safe filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_data_{timestamp}.json"
        filepath = os.path.join(DATA_DIR, filename)

        # Write combined JSON data to file
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
        save_raw_data(all_weather_data)
    else:
        logger.warning("No weather data was successfully fetched for any city.")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch current weather data from OpenWeather API for a list of cities."
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

    fetch_weather(target_cities, api_key)


if __name__ == "__main__":
    main()
