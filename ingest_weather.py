import os
import sys
import requests

def fetch_weather(cities, api_key):
    """
    Fetches current weather data for a list of cities using the OpenWeather API.
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    for city in cities:
        print(f"Fetching weather for {city}...")
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'  # Use metric units (Celsius). Change to 'imperial' for Fahrenheit.
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Print a summary of the fetched data
            temp = data['main']['temp']
            weather_desc = data['weather'][0]['description']
            print(f"Success: {city} is currently {temp}°C with {weather_desc}.")
            # In a full pipeline, you would save this JSON data to a database or file here.
            
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {city}: {http_err}")
            if response.status_code == 401:
                print("Please check if your OPENWEATHER_API_KEY is valid.")
        except Exception as err:
            print(f"An error occurred for {city}: {err}")

if __name__ == "__main__":
    # Retrieve API key from environment variable
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    
    if not api_key:
        print("Error: OPENWEATHER_API_KEY environment variable not set.")
        print("Please set it using: export OPENWEATHER_API_KEY='your_api_key'")
        sys.exit(1)
        
    # Default list of cities if none are provided via command line arguments
    target_cities = sys.argv[1:] if len(sys.argv) > 1 else ["London", "New York", "Tokyo"]
    
    fetch_weather(target_cities, api_key)
