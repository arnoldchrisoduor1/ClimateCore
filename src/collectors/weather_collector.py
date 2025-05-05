import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

from ..config import WEATHER_API_KEY, WEATHER_API_BASE_URL, CITIES, RAW_DATA_DIR
from ..utils.logger import logger

class WeatherCollector:
    def __init__(self):
        self.api_key = WEATHER_API_KEY
        self.base_url = WEATHER_API_BASE_URL
        self.cities = CITIES
        self.raw_data_dir = RAW_DATA_DIR
        
        # Ensuring the raw data directory exists.
        Path(self.raw_data_dir).mkdir(parents=True, exist_ok=True)
        
    def fetch_weather_data(self, city):
        """Fetching the current weather data for a specified city"""
        url = f"{self.base_url}/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city}: {e}")
            return None
        
    def collect_and_save(self):
        """Collect weather data for all cities and save files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_data = []
        
        for city in self.cities:
            logger.info(f"Collecting weather data for {city}")
            data = self.fetch_weather_data(city)
            
            if data:
                # Adding the collection timestamp
                data["collection_time"] = datetime.now().isoformat()
                all_data.append(data)
                
                # Saving the individual city data.
                city_filename = f"{city.lower().replace(' ', '_')}_{timestamp}.json"
                city_filepath = os.path.join(self.raw_data_dir, city_filename)
                
                with open(city_filepath, "w") as f:
                    json.dump(data, f, indent=2)
                    
                logger.info(f"Saved data for {city} to {city_filepath}")
                
        # Saved the combined data.
        combined_filename = f"weather_data_{timestamp}.json"
        combined_filepath = os.path.join(self.raw_data_dir, combined_filename)
        
        with open(combined_filepath, 'w') as f:
            json.dump(all_data, f, indent=2)
            
        logger.info(f"Saved combined data to {combined_filepath}")
        return combined_filepath
    
def run_collector():
    """Run the weather collector once"""
    collector = WeatherCollector()
    return collector.collect_and_save()

if __name__ == "__main__":
    run_collector()
    