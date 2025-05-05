import os
from dotenv import load_dotenv

# loading environment variables from .env file
load_dotenv()
# API configuration
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_API_BASE_URL = "https://api.openweathermap.org/data/2.5"

# Data collection settings.
CITIES = ["London", "New York", "Tokyo", "Sydney", "Rio de Janeiro"]
COLLECTION_INTERVAL_HOURS = 1

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw")