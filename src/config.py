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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

# Database configuration
DB_USER = os.getenv("DB_USER", "weatherflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "weatherflow")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "weatherflow")

# SQLite for local development, PostgreSQL for production
USE_SQLITE = os.getenv("USE_SQLITE", "True").lower() == "true"

if USE_SQLITE:
    DATABASE_URL = f"sqlite:///{os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/db/weatherflow.db')}"
else:
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Data versioning settings
DATA_VERSION_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data/versions")
ENABLE_VERSIONING = os.getenv("ENABLE_VERSIONING", "True").lower() == "true"
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
