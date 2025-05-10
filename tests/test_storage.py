import os
import sys
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import init_db, get_db_session
from src.database.models import City, WeatherData, DataVersion
from src.database.operations import DatabaseOperations

def test_storage():
    """Test database storage operations"""
    # Initialize database
    init_db()
    
    # Create test data
    test_city = {
        "city_id": 1,
        "city_name": "TestCity",
        "country": "TC",
        "latitude": 0.0,
        "longitude": 0.0
    }
    
    test_weather = {
        "city_id": 1,
        "city_name": "TestCity",
        "country": "TC",
        "latitude": 0.0,
        "longitude": 0.0,
        "timestamp": datetime.utcnow(),
        "collection_time": datetime.utcnow(),
        "temperature": 25.0,
        "temperature_feels_like": 26.0,
        "temperature_min": 24.0,
        "temperature_max": 27.0,
        "pressure": 1013,
        "humidity": 50,
        "weather_main": "Clear",
        "weather_description": "clear sky",
        "wind_speed": 3.0,
        "wind_direction": 180,
        "cloudiness": 0,
        "visibility": 10000,
        "sunrise": datetime.utcnow().replace(hour=6, minute=0),
        "sunset": datetime.utcnow().replace(hour=18, minute=0),
        "timezone_offset": 0
    }

    with get_db_session() as session:
        # Test city creation
        city = DatabaseOperations.get_or_create_city(session, test_city)
        print(f"City created: {city.name} (ID: {city.id})")
        
        # Test version creation
        version = DatabaseOperations.create_data_version(
            session,
            name="test_version",
            description="Test version"
        )
        print(f"Version created: {version.version_name} (ID: {version.id})")
        
        # Test weather data storage
        count = DatabaseOperations.store_weather_data(
            session,
            [test_weather],
            version.id
        )
        print(f"Inserted {count} weather records")
        
        # Verify data
        records = session.query(WeatherData).count()
        print(f"Total weather records in DB: {records}")

if __name__ == "__main__":
    test_storage()