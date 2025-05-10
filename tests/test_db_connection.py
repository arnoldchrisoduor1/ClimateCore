import sys
import os
import time

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import init_db, get_db_session
from src.database.models import City, WeatherData, DataVersion, DailyWeatherStats
from src.database.operations import DatabaseOperations
from src.utils.logger import logger
from src.config import DATABASE_URL

def test_connection():
    """Test database connection and table creation"""
    logger.info(f"Testing connection to database: {DATABASE_URL}")
    
    try:
        # Initialize database tables
        init_db()
        logger.info("Database tables created successfully")
        
        # Get session
        session = get_db_session()
        
        # Test creating a data version
        version = DatabaseOperations.create_data_version(
            session=session,
            name="test_version",
            description="Test version for connection testing"
        )
        logger.info(f"Created data version with ID: {version.id}")
        
        # Test creating a city
        city_data = {
            "city_id": 1,
            "city_name": "Test City",
            "country": "TC",
            "latitude": 10.0,
            "longitude": 10.0
        }
        city = DatabaseOperations.get_or_create_city(session, city_data)
        logger.info(f"Created city with ID: {city.id}")
        
        # Close session
        session.close()
        logger.info("Database connection test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error testing database connection: {e}")
        return False

if __name__ == "__main__":
    # Delay to ensure database is ready
    time.sleep(2)
    test_connection()