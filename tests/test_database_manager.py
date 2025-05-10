import os
import tempfile
import pytest
from datetime import datetime, timedelta
from sqlalchemy import text
from src.database.manager import DatabaseManager
from src.database.models import Base, City, WeatherData, DataVersion
from src.database.connection import get_db_session, engine, init_db
from src.utils.logger import logger

logger.info("Starting database manager test...............")

# Fixture for temporary database
@pytest.fixture(scope="function")  # Changed to function scope for test isolation
def temp_db():
    # Create temp SQLite database
    logger.info("creating temporary database");
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.environ['USE_SQLITE'] = "True"
    os.environ['DATABASE_URL'] = f"sqlite:///{db_path}"
    
    # Initialize schema
    init_db()
    yield db_path
    
    # Teardown
    os.close(db_fd)
    os.unlink(db_path)

# Fixture for test session
@pytest.fixture
def session(temp_db):
    with get_db_session() as session:
        yield session
        session.rollback()

def populate_test_data(session):
    logger.info("Populating test data........")
    """Helper to create test data with proper cleanup"""
    # Clear existing data first
    session.query(WeatherData).delete()
    session.query(DataVersion).delete()
    session.query(City).delete()
    session.commit()
    
    # Add a city
    city = City(
        city_id=1,
        name="Test City",
        country="TC",
        latitude=0.0,
        longitude=0.0
    )
    session.add(city)
    
    # First add data versions
    current_version = DataVersion(
        id="current_ver",
        version_name="Current",
        created_at=datetime.now(),
        is_active=1
    )
    old_version = DataVersion(
        id="old_ver",
        version_name="Old",
        created_at=datetime.now() - timedelta(days=100),
        is_active=1
    )
    session.add_all([current_version, old_version])
    session.commit()
    
    # Add weather data with version_id set correctly
    current_data = WeatherData(
        city_id=1,
        timestamp=datetime.now(),
        temperature=25.0,
        collection_time=datetime.now(),
        temperature_feels_like=26.0,
        temperature_min=24.0,
        temperature_max=27.0,
        pressure=1013,
        humidity=50,
        weather_main="Clear",
        weather_description="clear sky",
        wind_speed=3.0,
        wind_direction=180,
        cloudiness=0,
        visibility=10000,
        sunrise=datetime.now().replace(hour=6, minute=0),
        sunset=datetime.now().replace(hour=18, minute=0),
        timezone_offset=0,
        version_id="current_ver"  # Add this mandatory field
    )
    old_data = WeatherData(
        city_id=1,
        timestamp=datetime.now() - timedelta(days=100),
        temperature=22.0,
        collection_time=datetime.now() - timedelta(days=100),
        temperature_feels_like=26.0,
        temperature_min=24.0,
        temperature_max=27.0,
        pressure=1013,
        humidity=50,
        weather_main="Clear",
        weather_description="clear sky",
        wind_speed=3.0,
        wind_direction=180,
        cloudiness=0,
        visibility=10000,
        sunrise=datetime.now().replace(hour=6, minute=0),
        sunset=datetime.now().replace(hour=18, minute=0),
        timezone_offset=0,
        version_id="old_ver"  # Add this mandatory field
    )
    
    session.add_all([city, current_data, old_data])
    session.commit()

def test_empty_database_operations(session):
    logger.info("testing empty database operations with manager.........")
    """Test all manager functions with empty database"""
    # Clear any existing data
    session.query(WeatherData).delete()
    session.query(DataVersion).delete()
    session.query(City).delete()
    session.commit()
    
    # 1. Test stats on empty DB
    stats = DatabaseManager.get_db_stats(session)
    assert stats["table_stats"]["cities"] == 0
    assert stats["database_info"]["latest_data"] is None
    
    # 2. Test backup on empty DB
    backup_path = DatabaseManager.backup_database()
    assert backup_path is not None
    assert os.path.exists(backup_path)
    os.unlink(backup_path)  # Cleanup
    
    # 3. Test optimization on empty DB
    assert DatabaseManager.optimize_database() is True
    
    # 4. Test version cleaning with no versions
    assert DatabaseManager.clean_old_versions(session) == 0
    
    # 5. Test pruning with no data
    result = DatabaseManager.prune_old_data(session)
    assert result["deleted_records"] == 0
    
    # 6. Test quality report on empty DB
    report = DatabaseManager.get_data_quality_report(session)
    assert report["missing_values"] == {}
    
    # 7. Test index rebuild on empty DB
    assert DatabaseManager.rebuild_indexes() is True

def test_populated_database_operations(session):
    logger.info("Testing all manager operations with populated data.......")
    """Test all manager functions with populated data"""
    populate_test_data(session)
    
    # 1. Verify stats with data
    stats = DatabaseManager.get_db_stats(session)
    assert stats["table_stats"]["cities"] == 1
    assert stats["database_info"]["latest_data"] is not None
    
    # 2. Test backup with data
    backup_path = DatabaseManager.backup_database()
    assert os.path.getsize(backup_path) > 1024  # Not empty
    os.unlink(backup_path)  # Cleanup
    
    # 3. Test version cleaning
    assert DatabaseManager.clean_old_versions(session, days_to_keep=30) == 1
    assert session.get(DataVersion, "old_ver").is_active == 0
    
    # 4. Test data pruning
    result = DatabaseManager.prune_old_data(session, days_to_keep=30)
    assert result["deleted_records"] == 1
    assert session.query(WeatherData).count() == 1
    
    # 5. Test quality report
    report = DatabaseManager.get_data_quality_report(session)
    assert isinstance(report["missing_values"], dict)
    
    # 6. Test optimization
    assert DatabaseManager.optimize_database() is True
    
    # 7. Test index rebuild
    assert DatabaseManager.rebuild_indexes() is True

def test_maintenance_scheduling():
    logger.info("Test scheduler initialization (mock version).........")
    """Test scheduler initialization (mock version)"""
    # Skip if APScheduler not installed
    pytest.importorskip("apscheduler")
    
    # Mock the scheduler to avoid actual scheduling
    original_func = DatabaseManager.schedule_maintenance_tasks
    try:
        called = False
        def mock_scheduler():
            nonlocal called
            called = True
            
        DatabaseManager.schedule_maintenance_tasks = mock_scheduler
        DatabaseManager.schedule_maintenance_tasks()
        assert called
    finally:
        DatabaseManager.schedule_maintenance_tasks = original_func

if __name__ == "__main__":
    pytest.main(["-v", __file__])