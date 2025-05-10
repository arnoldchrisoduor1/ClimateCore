import json
import os
import time
import datetime
import shutil
import pandas as pd
import subprocess
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import or_, text, func, desc
from sqlalchemy.orm import Session

from ..config import DATABASE_URL, USE_SQLITE, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
from ..utils.logger import logger
from .connection import get_db_session, engine
from .models import City, WeatherData, DataVersion, DailyWeatherStats


class DatabaseManager:
    """Class for database management and maintenance operations"""
    
    @staticmethod
    def get_db_stats(session: Session) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            # Get table row counts
            city_count = session.query(func.count(City.id)).scalar()
            weather_count = session.query(func.count(WeatherData.id)).scalar()
            version_count = session.query(func.count(DataVersion.id)).scalar()
            stats_count = session.query(func.count(DailyWeatherStats.id)).scalar()
            
            # Get disk usage for SQLite
            db_size = 0
            if USE_SQLITE:
                db_path = DATABASE_URL.replace('sqlite:///', '')
                if os.path.exists(db_path):
                    db_size = os.path.getsize(db_path) / (1024 * 1024)  # Size in MB
            
            # Get latest weather data timestamp
            latest_record = session.query(WeatherData).order_by(desc(WeatherData.timestamp)).first()
            latest_timestamp = latest_record.timestamp if latest_record else None
            
            # Get the number of cities
            unique_cities = session.query(func.count(City.id)).scalar()
            
            # Get version information
            latest_version = session.query(DataVersion).order_by(desc(DataVersion.created_at)).first()
            
            return {
                "table_stats": {
                    "cities": city_count,
                    "weather_records": weather_count,
                    "data_versions": version_count,
                    "daily_stats": stats_count,
                    "unique_cities": unique_cities,
                },
                "database_info": {
                    "db_size_mb": db_size if USE_SQLITE else "N/A (PostgreSQL)",
                    "latest_data": latest_timestamp.isoformat() if latest_timestamp else None,
                    "latest_version": {
                        "id": latest_version.id if latest_version else None,
                        "name": latest_version.version_name if latest_version else None,
                        "created_at": latest_version.created_at.isoformat() if latest_version else None
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {"error": str(e)}
    
    @staticmethod
    def backup_database() -> Optional[str]:
        """Create a database backup"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data/backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            if USE_SQLITE:
                # SQLite backup by file copy
                db_path = DATABASE_URL.replace('sqlite:///', '')
                backup_path = os.path.join(backup_dir, f"weatherflow_backup_{timestamp}.db")
                
                if os.path.exists(db_path):
                    shutil.copy2(db_path, backup_path)
                    logger.info(f"SQLite database backup created at {backup_path}")
                    return backup_path
                else:
                    logger.warning(f"SQLite database file not found at {db_path}")
                    return None
            else:
                # PostgreSQL backup using pg_dump
                backup_path = os.path.join(backup_dir, f"weatherflow_backup_{timestamp}.sql")
                
                pg_dump_cmd = [
                    "pg_dump",
                    "-h", DB_HOST,
                    "-p", DB_PORT,
                    "-U", DB_USER,
                    "-F", "p",  # plain text format
                    "-f", backup_path,
                    DB_NAME
                ]
                
                # Set PGPASSWORD environment variable
                env = os.environ.copy()
                env["PGPASSWORD"] = DB_PASSWORD
                
                result = subprocess.run(pg_dump_cmd, env=env, capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info(f"PostgreSQL database backup created at {backup_path}")
                    return backup_path
                else:
                    logger.error(f"PostgreSQL backup failed: {result.stderr}")
                    return None
        except Exception as e:
            logger.error(f"Error backing up database: {e}")
            return None
    
    @staticmethod
    def optimize_database() -> bool:
        """Optimize the database (vacuum for SQLite, analyze for PostgreSQL)"""
        try:
            if USE_SQLITE:
                # SQLite VACUUM
                with engine.connect() as conn:
                    conn.execute(text("VACUUM;"))
                    logger.info("SQLite database vacuumed successfully")
            else:
                # PostgreSQL VACUUM ANALYZE
                with engine.connect() as conn:
                    conn.execute(text("VACUUM ANALYZE;"))
                    logger.info("PostgreSQL database vacuumed and analyzed successfully")
            return True
        except Exception as e:
            logger.error(f"Error optimizing database: {e}")
            return False
    
    @staticmethod
    def clean_old_versions(session: Session, days_to_keep: int = 30) -> int:
        """Deactivate old data versions but keep the data"""
        try:
            cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_to_keep)
            
            # Find old versions
            old_versions = session.query(DataVersion).filter(
                DataVersion.created_at < cutoff_date,
                DataVersion.is_active == 1
            ).all()
            
            # Deactivate them
            for version in old_versions:
                version.is_active = 0
                
            session.commit()
            logger.info(f"Deactivated {len(old_versions)} old data versions")
            return len(old_versions)
        except Exception as e:
            logger.error(f"Error cleaning old versions: {e}")
            session.rollback()
            return 0
    
    @staticmethod
    def prune_old_data(session: Session, days_to_keep: int = 90) -> Dict[str, int]:
        """Remove weather data older than specified days while keeping aggregated stats"""
        try:
            cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_to_keep)
            
            # Delete old weather data
            deleted = session.query(WeatherData).filter(
                WeatherData.timestamp < cutoff_date
            ).delete()
            
            session.commit()
            logger.info(f"Removed {deleted} old weather records older than {days_to_keep} days")
            return {"deleted_records": deleted}
        except Exception as e:
            logger.error(f"Error pruning old data: {e}")
            session.rollback()
            return {"error": str(e), "deleted_records": 0}
    
    @staticmethod
    def get_data_quality_report(session: Session) -> Dict[str, Any]:
        """Check data quality issues"""
        report = {
            "missing_values": {},
            "outliers": {},
            "duplicate_timestamps": 0,
            "cities_without_data": [],
            "recommendations": []
        }
        
        try:
            # Check if there's any weather data at all
            if session.query(WeatherData).count() == 0:
                # Return empty report for empty database
                return report
            
            # Check for missing values in critical columns
            critical_columns = ['temperature', 'humidity', 'pressure']
            for column in critical_columns:
                null_count = session.query(func.count(WeatherData.id)).filter(
                    text(f"{column} IS NULL")
                ).scalar()
                report["missing_values"][column] = null_count
                
                if null_count > 0:
                    report["recommendations"].append(
                        f"Consider handling {null_count} NULL values in {column} column"
                    )
            
            # Check for outliers in temperature
            # Extreme values for temperature (-60°C to 60°C should cover most real-world measurements)
            extreme_temp_count = session.query(func.count(WeatherData.id)).filter(
                or_(
                    WeatherData.temperature < -60,
                    WeatherData.temperature > 60
                )
            ).scalar()
            report["outliers"]["extreme_temperatures"] = extreme_temp_count
            
            if extreme_temp_count > 0:
                report["recommendations"].append(
                    f"Review {extreme_temp_count} records with extreme temperature values"
                )
            
            # Check for duplicate timestamps for the same city
            # This is a complex query that would be better executed directly
            city_ids = session.query(City.id).all()
            total_dupes = 0
            
            for (city_id,) in city_ids:
                timestamp_counts = session.query(
                    WeatherData.timestamp, 
                    func.count(WeatherData.id).label('count')
                ).filter(
                    WeatherData.city_id == city_id
                ).group_by(
                    WeatherData.timestamp
                ).having(
                    func.count(WeatherData.id) > 1
                ).all()
                
                dupes = sum(count - 1 for _, count in timestamp_counts)
                total_dupes += dupes
            
            report["duplicate_timestamps"] = total_dupes
            
            if total_dupes > 0:
                report["recommendations"].append(
                    f"Found {total_dupes} duplicate timestamp entries for same cities"
                )
            
            # Check for cities with no weather data
            cities_without_data = session.query(City).outerjoin(
                WeatherData, City.id == WeatherData.city_id
            ).group_by(City.id).having(
                func.count(WeatherData.id) == 0
            ).all()
            
            report["cities_without_data"] = [city.name for city in cities_without_data]
            
            if cities_without_data:
                report["recommendations"].append(
                    f"Found {len(cities_without_data)} cities with no weather data"
                )
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating data quality report: {e}")
            return {"error": str(e)}
    
    @staticmethod
    def rebuild_indexes() -> bool:
        """Rebuild database indexes for better performance"""
        try:
            if USE_SQLITE:
                # SQLite REINDEX
                with engine.connect() as conn:
                    conn.execute(text("REINDEX;"))
                    logger.info("SQLite indexes rebuilt successfully")
            else:
                # PostgreSQL REINDEX
                tables = ["cities", "weather_data", "data_versions", "daily_weather_stats"]
                with engine.connect() as conn:
                    for table in tables:
                        conn.execute(text(f"REINDEX TABLE {table};"))
                    logger.info("PostgreSQL indexes rebuilt successfully")
            return True
        except Exception as e:
            logger.error(f"Error rebuilding indexes: {e}")
            return False

    @staticmethod
    def schedule_maintenance_tasks():
        """Schedule regular maintenance tasks"""
        from apscheduler.schedulers.background import BackgroundScheduler
        
        scheduler = BackgroundScheduler()
        
        # Daily optimization at 3:00 AM
        scheduler.add_job(
            DatabaseManager.optimize_database,
            'cron', 
            hour=3, 
            minute=0
        )
        
        # Weekly backup at 2:00 AM on Sundays
        scheduler.add_job(
            DatabaseManager.backup_database,
            'cron', 
            day_of_week='sun', 
            hour=2, 
            minute=0
        )
        
        # Monthly pruning of old data at 4:00 AM on the 1st of each month
        def monthly_prune():
            with get_db_session() as session:
                DatabaseManager.prune_old_data(session, days_to_keep=90)
                DatabaseManager.clean_old_versions(session, days_to_keep=30)
                
        scheduler.add_job(
            monthly_prune,
            'cron', 
            day=1, 
            hour=4, 
            minute=0
        )
        
        # Start the scheduler
        scheduler.start()
        logger.info("Database maintenance tasks scheduled")


# When imported, add a command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database Management Utility")
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--backup', action='store_true', help='Create a database backup')
    parser.add_argument('--optimize', action='store_true', help='Optimize the database')
    parser.add_argument('--rebuild-indexes', action='store_true', help='Rebuild database indexes')
    parser.add_argument('--quality-report', action='store_true', help='Generate data quality report')
    parser.add_argument('--prune', type=int, metavar='DAYS', help='Prune data older than specified days')
    parser.add_argument('--clean-versions', type=int, metavar='DAYS', help='Deactivate versions older than specified days')
    
    args = parser.parse_args()
    
    if args.stats:
        with get_db_session() as session:
            stats = DatabaseManager.get_db_stats(session)
            print(json.dumps(stats, indent=2))
    
    if args.backup:
        backup_path = DatabaseManager.backup_database()
        if backup_path:
            print(f"Backup created at: {backup_path}")
        else:
            print("Backup failed.")
    
    if args.optimize:
        success = DatabaseManager.optimize_database()
        print(f"Database optimization {'successful' if success else 'failed'}")
    
    if args.rebuild_indexes:
        success = DatabaseManager.rebuild_indexes()
        print(f"Index rebuild {'successful' if success else 'failed'}")
    
    if args.quality_report:
        with get_db_session() as session:
            report = DatabaseManager.get_data_quality_report(session)
            print(json.dumps(report, indent=2))
    
    if args.prune:
        with get_db_session() as session:
            result = DatabaseManager.prune_old_data(session, days_to_keep=args.prune)
            print(f"Pruned {result['deleted_records']} old records")
    
    if args.clean_versions:
        with get_db_session() as session:
            count = DatabaseManager.clean_old_versions(session, days_to_keep=args.clean_versions)
            print(f"Deactivated {count} old versions")