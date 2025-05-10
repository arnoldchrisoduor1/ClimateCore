from typing import List, Dict, Any, Optional, Union
from datetime import datetime, date, timedelta
import pandas as pd
import uuid

from sqlalchemy import func, and_, or_, desc
from sqlalchemy.orm import Session

from .connection import get_db_session
from .models import City, WeatherData, DataVersion, DailyWeatherStats
from ..utils.logger import logger

class DatabaseOperations:
    """Class for database operations"""
    
    @staticmethod
    def get_or_create_city(session: Session, city_data: Dict[str, Any]) -> City:
        """Get or create a city record"""
        city = session.query(City).filter(City.city_id == city_data['city_id']).first()
        
        if not city:
            city = City(
                city_id=city_data['city_id'],
                name=city_data['city_name'],
                country=city_data['country'],
                latitude=city_data['latitude'],
                longitude=city_data['longitude']
            )
            session.add(city)
            session.commit()
            logger.info(f"Created new city record for {city_data['city_name']}")
        
        return city
    
    @staticmethod
    def create_data_version(session: Session, name: str, description: str = None, 
                           parent_version_id: str = None) -> DataVersion:
        """Create a new data version"""
        version = DataVersion(
            id=str(uuid.uuid4()),
            version_name=name,
            description=description,
            parent_version_id=parent_version_id,
            created_at=datetime.utcnow(),
            created_by="system",
            is_active=1
        )
        session.add(version)
        session.commit()
        logger.info(f"Created new data version: {version.id} - {name}")
        return version
    
    @staticmethod
    def get_latest_version(session: Session) -> Optional[DataVersion]:
        """Get the latest active data version"""
        return session.query(DataVersion)\
                     .filter(DataVersion.is_active == 1)\
                     .order_by(desc(DataVersion.created_at))\
                     .first()
    
    @staticmethod
    def store_weather_data(session: Session, weather_data: List[Dict[str, Any]], 
                          version_id: str) -> int:
        """Store weather data in the database"""
        inserted_count = 0
        
        for data in weather_data:
            try:
                # Get or create city
                city = DatabaseOperations.get_or_create_city(session, data)
                
                # Check if this exact data point already exists
                existing = session.query(WeatherData).filter(
                    WeatherData.city_id == city.id,
                    WeatherData.timestamp == data['timestamp']
                ).first()
                
                if existing:
                    logger.debug(f"Weather data for {data['city_name']} at {data['timestamp']} already exists")
                    continue
                
                # Create weather data record
                weather_record = WeatherData(
                    city_id=city.id,
                    timestamp=data['timestamp'],
                    collection_time=data['collection_time'],
                    temperature=data['temperature'],
                    temperature_feels_like=data['temperature_feels_like'],
                    temperature_min=data['temperature_min'],
                    temperature_max=data['temperature_max'],
                    pressure=data['pressure'],
                    humidity=data['humidity'],
                    weather_main=data['weather_main'],
                    weather_description=data['weather_description'],
                    wind_speed=data['wind_speed'],
                    wind_direction=data['wind_direction'],
                    cloudiness=data['cloudiness'],
                    visibility=data['visibility'],
                    sunrise=data['sunrise'],
                    sunset=data['sunset'],
                    timezone_offset=data['timezone_offset'],
                    version_id=version_id
                )
                
                session.add(weather_record)
                inserted_count += 1
                
                # Commit every 100 records to avoid large transactions
                if inserted_count % 100 == 0:
                    session.commit()
                    
            except Exception as e:
                logger.error(f"Error storing weather data: {e}")
                session.rollback()
        
        # Final commit
        session.commit()
        logger.info(f"Inserted {inserted_count} weather records")
        return inserted_count
    
    @staticmethod
    def store_daily_stats(session: Session, stats_data: pd.DataFrame, version_id: str) -> int:
        """Store daily weather statistics"""
        inserted_count = 0
        
        for _, row in stats_data.iterrows():
            try:
                # Get city
                city = session.query(City).filter(City.name == row['city_name']).first()
                
                if not city:
                    logger.warning(f"City not found for stats: {row['city_name']}")
                    continue
                
                # Convert date if it's a string
                if isinstance(row['date'], str):
                    stat_date = datetime.strptime(row['date'], '%Y-%m-%d').date()
                else:
                    stat_date = row['date']
                
                # Check if this daily stat already exists
                existing = session.query(DailyWeatherStats).filter(
                    DailyWeatherStats.city_id == city.id,
                    DailyWeatherStats.date == stat_date
                ).first()
                
                if existing:
                    # Update existing stat
                    existing.temperature_min = row['temperature_min']
                    existing.temperature_max = row['temperature_max']
                    existing.temperature_avg = row['temperature_mean']
                    existing.humidity_min = row['humidity_min'] if 'humidity_min' in row else None
                    existing.humidity_max = row['humidity_max'] if 'humidity_max' in row else None
                    existing.humidity_avg = row['humidity_mean'] if 'humidity_mean' in row else None
                    existing.pressure_min = row['pressure_min'] if 'pressure_min' in row else None
                    existing.pressure_max = row['pressure_max'] if 'pressure_max' in row else None
                    existing.pressure_avg = row['pressure_mean'] if 'pressure_mean' in row else None
                    existing.wind_speed_min = row['wind_speed_min'] if 'wind_speed_min' in row else None
                    existing.wind_speed_max = row['wind_speed_max'] if 'wind_speed_max' in row else None
                    existing.wind_speed_avg = row['wind_speed_mean'] if 'wind_speed_mean' in row else None
                    existing.temperature_variation = row['temperature_variation'] if 'temperature_variation' in row else None
                    existing.version_id = version_id
                else:
                    # Create new stat
                    daily_stat = DailyWeatherStats(
                        city_id=city.id,
                        date=stat_date,
                        temperature_min=row['temperature_min'],
                        temperature_max=row['temperature_max'],
                        temperature_avg=row['temperature_mean'],
                        humidity_min=row['humidity_min'] if 'humidity_min' in row else None,
                        humidity_max=row['humidity_max'] if 'humidity_max' in row else None,
                        humidity_avg=row['humidity_mean'] if 'humidity_mean' in row else None,
                        pressure_min=row['pressure_min'] if 'pressure_min' in row else None,
                        pressure_max=row['pressure_max'] if 'pressure_max' in row else None,
                        pressure_avg=row['pressure_mean'] if 'pressure_mean' in row else None,
                        wind_speed_min=row['wind_speed_min'] if 'wind_speed_min' in row else None,
                        wind_speed_max=row['wind_speed_max'] if 'wind_speed_max' in row else None,
                        wind_speed_avg=row['wind_speed_mean'] if 'wind_speed_mean' in row else None,
                        temperature_variation=row['temperature_variation'] if 'temperature_variation' in row else None,
                        version_id=version_id
                    )
                    session.add(daily_stat)
                    inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error storing daily stats: {e}")
                session.rollback()
        
        # Commit changes
        session.commit()
        logger.info(f"Inserted/updated {inserted_count} daily stats records")
        return inserted_count
    
    @staticmethod
    def get_weather_by_city(session: Session, city_name: str, 
                           start_date: datetime, end_date: datetime = None) -> List[Dict]:
        """Get weather data for a city in a date range"""
        if end_date is None:
            end_date = datetime.utcnow()
            
        # Get the city
        city = session.query(City).filter(City.name == city_name).first()
        
        if not city:
            logger.warning(f"City not found: {city_name}")
            return []
        
        # Get weather data
        query = session.query(WeatherData).filter(
            WeatherData.city_id == city.id,
            WeatherData.timestamp >= start_date,
            WeatherData.timestamp <= end_date
        ).order_by(WeatherData.timestamp)
        
        results = query.all()
        return [
            {
                "id": record.id,
                "city_name": city.name,
                "timestamp": record.timestamp,
                "temperature": record.temperature,
                "weather_main": record.weather_main,
                "humidity": record.humidity,
                "wind_speed": record.wind_speed,
                "pressure": record.pressure,
                "version_id": record.version_id
            }
            for record in results
        ]
    
    @staticmethod
    def get_stats_by_city(session: Session, city_name: str, 
                         start_date: date, end_date: date = None) -> List[Dict]:
        """Get daily stats for a city in a date range"""
        if end_date is None:
            end_date = datetime.utcnow().date()
            
        # Get the city
        city = session.query(City).filter(City.name == city_name).first()
        
        if not city:
            logger.warning(f"City not found: {city_name}")
            return []
        
        # Get daily stats
        query = session.query(DailyWeatherStats).filter(
            DailyWeatherStats.city_id == city.id,
            DailyWeatherStats.date >= start_date,
            DailyWeatherStats.date <= end_date
        ).order_by(DailyWeatherStats.date)
        
        results = query.all()
        return [
            {
                "date": record.date,
                "temperature_min": record.temperature_min,
                "temperature_max": record.temperature_max,
                "temperature_avg": record.temperature_avg,
                "temperature_variation": record.temperature_variation,
                "humidity_avg": record.humidity_avg,
                "pressure_avg": record.pressure_avg,
                "wind_speed_avg": record.wind_speed_avg,
                "version_id": record.version_id
            }
            for record in results
        ]