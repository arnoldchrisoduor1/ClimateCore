import uuid
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index, Table, Numeric, Text, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import backref

from .connection import Base

class City(Base):
    """City model to store city information"""
    __tablename__ = "cities"
    
    id = Column(Integer, primary_key=True, index=True)
    city_id = Column(Integer, unique=True, index=True, comment="Original city ID from API")
    name = Column(String(100), index=True, nullable=False)
    country = Column(String(2), index=True, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    weather_data = relationship("WeatherData", back_populates="city")
    
    # Indexes
    __table_args__ = (
        Index('idx_city_location', 'latitude', 'longitude'),
        Index('idx_city_name_country', 'name', 'country'),
    )

class WeatherData(Base):
    """Weather data model for storing weather observations"""
    __tablename__ = "weather_data"
    
    id = Column(Integer, primary_key=True, index=True)
    city_id = Column(Integer, ForeignKey("cities.id"), index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    collection_time = Column(DateTime, nullable=False)
    temperature = Column(Float, nullable=False)
    temperature_feels_like = Column(Float)
    temperature_min = Column(Float)
    temperature_max = Column(Float)
    pressure = Column(Integer)
    humidity = Column(Integer)
    weather_main = Column(String(50))
    weather_description = Column(String(100))
    wind_speed = Column(Float)
    wind_direction = Column(Integer)
    cloudiness = Column(Integer)
    visibility = Column(Integer)
    sunrise = Column(DateTime)
    sunset = Column(DateTime)
    timezone_offset = Column(Integer)
    version_id = Column(String(36), nullable=False, index=True, comment="Data version identifier")
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    city = relationship("City", back_populates="weather_data")
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_weather_city_time', 'city_id', 'timestamp'),
        Index('idx_weather_version', 'version_id'),
        Index('idx_weather_temp', 'temperature'),
        Index('idx_weather_conditions', 'weather_main'),
        Index('idx_weather_timestamp_range', 'timestamp', 'city_id'),
    )

class DataVersion(Base):
    """Model for tracking data versions"""
    __tablename__ = "data_versions"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    version_name = Column(String(100), nullable=False)
    description = Column(Text)
    parent_version_id = Column(String(36), ForeignKey("data_versions.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = Column(String(100), default="system")
    is_active = Column(Integer, default=1, nullable=False)
    metadata_json = Column(Text, comment="Additional version metadata")
    
    # Corrected relationship
    child_versions = relationship(
        "DataVersion",
        backref=backref("parent_version", remote_side=[id]),
        foreign_keys=[parent_version_id]
    )

class DailyWeatherStats(Base):
    """Model for storing daily weather statistics"""
    __tablename__ = "daily_weather_stats"
    
    id = Column(Integer, primary_key=True)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    temperature_min = Column(Float)
    temperature_max = Column(Float)
    temperature_avg = Column(Float)
    humidity_min = Column(Integer)
    humidity_max = Column(Integer)
    humidity_avg = Column(Float)
    pressure_min = Column(Integer)
    pressure_max = Column(Integer)
    pressure_avg = Column(Float)
    wind_speed_min = Column(Float)
    wind_speed_max = Column(Float)
    wind_speed_avg = Column(Float)
    temperature_variation = Column(Float)
    version_id = Column(String(36), ForeignKey("data_versions.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    city = relationship("City")
    
    # Indexes
    __table_args__ = (
        Index('idx_daily_stats_city_date', 'city_id', 'date', unique=True),
        Index('idx_daily_stats_version', 'version_id'),
    )