import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Union

from ..models.weather_schema import WeatherData, ProcessedWeatherData
from ..utils.logger import logger

class DataCleaner:
    @staticmethod
    def clean_weather_data(raw_data: Dict[str, Any]) -> ProcessedWeatherData:
        """Clean and standardize raw weather data"""
        try:
            #Validation with our schema.
            validated_data = WeatherData(**raw_data)
            
            # Convert to our processed format.
            processed_data = ProcessedWeatherData(
                city_id=validated_data.id,
                city_name=validated_data.name,
                country=validated_data.sys.country,
                latitude=validated_data.coord.lat,
                longitude=validated_data.coord.lon,
                timestamp=datetime.fromtimestamp(validated_data.dt),
                collection_time=datetime.fromisoformat(validated_data.collection_time),
                temperature=validated_data.main.temp,
                temperature_feels_like=validated_data.main.feels_like,
                temperature_min=validated_data.main.temp_min,
                temperature_max=validated_data.main.temp_max,
                pressure=validated_data.main.pressure,
                humidity=validated_data.main.humidity,
                weather_main=validated_data.weather[0].main,
                weather_description=validated_data.weather[0].description,
                wind_speed=validated_data.wind.speed,
                wind_direction=validated_data.wind.deg,
                cloudiness=validated_data.clouds.all,
                visibility=validated_data.visibility,
                sunrise=datetime.fromtimestamp(validated_data.sys.sunrise),
                sunset=datetime.fromtimestamp(validated_data.sys.sunset),
                timezone_offset=validated_data.timezone
            )
            
            return processed_data
        except Exception as e:
            logger.error(f"Error cleaning weather data: {e}")
            raise
        
    @staticmethod
    def detect_outliers(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.Series:
        """Detect outliers using Z-score method"""
        z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
        return z_scores > threshold
    
    @staticmethod
    def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
        """Handle Missing values in the dataframe"""
        # For numeric columns we will fill with median
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].median())
            
        # For categorical/text columns, fill with mode
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else "Unknown")
            
        return df