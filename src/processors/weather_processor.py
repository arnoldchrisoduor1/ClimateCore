import os
import json
import glob
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Union, Optional

from ..config import RAW_DATA_DIR, ENABLE_VERSIONING
from ..utils.logger import logger
from .data_cleaner import DataCleaner
from .data_analyzer import WeatherAnalyzer
from ..database.connection import get_db_session
from ..database.operations import DatabaseOperations
from ..versioning.data_versioner import DataVersioner

class WeatherProcessor:
    def __init__(self):
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = os.path.join(os.path.dirname(self.raw_data_dir), "processed")
        self.analytics_data_dir = os.path.join(os.path.dirname(self.raw_data_dir), "analytics")
        
        # Ensure directories exist
        Path(self.processed_data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.analytics_data_dir).mkdir(parents=True, exist_ok=True)
        
        self.cleaner = DataCleaner()
        
        # Initialize versioner if enabled
        self.versioner = DataVersioner() if ENABLE_VERSIONING else None
    
    def get_latest_raw_files(self, hours_back: int = 24) -> List[str]:
        """Get a list of raw data files from the last N hours"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        # Get all JSON files in the raw data directory
        all_files = glob.glob(os.path.join(self.raw_data_dir, "*.json"))
        
        # Filter by modified time
        recent_files = [
            file for file in all_files 
            if datetime.fromtimestamp(os.path.getmtime(file)) > cutoff_time
        ]
        
        return recent_files
    
    def process_raw_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Process a single raw weather data file"""
        processed_data = []
        
        try:
            with open(file_path, 'r') as f:
                raw_data = json.load(f)
            
            # Handle both single city files and combined files
            if isinstance(raw_data, list):
                # Combined file with multiple cities
                for city_data in raw_data:
                    try:
                        cleaned_data = self.cleaner.clean_weather_data(city_data)
                        processed_data.append(cleaned_data.dict())
                    except Exception as e:
                        logger.error(f"Error processing city data in {file_path}: {e}")
            else:
                # Single city file
                try:
                    cleaned_data = self.cleaner.clean_weather_data(raw_data)
                    processed_data.append(cleaned_data.dict())
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                    
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            
        return processed_data
    
    def process_recent_data(self) -> pd.DataFrame:
        """Process all recent raw data files"""
        recent_files = self.get_latest_raw_files()
        logger.info(f"Found {len(recent_files)} recent files to process")
        
        all_processed_data = []
        
        for file_path in recent_files:
            processed_data = self.process_raw_file(file_path)
            all_processed_data.extend(processed_data)
        
        # Convert the data to dataframe
        if all_processed_data:
            df = pd.DataFrame(all_processed_data)
            
            # Handle missing values
            df = self.cleaner.handle_missing_values(df)
            
            # Save processed data to file system
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_filepath = os.path.join(
                self.processed_data_dir, 
                f"processed_weather_{timestamp}.csv"
            )
            df.to_csv(processed_filepath, index=False)
            logger.info(f"Saved processed data to {processed_filepath}")
            
            # Store the data in the database with versioning
            self._store_in_database(df)
            
            return df
        else:
            logger.warning("No data was processed")
            return pd.DataFrame()
    
    def _store_in_database(self, df: pd.DataFrame) -> str:
        """Store processed data in database with versioning"""
        if df.empty:
            logger.warning("No data to store in database")
            return None
            
        # Create a new version for this data
        version_id = None
        version_name = f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        with get_db_session() as session:
            try:
                # Create a new version if versioning is enabled
                if ENABLE_VERSIONING:
                    latest_version = DatabaseOperations.get_latest_version(session)
                    parent_id = latest_version.id if latest_version else None
                    
                    # Create version in database
                    version = DatabaseOperations.create_data_version(
                        session,
                        version_name,
                        f"Weather data collected at {datetime.now().isoformat()}",
                        parent_id
                    )
                    version_id = version.id
                    
                    # Create version in file system
                    self.versioner.create_version(
                        version_name,
                        f"Weather data collected at {datetime.now().isoformat()}",
                        parent_id
                    )
                    
                    # Add data to version
                    self.versioner.add_data_to_version(version_id, df, "processed_weather")
                    
                    logger.info(f"Created new data version: {version_id}")
                else:
                    # If versioning is disabled, use a default version ID
                    version_id = "current"
                
                # Store the data in the database
                data_for_storage = df.to_dict('records')
                stored_count = DatabaseOperations.store_weather_data(session, data_for_storage, version_id)
                
                logger.info(f"Stored {stored_count} weather records in database with version {version_id}")
                
                return version_id
                
            except Exception as e:
                logger.error(f"Error storing data in database: {e}")
                session.rollback()
                return None
    
    def generate_daily_stats(self, df: pd.DataFrame = None) -> Optional[pd.DataFrame]:
        """Generate daily statistics from processed data"""
        if df is None or df.empty:
            # Load recent processed files
            processed_files = glob.glob(os.path.join(self.processed_data_dir, "*.csv"))
            if not processed_files:
                logger.warning("No processed files available for analytics")
                return None
                
            df = pd.concat([pd.read_csv(f) for f in processed_files])
        
        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract date for grouping
        df['date'] = df['timestamp'].dt.date
        
        # Generate daily statistics by city
        daily_stats = df.groupby(['date', 'city_name']).agg({
            'temperature': ['min', 'max', 'mean'],
            'humidity': ['min', 'max', 'mean'],
            'pressure': ['min', 'max', 'mean'],
            'wind_speed': ['min', 'max', 'mean']
        })
        
        # Reset index for easier handling
        daily_stats = daily_stats.reset_index()
        
        # Flatten column names
        daily_stats.columns = [
            '_'.join(col).strip('_') for col in daily_stats.columns.values
        ]
        
        # Calculate temperature variation
        daily_stats['temperature_variation'] = daily_stats['temperature_max'] - daily_stats['temperature_min']
        
        # Save analytics to file system
        timestamp = datetime.now().strftime("%Y%m%d")
        analytics_filepath = os.path.join(
            self.analytics_data_dir, 
            f"daily_weather_stats_{timestamp}.csv"
        )
        daily_stats.to_csv(analytics_filepath, index=False)
        logger.info(f"Saved daily analytics to {analytics_filepath}")
        
        # Store the daily stats in the database with versioning
        with get_db_session() as session:
            try:
                # Get the latest version ID
                if ENABLE_VERSIONING:
                    latest_version = DatabaseOperations.get_latest_version(session)
                    version_id = latest_version.id if latest_version else "current"
                else:
                    version_id = "current"
                
                # Store the stats in the database
                DatabaseOperations.store_daily_stats(session, daily_stats, version_id)
                logger.info(f"Stored daily stats in database with version {version_id}")
                
                # If versioning is enabled, also add the stats to the version file
                if ENABLE_VERSIONING and self.versioner:
                    self.versioner.add_data_to_version(version_id, daily_stats, "daily_stats")
                
            except Exception as e:
                logger.error(f"Error storing daily stats in database: {e}")
                session.rollback()
        
        return daily_stats
    
    def run_analysis(self, df: pd.DataFrame = None) -> None:
        """Run various analyses on the processed data"""
        analyzer = WeatherAnalyzer(self.processed_data_dir, self.analytics_data_dir)
        
        if df is None or df.empty:
            df = analyzer.load_recent_data()
            
        if not df.empty:
            # Run various analyses
            analyzer.analyze_temperature_trends(df)
            analyzer.analyze_weather_patterns(df)
            logger.info("Completed weather data analysis")

def run_processor():
    """Run the weather processor and analyzer"""
    processor = WeatherProcessor()
    df = processor.process_recent_data()
    if not df.empty:
        processor.generate_daily_stats(df)
        processor.run_analysis(df)
    return df

if __name__ == "__main__":
    run_processor()