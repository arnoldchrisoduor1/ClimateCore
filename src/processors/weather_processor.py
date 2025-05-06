import os
import json
import glob
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Union
from .data_analyzer import WeatherAnalyzer

from ..config import RAW_DATA_DIR
from ..utils.logger import logger
from .data_cleaner import DataCleaner

class WeatherProcessor:
    def __init__(self):
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = os.path.join(os.path.dirname(self.raw_data_dir), "processed")
        self.analytics_data_dir = os.path.join(os.path.dirname(self.raw_data_dir), "analytics")
        
        # Ensure directories exist
        Path(self.processed_data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.analytics_data_dir).mkdir(parents=True, exist_ok=True)
        
        self.cleaner = DataCleaner()
    
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
        
        # Convert to DataFrame
        if all_processed_data:
            df = pd.DataFrame(all_processed_data)
            
            # Handle missing values
            df = self.cleaner.handle_missing_values(df)
            
            # Save processed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_filepath = os.path.join(
                self.processed_data_dir, 
                f"processed_weather_{timestamp}.csv"
            )
            df.to_csv(processed_filepath, index=False)
            logger.info(f"Saved processed data to {processed_filepath}")
            
            return df
        else:
            logger.warning("No data was processed")
            return pd.DataFrame()
    
    def generate_daily_stats(self, df: pd.DataFrame = None) -> None:
        """Generate daily statistics from processed data"""
        if df is None or df.empty:
            # Load recent processed files
            processed_files = glob.glob(os.path.join(self.processed_data_dir, "*.csv"))
            if not processed_files:
                logger.warning("No processed files available for analytics")
                return
                
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
        
        # Save analytics
        timestamp = datetime.now().strftime("%Y%m%d")
        analytics_filepath = os.path.join(
            self.analytics_data_dir, 
            f"daily_weather_stats_{timestamp}.csv"
        )
        daily_stats.to_csv(analytics_filepath, index=False)
        logger.info(f"Saved daily analytics to {analytics_filepath}")
        
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