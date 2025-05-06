import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..utils.logger import logger

class WeatherAnalyzer:
    def __init__(self, processed_data_dir: str, analytics_data_dir: str):
        self.processed_data_dir = processed_data_dir
        self.analytics_data_dir = analytics_data_dir
        
        # Ensure analytics directory exists
        Path(self.analytics_data_dir).mkdir(parents=True, exist_ok=True)
    
    def load_recent_data(self, days: int = 7) -> pd.DataFrame:
        """Load processed data from the last N days"""
        cutoff_time = datetime.now() - timedelta(days=days)
        
        processed_files = [
            os.path.join(self.processed_data_dir, f) 
            for f in os.listdir(self.processed_data_dir) 
            if f.endswith('.csv')
        ]
        
        # Filter by modified time
        recent_files = [
            file for file in processed_files 
            if datetime.fromtimestamp(os.path.getmtime(file)) > cutoff_time
        ]
        
        if not recent_files:
            logger.warning(f"No processed data files found from the last {days} days")
            return pd.DataFrame()
        
        # Combine all recent data
        dfs = []
        for file in recent_files:
            try:
                df = pd.read_csv(file)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
        
        if not dfs:
            logger.warning("No data could be loaded")
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert datetime columns
        datetime_cols = ['timestamp', 'collection_time', 'sunrise', 'sunset']
        for col in datetime_cols:
            if col in combined_df.columns:
                combined_df[col] = pd.to_datetime(combined_df[col])
        
        return combined_df
    
    def analyze_temperature_trends(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Analyze temperature trends over time"""
        if df is None or df.empty:
            df = self.load_recent_data()
            
        if df.empty:
            return pd.DataFrame()
        
        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Group by date and city
        df['date'] = df['timestamp'].dt.date
        temp_trends = df.groupby(['date', 'city_name'])['temperature'].agg(['mean', 'min', 'max'])
        temp_trends = temp_trends.reset_index()
        
        # Calculate rolling averages (3-day)
        city_groups = temp_trends.groupby('city_name')
        rolling_avgs = []
        
        for city, group in city_groups:
            group = group.sort_values('date')
            group['rolling_avg_3day'] = group['mean'].rolling(window=3, min_periods=1).mean()
            rolling_avgs.append(group)
        
        result_df = pd.concat(rolling_avgs, ignore_index=True)
        
        # Save the analysis
        timestamp = datetime.now().strftime("%Y%m%d")
        filepath = os.path.join(
            self.analytics_data_dir, 
            f"temperature_trends_{timestamp}.csv"
        )
        result_df.to_csv(filepath, index=False)
        logger.info(f"Saved temperature trend analysis to {filepath}")
        
        return result_df
    
    def analyze_weather_patterns(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Analyze weather patterns and frequencies"""
        if df is None or df.empty:
            df = self.load_recent_data()
            
        if df.empty:
            return {}
        
        # Count weather conditions by city
        weather_counts = df.groupby(['city_name', 'weather_main']).size().reset_index(name='count')
        
        # Calculate percentages
        total_by_city = weather_counts.groupby('city_name')['count'].sum().reset_index()
        weather_counts = weather_counts.merge(total_by_city, on='city_name', suffixes=('', '_total'))
        weather_counts['percentage'] = (weather_counts['count'] / weather_counts['count_total'] * 100).round(2)
        
        # Pivot for easier reading
        pattern_pivot = weather_counts.pivot(
            index='city_name', 
            columns='weather_main', 
            values='percentage'
        ).fillna(0).reset_index()
        
        # Save the analysis
        timestamp = datetime.now().strftime("%Y%m%d")
        filepath = os.path.join(
            self.analytics_data_dir, 
            f"weather_patterns_{timestamp}.csv"
        )
        pattern_pivot.to_csv(filepath, index=False)
        logger.info(f"Saved weather pattern analysis to {filepath}")
        
        return {
            "weather_counts": weather_counts.to_dict(orient='records'),
            "pattern_pivot": pattern_pivot.to_dict(orient='records')
        }