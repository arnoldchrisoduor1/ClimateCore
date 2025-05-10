import time
import schedule
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import glob
import os

from src.database.operations import DatabaseOperations
from src.database.connection import get_db_session
from src.utils.logger import logger

class StorageScheduler:
    def __init__(self):
        self.processed_data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "processed"
        )
        self.last_processed_time = datetime.min

    def get_new_files(self) -> list:
        """Get newly created processed files since last run"""
        all_files = glob.glob(os.path.join(self.processed_data_dir, "*.csv"))
        return [
            f for f in all_files
            if datetime.fromtimestamp(os.path.getmtime(f)) > self.last_processed_time
        ]

    def process_file(self, file_path: str) -> pd.DataFrame:
        """Load and validate a processed CSV file"""
        try:
            df = pd.read_csv(file_path)
            required_columns = [
                'city_id', 'city_name', 'country', 'timestamp',
                'temperature', 'humidity', 'pressure'
            ]
            
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in {file_path}")
                return pd.DataFrame()
            
            # Convert string timestamps to datetime
            datetime_cols = ['timestamp', 'collection_time', 'sunrise', 'sunset']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return pd.DataFrame()

    def store_data(self):
        """Main storage job to process new files"""
        logger.info("Starting storage job...")
        new_files = self.get_new_files()
        
        if not new_files:
            logger.info("No new files to process")
            return

        with get_db_session() as session:
            try:
                # Create new version for this batch
                version = DatabaseOperations.create_data_version(
                    session,
                    name=f"auto_{datetime.now().strftime('%Y%m%d_%H%M')}",
                    description="Automatically created version"
                )
                
                total_records = 0
                
                for file_path in new_files:
                    df = self.process_file(file_path)
                    if not df.empty:
                        records = DatabaseOperations.store_weather_data(
                            session,
                            df.to_dict('records'),
                            version.id
                        )
                        total_records += records
                        logger.info(f"Stored {records} records from {Path(file_path).name}")
                        
                        # Generate daily stats for this batch
                        stats_df = self.generate_daily_stats(df)
                        if not stats_df.empty:
                            DatabaseOperations.store_daily_stats(
                                session,
                                stats_df,
                                version.id
                            )
                
                logger.info(f"Storage job complete. Total records: {total_records}")
                self.last_processed_time = datetime.now()
                
            except Exception as e:
                logger.error(f"Storage job failed: {str(e)}")
                session.rollback()

    def generate_daily_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate daily stats from processed data"""
        try:
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            stats = df.groupby(['date', 'city_name']).agg({
                'temperature': ['min', 'max', 'mean'],
                'humidity': ['min', 'max', 'mean'],
                'pressure': ['min', 'max', 'mean'],
                'wind_speed': ['min', 'max', 'mean']
            })
            
            stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
            stats = stats.reset_index()
            stats['temperature_variation'] = stats['temperature_max'] - stats['temperature_min']
            
            return stats
            
        except Exception as e:
            logger.error(f"Error generating stats: {str(e)}")
            return pd.DataFrame()

def main():
    scheduler = StorageScheduler()
    
    # Run immediately on startup
    scheduler.store_data()
    
    # Schedule to run every hour
    schedule.every(1).hours.do(scheduler.store_data)
    
    logger.info("Storage scheduler started. Runs hourly...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()