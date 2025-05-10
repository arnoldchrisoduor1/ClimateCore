import time
import schedule
from datetime import datetime

from src.collectors.weather_collector import run_collector
from src.utils.logger import logger

# Schedule data collection.
def job():
    logger.info(f"Running scheduled data collection at {datetime.now().isoformat()}")
    try:
        run_collector()
        logger.info("Scheduled collection completed successfully")
    except Exception as e:
        logger.error(f"Error in scheduled collection: {e}")
        
def main():
    # run once immediately at startup
    job()
    
    # setting up the schedule hourly by default.
    schedule.every(1).hours.do(job)
    
    logger.info("weather data collector scheduler started")
    
    # keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # checking every minute.
        
if __name__ == "__main__":
    main()