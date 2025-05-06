import time
import schedule
from datetime import datetime

from processors.weather_processor import run_processor
from utils.logger import logger

def job():
    logger.info(f"Running scheduled data processing at {datetime.now().isoformat()}")
    try:
        run_processor()
        logger.info("Scheduled processing completed successfuly")
    except Exception as e:
        logger.error(f"Error in scheduled processing: {e}")
        
def main():
    # Run once immediately at startup
    job()
    
    # process data every 3 hours.
    schedule.every(3).hours.do(job)
    
    logger.info("Weather data processor scheduler started")
    
    # Keeping the script running.
    while True:
        schedule.run_pending()
        time.sleep(60) # checking every minute
        
if __name__ == "__main__":
    main()