#!/usr/bin/env python3
import time
import schedule
from datetime import datetime
import threading
from colorama import Fore, Style, init

# Initialize colorama for cross-platform colored terminal output
init(autoreset=True)

# Import your existing modules
from src.collectors.weather_collector import run_collector
from src.processors.weather_processor import run_processor
from src.storage_scheduler import StorageScheduler
from src.utils.logger import logger, format_colored_log, get_console_logger

class CombinedScheduler:
    def __init__(self):
        # Initialize the storage scheduler
        self.storage_scheduler = StorageScheduler()
        
        # Get console logger for colored output
        self.console_logger = get_console_logger("combined_scheduler")
        
        # Task intervals (in seconds) - shortened for testing
        self.intervals = {
            "collector": 30,  # Run every 30 seconds (was 3600 / 1 hour)
            "processor": 30,  # Run every 30 seconds (was 10800 / 3 hours)
            "storage": 30,    # Run every 30 seconds (was 3600 / 1 hour)
        }
        
        # For status reporting
        self.last_run = {
            "collector": None,
            "processor": None,
            "storage": None
        }
        self.status = {
            "collector": "Waiting to run",
            "processor": "Waiting to run",
            "storage": "Waiting to run"
        }

    def run_collector_job(self):
        """Run the collector job"""
        self.status["collector"] = "Running..."
        message = format_colored_log("COLLECTOR", f"Running at {datetime.now().isoformat()}")
        logger.info(message)
        print(f"\n{Fore.CYAN}[COLLECTOR] {Style.RESET_ALL}Running at {datetime.now().isoformat()}")
        
        try:
            run_collector()
            self.status["collector"] = "Success"
            message = format_colored_log("COLLECTOR", "Completed successfully", "SUCCESS")
            logger.info(message)
            print(f"{Fore.GREEN}[COLLECTOR] {Style.RESET_ALL}Completed successfully")
        except Exception as e:
            self.status["collector"] = f"Failed: {str(e)}"
            message = format_colored_log("COLLECTOR", f"Error: {e}", "ERROR")
            logger.error(message)
            print(f"{Fore.RED}[COLLECTOR] {Style.RESET_ALL}Error: {e}")
        
        self.last_run["collector"] = datetime.now()

    def run_processor_job(self):
        """Run the processor job"""
        self.status["processor"] = "Running..."
        message = format_colored_log("PROCESSOR", f"Running at {datetime.now().isoformat()}")
        logger.info(message)
        print(f"\n{Fore.YELLOW}[PROCESSOR] {Style.RESET_ALL}Running at {datetime.now().isoformat()}")
        
        try:
            run_processor()
            self.status["processor"] = "Success"
            message = format_colored_log("PROCESSOR", "Completed successfully", "SUCCESS")
            logger.info(message)
            print(f"{Fore.GREEN}[PROCESSOR] {Style.RESET_ALL}Completed successfully")
        except Exception as e:
            self.status["processor"] = f"Failed: {str(e)}"
            message = format_colored_log("PROCESSOR", f"Error: {e}", "ERROR")
            logger.error(message)
            print(f"{Fore.RED}[PROCESSOR] {Style.RESET_ALL}Error: {e}")
        
        self.last_run["processor"] = datetime.now()

    def run_storage_job(self):
        """Run the storage job"""
        self.status["storage"] = "Running..."
        message = format_colored_log("STORAGE", f"Running at {datetime.now().isoformat()}")
        logger.info(message)
        print(f"\n{Fore.MAGENTA}[STORAGE] {Style.RESET_ALL}Running at {datetime.now().isoformat()}")
        
        try:
            self.storage_scheduler.store_data()
            self.status["storage"] = "Success"
            message = format_colored_log("STORAGE", "Completed successfully", "SUCCESS")
            logger.info(message)
            print(f"{Fore.GREEN}[STORAGE] {Style.RESET_ALL}Completed successfully")
        except Exception as e:
            self.status["storage"] = f"Failed: {str(e)}"
            message = format_colored_log("STORAGE", f"Error: {e}", "ERROR")
            logger.error(message)
            print(f"{Fore.RED}[STORAGE] {Style.RESET_ALL}Error: {e}")
        
        self.last_run["storage"] = datetime.now()

    def print_status(self):
        """Print current status of all jobs"""
        status_header = f"WEATHER SYSTEM STATUS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        divider = "="*80
        
        # Log status to file
        logger.info(f"\n{divider}\n{status_header}\n{divider}")
        
        # Print to terminal with colors
        print("\n" + divider)
        print(f"{Fore.WHITE}{Style.BRIGHT}{status_header}")
        print(divider)
        
        for job, status in self.status.items():
            last_run = self.last_run[job]
            last_run_str = last_run.strftime('%Y-%m-%d %H:%M:%S') if last_run else "Never"
            
            color = Fore.GREEN
            log_level = "INFO"
            if "Failed" in status:
                color = Fore.RED
                log_level = "ERROR"
            elif "Running" in status:
                color = Fore.YELLOW
                log_level = "WARNING"
            
            status_line = f"{job.upper():<10} - Last run: {last_run_str:<20} - Status: {status}"
            # Use proper loguru level names (uppercase)
            logger.log(log_level, status_line)
            print(f"{job.upper():<10} - Last run: {last_run_str:<20} - Status: {color}{status}{Style.RESET_ALL}")
        
        logger.info(divider)
        print(divider + "\n")

    def start(self):
        """Start the combined scheduler"""
        start_message = "Starting Combined Weather System Scheduler"
        logger.info(start_message)
        print(f"{Fore.CYAN}{Style.BRIGHT}{start_message}{Style.RESET_ALL}")
        
        # Run all jobs once at startup
        init_message = "Running initial jobs..."
        logger.info(init_message)
        print(f"\n{init_message}")
        self.run_collector_job()
        self.run_processor_job()
        self.run_storage_job()
        
        # Schedule jobs - using seconds for testing
        schedule.every(self.intervals["collector"]).seconds.do(self.run_collector_job)
        schedule.every(self.intervals["processor"]).seconds.do(self.run_processor_job)
        schedule.every(self.intervals["storage"]).seconds.do(self.run_storage_job)
        
        # Schedule status report every 15 seconds
        schedule.every(15).seconds.do(self.print_status)
        
        # Log scheduler setup details
        setup_message = f"All schedulers started successfully with the following intervals:\n" \
                        f"  Collector: Every {self.intervals['collector']} seconds\n" \
                        f"  Processor: Every {self.intervals['processor']} seconds\n" \
                        f"  Storage: Every {self.intervals['storage']} seconds\n" \
                        f"  Status updates: Every 15 seconds"
        logger.info(setup_message)
        
        # Print to terminal with color
        print(f"\n{Fore.GREEN}All schedulers started successfully with the following intervals:")
        print(f"  Collector: Every {self.intervals['collector']} seconds")
        print(f"  Processor: Every {self.intervals['processor']} seconds")
        print(f"  Storage: Every {self.intervals['storage']} seconds")
        print(f"  Status updates: Every 15 seconds{Style.RESET_ALL}\n")
        
        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(1)  # Check every second for pending tasks

def main():
    scheduler = CombinedScheduler()
    scheduler.start()

if __name__ == "__main__":
    main()