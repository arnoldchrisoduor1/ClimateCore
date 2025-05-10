from loguru import logger
import sys
import os
from datetime import datetime
import logging

# Create standard log path
log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
os.makedirs(log_path, exist_ok=True)

# Remove the default handler
logger.remove()

# Add handlers for console and file logging
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add(
    os.path.join(log_path, "weatherflow_{time}.log"),
    rotation="10 MB",
    retention="30 days",
    level="DEBUG"
)

# Create a LoguruHandler class to bridge loguru with standard logging
# This allows the colorama-formatted logs to work alongside loguru
class LoguruHandler(logging.Handler):
    def emit(self, record):
        # Mapping between logging levels and loguru levels
        level_mapping = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL",
        }
        
        # Get corresponding loguru level
        level = level_mapping.get(record.levelno, "INFO")
        
        # Format the message
        message = self.format(record)
        
        # Send to loguru
        logger.opt(depth=1).log(level, message)

def get_console_logger(name):
    """
    Get a standard logging logger that sends output to loguru
    This is for compatibility with the colorama output in the combined scheduler
    """
    # Create standard logging logger
    std_logger = logging.getLogger(name)
    std_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to avoid duplicates
    if std_logger.hasHandlers():
        std_logger.handlers.clear()
    
    # Add the LoguruHandler
    std_logger.addHandler(LoguruHandler())
    
    return std_logger

# Function to create colored log messages for terminal display
def format_colored_log(component, message, status="INFO"):
    """Format a log message with component name for the terminal display"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp} - [{component}] - {status} - {message}"