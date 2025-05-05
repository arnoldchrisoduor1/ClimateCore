from loguru import logger
import sys
import os

# configuring the logger.
log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
os.makedirs(log_path, exist_ok=True)

logger.remove() # removing the default handler.
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add(
    os.path.join(log_path, "weatherflow_{time}.log"),
    rotation="10 MB",
    retention="30 days",
    level="DEBUG"
)