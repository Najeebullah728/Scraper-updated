"""
Utility functions for the Craigslist scraper.
"""
import logging
import random
import time
from pathlib import Path
from typing import Optional, Union, List, Dict, Any

import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """
    Sleep for a random amount of time between min and max seconds.
    
    Args:
        min_seconds: Minimum sleep time in seconds
        max_seconds: Maximum sleep time in seconds
    """
    delay = random.uniform(min_seconds, max_seconds)
    logger.debug(f"Sleeping for {delay:.2f} seconds")
    time.sleep(delay)


def retry_on_exception(max_retries: int = 3, delay: int = 5):
    """
    Decorator to retry a function on exception.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    logger.warning(f"Error in {func.__name__}: {e}. Retry {retries}/{max_retries}")
                    if retries >= max_retries:
                        logger.error(f"Max retries reached for {func.__name__}")
                        raise
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


def save_to_file(content: str, file_path: Union[str, Path], mode: str = "w") -> bool:
    """
    Save content to a file with proper error handling.
    
    Args:
        content: Content to save
        file_path: Path to save the file
        mode: File open mode
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(file_path, mode, encoding="utf-8") as f:
            f.write(content)
        return True
    except Exception as e:
        logger.error(f"Error saving to {file_path}: {e}")
        return False


def create_directory(directory: Union[str, Path]) -> bool:
    """
    Create a directory if it doesn't exist.
    
    Args:
        directory: Directory path to create
        
    Returns:
        bool: True if successful or already exists, False otherwise
    """
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {e}")
        return False


def get_progress_bar(current: int, total: int, width: int = 50) -> str:
    """
    Generate a text-based progress bar.
    
    Args:
        current: Current progress value
        total: Total value
        width: Width of the progress bar
        
    Returns:
        str: Text-based progress bar
    """
    percent = current / total
    filled_length = int(width * percent)
    bar = '█' * filled_length + '░' * (width - filled_length)
    return f"[{bar}] {current}/{total} ({percent:.1%})"
