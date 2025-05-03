"""
Configuration settings for the Craigslist scraper.
"""
import os
from pathlib import Path

# Base directories
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = BASE_DIR / "data"
MAIN_DATA_DIR = BASE_DIR / "main_data"
OUTPUT_DIR = BASE_DIR / "output"

# Create directories if they don't exist
for directory in [DATA_DIR, MAIN_DATA_DIR, OUTPUT_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# File paths
LINKS_CSV = OUTPUT_DIR / "craigslist_links.csv"
OUTPUT_CSV = OUTPUT_DIR / "output_data.csv"
FILTERED_CSV = OUTPUT_DIR / "filtered_phone_numbers.csv"

# Scraping settings
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Timing settings (in seconds)
PAGE_LOAD_WAIT = 8
REPLY_BUTTON_WAIT = 10  # Wait time after clicking Reply button
CALL_BUTTON_WAIT = 10   # Wait time after clicking Call button
NEXT_PAGE_WAIT_MIN = 5
NEXT_PAGE_WAIT_MAX = 10
NEXT_LISTING_WAIT_MIN = 3
NEXT_LISTING_WAIT_MAX = 7

# Selenium settings
WEBDRIVER_WAIT_TIMEOUT = 15
HEADLESS_MODE = True

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5

# Proxy settings (if used)
USE_PROXIES = False
PROXY_LIST = []  # Add your proxies here if needed

# Logging settings
LOG_LEVEL = "INFO"
LOG_FILE = BASE_DIR / "scraper.log"
