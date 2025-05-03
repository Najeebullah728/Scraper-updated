"""
Test script to verify that the scraper is working correctly.
"""
import os
import sys
from pathlib import Path

import config
from browser import Browser
from selenium.webdriver.common.by import By
from utils import logger

def test_browser():
    """Test that the browser can navigate to Craigslist and find listings."""
    logger.info("Testing browser navigation...")
    
    url = "https://losangeles.craigslist.org/search/cta#search=2~gallery~0"
    
    with Browser(headless=True) as browser:
        # Navigate to the URL
        if not browser.navigate(url):
            logger.error("Failed to navigate to URL")
            return False
            
        # Wait for page to load and check for listings
        listings = browser.find_elements(By.CLASS_NAME, "gallery-card")
        
        # If no listings found with gallery-card, try other common selectors
        if not listings:
            listings = browser.find_elements(By.CSS_SELECTOR, ".result-row, .cl-static-search-result")
            
        if not listings:
            logger.error("No listings found on the page")
            return False
            
        logger.info(f"Found {len(listings)} listings - Browser test PASSED")
        return True

def test_directories():
    """Test that all required directories exist."""
    logger.info("Testing directory structure...")
    
    directories = [
        config.DATA_DIR,
        config.MAIN_DATA_DIR,
        config.OUTPUT_DIR
    ]
    
    all_exist = True
    
    for directory in directories:
        if not directory.exists():
            logger.error(f"Directory does not exist: {directory}")
            all_exist = False
            
    if all_exist:
        logger.info("All directories exist - Directory test PASSED")
    
    return all_exist

def main():
    """Run all tests."""
    logger.info("Starting scraper tests...")
    
    tests = [
        ("Directory Structure", test_directories),
        ("Browser Navigation", test_browser)
    ]
    
    all_passed = True
    
    for name, test_func in tests:
        logger.info(f"Running test: {name}")
        try:
            result = test_func()
            if not result:
                all_passed = False
                logger.error(f"Test FAILED: {name}")
            else:
                logger.info(f"Test PASSED: {name}")
        except Exception as e:
            all_passed = False
            logger.error(f"Test ERROR: {name} - {e}")
    
    if all_passed:
        logger.info("All tests PASSED! The scraper is ready to use.")
        return 0
    else:
        logger.error("Some tests FAILED. Please check the logs for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
