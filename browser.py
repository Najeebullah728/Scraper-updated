"""
Browser management module for Selenium interactions.
"""
import logging
import random
from typing import Optional, List, Dict, Any, Union

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager

import config
from utils import logger, random_delay, retry_on_exception

class Browser:
    """
    Browser class to handle Selenium WebDriver operations with enhanced reliability.
    """

    def __init__(self, headless: bool = config.HEADLESS_MODE, proxy: Optional[str] = None):
        """
        Initialize the browser with specified options.

        Args:
            headless: Whether to run in headless mode
            proxy: Optional proxy server to use
        """
        self.driver = self._setup_driver(headless, proxy)
        self.wait = WebDriverWait(self.driver, config.WEBDRIVER_WAIT_TIMEOUT)
        logger.info("Browser initialized")

    def _setup_driver(self, headless: bool, proxy: Optional[str]) -> webdriver.Chrome:
        """
        Set up and configure the Chrome WebDriver.

        Args:
            headless: Whether to run in headless mode
            proxy: Optional proxy server to use

        Returns:
            webdriver.Chrome: Configured Chrome WebDriver
        """
        options = Options()

        if headless:
            options.add_argument("--headless")

        # Anti-detection measures
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(f"user-agent={config.USER_AGENT}")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--start-maximized")

        # Add random window size to avoid fingerprinting
        width = random.randint(1050, 1200)
        height = random.randint(800, 950)
        options.add_argument(f"--window-size={width},{height}")

        # Add proxy if specified
        if proxy:
            options.add_argument(f'--proxy-server={proxy}')

        # Disable automation flags
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # Initialize the driver with the latest ChromeDriver that matches the installed Chrome version
        try:
            # First try to get the driver that matches the current Chrome version
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.warning(f"Error initializing ChromeDriver: {e}")
            logger.info("Trying alternative ChromeDriver installation method...")

            try:
                # Try a different approach with specific browser_version parameter
                logger.info("Trying with browser_version parameter...")
                service = Service(ChromeDriverManager(driver_version="latest").install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception as e2:
                logger.warning(f"Second method also failed: {e2}")
                logger.info("Trying to use Chrome directly without webdriver_manager...")

                # Final fallback: try to use Chrome directly
                driver = webdriver.Chrome(options=options)

        # Execute CDP commands to modify navigator.webdriver
        driver.execute_cdp_cmd(
            'Page.addScriptToEvaluateOnNewDocument',
            {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'}
        )

        return driver

    def navigate(self, url: str) -> bool:
        """
        Navigate to a URL with proper error handling.

        Args:
            url: URL to navigate to

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Navigating to: {url}")
            self.driver.get(url)
            return True
        except Exception as e:
            logger.error(f"Error navigating to {url}: {e}")
            return False

    def find_element(self, by: By, value: str, timeout: int = config.WEBDRIVER_WAIT_TIMEOUT) -> Optional[Any]:
        """
        Find an element with explicit wait.

        Args:
            by: Selenium By locator
            value: Locator value
            timeout: Wait timeout in seconds

        Returns:
            Optional[Any]: Element if found, None otherwise
        """
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except (TimeoutException, NoSuchElementException) as e:
            logger.warning(f"Element not found: {by}={value}, {e}")
            return None

    def find_elements(self, by: By, value: str, timeout: int = config.WEBDRIVER_WAIT_TIMEOUT) -> List[Any]:
        """
        Find elements with explicit wait.

        Args:
            by: Selenium By locator
            value: Locator value
            timeout: Wait timeout in seconds

        Returns:
            List[Any]: List of elements found (empty if none found)
        """
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return self.driver.find_elements(by, value)
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"No elements found: {by}={value}")
            return []

    def click_element(self, element: Any, scroll: bool = True) -> bool:
        """
        Click an element with proper error handling.

        Args:
            element: Element to click
            scroll: Whether to scroll to the element before clicking

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if scroll:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                random_delay(0.5, 1.5)

            element.click()
            return True
        except (ElementClickInterceptedException, StaleElementReferenceException) as e:
            logger.warning(f"Could not click element: {e}")
            try:
                # Try JavaScript click as fallback
                self.driver.execute_script("arguments[0].click();", element)
                return True
            except Exception as js_e:
                logger.error(f"JavaScript click also failed: {js_e}")
                return False
        except Exception as e:
            logger.error(f"Error clicking element: {e}")
            return False

    def get_page_source(self) -> str:
        """
        Get the current page source.

        Returns:
            str: Page source HTML
        """
        return self.driver.page_source

    def close(self) -> None:
        """
        Close the browser and release resources.
        """
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")

    def __enter__(self):
        """
        Context manager entry point.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point.
        """
        self.close()
