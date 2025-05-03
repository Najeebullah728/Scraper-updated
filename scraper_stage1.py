"""
Stage 1: Scrape Craigslist listings from search results pages.
"""
import argparse
import concurrent.futures
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from selenium.webdriver.common.by import By
from tqdm import tqdm

import config
from browser import Browser
from utils import logger, random_delay, retry_on_exception, save_to_file, get_progress_bar


class ListingScraper:
    """
    Scraper for collecting Craigslist listings from search results pages.
    """

    def __init__(self, base_url: str, output_dir: Path = config.DATA_DIR, max_pages: int = None):
        """
        Initialize the listing scraper.

        Args:
            base_url: Base URL for Craigslist search
            output_dir: Directory to save scraped data
            max_pages: Maximum number of pages to scrape (None for all)
        """
        self.base_url = base_url
        self.output_dir = Path(output_dir)
        self.max_pages = max_pages
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_counter = 1

    @retry_on_exception(max_retries=config.MAX_RETRIES, delay=config.RETRY_DELAY)
    def scrape_page(self, page_offset: int) -> Tuple[int, List[str]]:
        """
        Scrape a single page of listings.

        Args:
            page_offset: Page offset for pagination

        Returns:
            Tuple[int, List[str]]: Number of listings found and list of listing IDs
        """
        # Handle different URL formats (with # or with query parameters)
        if "#" in self.base_url:
            # For URLs with hash fragments like #search=2~gallery~0
            base_part, hash_part = self.base_url.split("#", 1)
            if "?" in base_part:
                url = f"{base_part}&s={page_offset}#{hash_part}"
            else:
                url = f"{base_part}?s={page_offset}#{hash_part}"
        else:
            # For traditional URLs with query parameters
            if "?" in self.base_url:
                url = f"{self.base_url}&s={page_offset}"
            else:
                url = f"{self.base_url}?s={page_offset}"

        listing_ids = []

        with Browser() as browser:
            browser.navigate(url)
            random_delay(1, 3)  # Initial wait for page load

            # Find all listing cards - try different selectors for different Craigslist layouts
            listings = browser.find_elements(By.CLASS_NAME, "gallery-card")

            # If no listings found with gallery-card, try other common selectors
            if not listings:
                listings = browser.find_elements(By.CSS_SELECTOR, ".result-row, .cl-static-search-result")

            if not listings:
                logger.info(f"No listings found on page with offset {page_offset}")
                return 0, []

            logger.info(f"Found {len(listings)} listings on page with offset {page_offset}")

            # Process each listing
            for listing in listings:
                try:
                    # Get listing ID for tracking
                    listing_id = listing.get_attribute("data-pid") or f"unknown_{self.file_counter}"
                    listing_ids.append(listing_id)

                    # Get HTML content
                    html_content = listing.get_attribute("outerHTML")

                    # Save to file
                    file_path = self.output_dir / f"craigslist_car_{self.file_counter}.html"
                    save_to_file(html_content, file_path)

                    self.file_counter += 1

                except Exception as e:
                    logger.error(f"Error processing listing: {e}")

        return len(listings), listing_ids

    def scrape_all_pages(self) -> int:
        """
        Scrape all pages of listings.

        Returns:
            int: Total number of listings scraped
        """
        page_offset = 0
        page_num = 1
        total_listings = 0

        while True:
            logger.info(f"Scraping page {page_num} (offset: {page_offset})")

            num_listings, listing_ids = self.scrape_page(page_offset)
            total_listings += num_listings

            if num_listings == 0:
                logger.info("No more listings found. Stopping.")
                break

            if self.max_pages and page_num >= self.max_pages:
                logger.info(f"Reached maximum number of pages ({self.max_pages}). Stopping.")
                break

            # Prepare for next page
            page_offset += 120  # Craigslist uses 120 items per page
            page_num += 1

            # Random delay between pages
            random_delay(
                config.NEXT_PAGE_WAIT_MIN,
                config.NEXT_PAGE_WAIT_MAX
            )

        logger.info(f"Scraping complete. Total listings: {total_listings}")
        return total_listings

    def scrape_with_parallel_processing(self, num_workers: int = 3) -> int:
        """
        Scrape listings using parallel processing for better performance.

        Args:
            num_workers: Number of parallel workers

        Returns:
            int: Total number of listings scraped
        """
        # First, determine how many pages we need to scrape
        with Browser() as browser:
            browser.navigate(self.base_url)

            # Try to find total count element
            total_count_elem = browser.find_element(By.CLASS_NAME, "totalcount")
            if total_count_elem:
                try:
                    total_count = int(total_count_elem.text.strip())
                    total_pages = (total_count // 120) + (1 if total_count % 120 > 0 else 0)
                except (ValueError, AttributeError):
                    logger.warning("Could not parse total count, using default")
                    total_pages = 10  # Default if we can't determine
            else:
                total_pages = 10  # Default if we can't find the element

        if self.max_pages and self.max_pages < total_pages:
            total_pages = self.max_pages

        logger.info(f"Planning to scrape {total_pages} pages")

        # Generate page offsets
        page_offsets = [i * 120 for i in range(total_pages)]

        total_listings = 0

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_offset = {
                executor.submit(self.scrape_page, offset): offset for offset in page_offsets
            }

            # Process results as they complete
            with tqdm(total=len(page_offsets), desc="Scraping pages") as pbar:
                for future in concurrent.futures.as_completed(future_to_offset):
                    offset = future_to_offset[future]
                    try:
                        num_listings, _ = future.result()
                        total_listings += num_listings
                        pbar.update(1)
                        pbar.set_postfix({"listings": total_listings})
                    except Exception as e:
                        logger.error(f"Error scraping page with offset {offset}: {e}")

        logger.info(f"Parallel scraping complete. Total listings: {total_listings}")
        return total_listings


def main():
    """
    Main function to run the scraper from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Listing Scraper - Stage 1")
    parser.add_argument("url", help="Base Craigslist search URL")
    parser.add_argument("--output", default=str(config.DATA_DIR), help="Output directory for HTML files")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages to scrape")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers")

    args = parser.parse_args()

    logger.info(f"Starting scraper with URL: {args.url}")

    scraper = ListingScraper(
        base_url=args.url,
        output_dir=Path(args.output),
        max_pages=args.max_pages
    )

    if args.parallel:
        logger.info(f"Using parallel processing with {args.workers} workers")
        scraper.scrape_with_parallel_processing(num_workers=args.workers)
    else:
        scraper.scrape_all_pages()

    logger.info("Scraping complete!")


if __name__ == "__main__":
    main()
