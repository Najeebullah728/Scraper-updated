"""
Stage 3: Scrape detailed information from individual listing pages.
"""
import argparse
import concurrent.futures
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm import tqdm

import config
from browser import Browser
from utils import logger, random_delay, retry_on_exception, save_to_file, get_progress_bar


class DetailScraper:
    """
    Scraper for collecting detailed information from individual Craigslist listings.
    """

    def __init__(
        self,
        input_file: Path = config.LINKS_CSV,
        output_dir: Path = config.MAIN_DATA_DIR,
        batch_size: int = 10,
        resume: bool = True
    ):
        """
        Initialize the detail scraper.

        Args:
            input_file: CSV file containing links to scrape
            output_dir: Directory to save scraped data
            batch_size: Number of links to process in each batch
            resume: Whether to resume from previous run
        """
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.resume = resume

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_links(self) -> pd.DataFrame:
        """
        Load links from CSV file.

        Returns:
            pd.DataFrame: DataFrame containing links
        """
        try:
            df = pd.read_csv(self.input_file)

            # Add metadata columns if they don't exist
            if "scraped" not in df.columns:
                df["scraped"] = False
            if "processed" not in df.columns:
                df["processed"] = False

            logger.info(f"Loaded {len(df)} links from {self.input_file}")
            return df

        except Exception as e:
            logger.error(f"Error loading links from {self.input_file}: {e}")
            return pd.DataFrame(columns=["link", "scraped", "processed"])

    def save_links(self, df: pd.DataFrame) -> bool:
        """
        Save links back to CSV file.

        Args:
            df: DataFrame containing links

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            df.to_csv(self.input_file, index=False)
            return True
        except Exception as e:
            logger.error(f"Error saving links to {self.input_file}: {e}")
            return False

    @retry_on_exception(max_retries=config.MAX_RETRIES, delay=config.RETRY_DELAY)
    def scrape_listing(self, link: str, idx: int) -> Tuple[bool, Optional[str]]:
        """
        Scrape a single listing and extract the phone number.

        Args:
            link: URL of the listing
            idx: Index of the listing

        Returns:
            Tuple[bool, Optional[str]]: Success status and extracted phone number
        """
        file_path = self.output_dir / f"listing_{idx}.html"
        phone_number = None

        # Skip if already scraped
        if file_path.exists() and self.resume:
            logger.info(f"Listing {idx} already scraped, skipping")
            return True, None

        with Browser() as browser:
            # Navigate to the listing page
            if not browser.navigate(link):
                return False, None

            # Wait for page to load
            random_delay(config.PAGE_LOAD_WAIT - 2, config.PAGE_LOAD_WAIT + 2)

            # Try to click the "Reply" button
            reply_button = browser.find_element(By.CSS_SELECTOR, "button.reply-button")
            if reply_button:
                if browser.click_element(reply_button):
                    logger.info(f"Clicked 'Reply' button for listing {idx}")

                    # Wait for 15 seconds after clicking Reply button
                    logger.info(f"Waiting {config.REPLY_BUTTON_WAIT} seconds after clicking Reply button...")
                    time.sleep(config.REPLY_BUTTON_WAIT)

                    # Try to click the "Call" button
                    call_button = browser.find_element(By.XPATH, "//button[contains(., 'call')]")
                    if call_button:
                        if browser.click_element(call_button):
                            logger.info(f"Clicked 'Call' button for listing {idx}")

                            # Wait for 10 seconds after clicking Call button
                            logger.info(f"Waiting {config.CALL_BUTTON_WAIT} seconds after clicking Call button...")
                            time.sleep(config.CALL_BUTTON_WAIT)

                            # Wait a moment for the phone number to appear
                            time.sleep(2)

                            # Extract phone number - specifically looking for the format like (714) 760-4016
                            # First try the most reliable selector
                            phone_element = browser.find_element(By.CSS_SELECTOR, ".reply-content-phone a[href^='tel:']")
                            if phone_element:
                                phone_text = phone_element.text.strip()
                                # Verify it's a proper phone number format (not a post ID)
                                import re
                                if re.match(r'\(\d{3}\)\s\d{3}-\d{4}', phone_text):
                                    phone_number = phone_text
                                    logger.info(f"Extracted phone number: {phone_number}")
                                else:
                                    logger.warning(f"Found element but not a valid phone number format: {phone_text}")
                                    phone_number = None

                            # If not found or not valid format, try alternative methods
                            if not phone_number:
                                # Try to find any phone number pattern in the page
                                page_source = browser.get_page_source()
                                phone_pattern = re.compile(r'\(\d{3}\)\s\d{3}-\d{4}')
                                phone_matches = phone_pattern.findall(page_source)
                                if phone_matches:
                                    phone_number = phone_matches[0]
                                    logger.info(f"Extracted phone number using regex: {phone_number}")
                                else:
                                    # Try a more lenient pattern as last resort
                                    phone_pattern = re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
                                    phone_matches = phone_pattern.findall(page_source)
                                    if phone_matches:
                                        # Verify it's not a post ID (post IDs are usually all digits without formatting)
                                        for match in phone_matches:
                                            if '(' in match or ')' in match or '-' in match or ' ' in match:
                                                phone_number = match
                                                logger.info(f"Extracted phone number using lenient regex: {phone_number}")
                                                break

                                    if not phone_number:
                                        logger.warning(f"Could not extract phone number for listing {idx}")

            # Get the page source
            html_content = browser.get_page_source()

            # Add phone number as a comment at the top of the HTML file
            if phone_number:
                html_content = f"<!-- PHONE_NUMBER: {phone_number} -->\n{html_content}"

            # Save to file
            if save_to_file(html_content, file_path):
                logger.info(f"Saved listing {idx} to {file_path}")
                return True, phone_number
            else:
                return False, None

    def scrape_batch(self, batch_df: pd.DataFrame, start_idx: int) -> pd.DataFrame:
        """
        Scrape a batch of listings.

        Args:
            batch_df: DataFrame containing links to scrape
            start_idx: Starting index for this batch

        Returns:
            pd.DataFrame: Updated DataFrame with scraped status and phone numbers
        """
        # Add phone_number column if it doesn't exist
        if "phone_number" not in batch_df.columns:
            batch_df["phone_number"] = None

        for i, (_, row) in enumerate(batch_df.iterrows()):
            idx = start_idx + i
            link = row["link"]

            logger.info(f"Scraping listing {idx}: {link}")
            success, phone_number = self.scrape_listing(link, idx)

            # Update scraped status and phone number
            batch_df.loc[batch_df["link"] == link, "scraped"] = success
            if phone_number:
                batch_df.loc[batch_df["link"] == link, "phone_number"] = phone_number

            # Random delay between listings
            if i < len(batch_df) - 1:  # No need to delay after the last one
                random_delay(
                    config.NEXT_LISTING_WAIT_MIN,
                    config.NEXT_LISTING_WAIT_MAX
                )

        return batch_df

    def scrape_all_listings(self) -> bool:
        """
        Scrape all listings.

        Returns:
            bool: True if successful, False otherwise
        """
        # Load links
        df = self.load_links()
        if df.empty:
            logger.error("No links to scrape")
            return False

        # Filter links that haven't been scraped yet
        if self.resume:
            to_scrape_df = df[~df["scraped"]]
            logger.info(f"{len(to_scrape_df)}/{len(df)} links need to be scraped")
        else:
            to_scrape_df = df
            logger.info(f"Scraping all {len(df)} links")

        if to_scrape_df.empty:
            logger.info("All links already scraped")
            return True

        # Process in batches
        total_batches = (len(to_scrape_df) + self.batch_size - 1) // self.batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = min(start_idx + self.batch_size, len(to_scrape_df))

            logger.info(f"Processing batch {batch_idx + 1}/{total_batches} (listings {start_idx + 1}-{end_idx})")

            # Get batch
            batch_df = to_scrape_df.iloc[start_idx:end_idx].copy()

            # Scrape batch
            updated_batch_df = self.scrape_batch(batch_df, start_idx + 1)

            # Update main DataFrame
            for _, row in updated_batch_df.iterrows():
                df.loc[df["link"] == row["link"], "scraped"] = row["scraped"]

            # Save progress
            self.save_links(df)

            # Show progress
            progress = get_progress_bar(end_idx, len(to_scrape_df))
            logger.info(f"Progress: {progress}")

        logger.info("All listings scraped")
        return True

    def scrape_with_parallel_processing(self, num_workers: int = 2) -> bool:
        """
        Scrape listings using parallel processing for better performance.

        Args:
            num_workers: Number of parallel workers

        Returns:
            bool: True if successful, False otherwise
        """
        # Load links
        df = self.load_links()
        if df.empty:
            logger.error("No links to scrape")
            return False

        # Add phone_number column if it doesn't exist
        if "phone_number" not in df.columns:
            df["phone_number"] = None

        # Filter links that haven't been scraped yet
        if self.resume:
            to_scrape_df = df[~df["scraped"]]
            logger.info(f"{len(to_scrape_df)}/{len(df)} links need to be scraped")
        else:
            to_scrape_df = df
            logger.info(f"Scraping all {len(df)} links")

        if to_scrape_df.empty:
            logger.info("All links already scraped")
            return True

        # Prepare tasks
        tasks = []
        for i, (_, row) in enumerate(to_scrape_df.iterrows()):
            tasks.append((row["link"], i + 1))

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_link = {
                executor.submit(self.scrape_listing, link, idx): link
                for link, idx in tasks
            }

            # Process results as they complete
            with tqdm(total=len(tasks), desc="Scraping listings") as pbar:
                for future in concurrent.futures.as_completed(future_to_link):
                    link = future_to_link[future]
                    try:
                        success, phone_number = future.result()
                        # Update scraped status and phone number
                        df.loc[df["link"] == link, "scraped"] = success
                        if phone_number:
                            df.loc[df["link"] == link, "phone_number"] = phone_number
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error scraping {link}: {e}")
                        df.loc[df["link"] == link, "scraped"] = False
                        pbar.update(1)

                    # Save progress periodically
                    if pbar.n % 5 == 0 or pbar.n == len(tasks):
                        self.save_links(df)

        logger.info("Parallel scraping complete")
        return True


def main():
    """
    Main function to run the detail scraper from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Detail Scraper - Stage 3")
    parser.add_argument("--input", default=str(config.LINKS_CSV), help="Input CSV file with links")
    parser.add_argument("--output", default=str(config.MAIN_DATA_DIR), help="Output directory for HTML files")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    parser.add_argument("--no-resume", action="store_true", help="Don't resume from previous run")
    parser.add_argument("--parallel", action="store_true", help="Use parallel processing")
    parser.add_argument("--workers", type=int, default=2, help="Number of parallel workers")

    args = parser.parse_args()

    scraper = DetailScraper(
        input_file=Path(args.input),
        output_dir=Path(args.output),
        batch_size=args.batch_size,
        resume=not args.no_resume
    )

    if args.parallel:
        logger.info(f"Using parallel processing with {args.workers} workers")
        success = scraper.scrape_with_parallel_processing(num_workers=args.workers)
    else:
        success = scraper.scrape_all_listings()

    if success:
        logger.info("Detail scraping completed successfully")
    else:
        logger.error("Detail scraping failed")


if __name__ == "__main__":
    main()
