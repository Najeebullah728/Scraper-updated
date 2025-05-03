"""
Start the Craigslist scraper by asking the user for a URL.
"""
import sys
import time
from pathlib import Path

import config
from utils import logger
from run_scraper import run_pipeline

def validate_url(url):
    """
    Validate that the URL is a Craigslist URL.

    Args:
        url: URL to validate

    Returns:
        str or False: Validated URL if valid, False otherwise
    """
    if not url.strip():
        print("Error: URL cannot be empty.")
        return False

    # Add https:// if missing
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
        print(f"Added https:// to URL: {url}")

    # Check if it's a Craigslist URL
    if "craigslist" not in url.lower():
        print("Warning: URL does not appear to be a Craigslist URL.")
        confirm = input("Do you want to continue anyway? (y/n): ").strip().lower()
        if confirm != 'y':
            return False

    # Check if it's a search URL
    if "/search/" not in url.lower():
        print("Warning: URL does not appear to be a search page.")
        print("The URL should contain '/search/' like: https://losangeles.craigslist.org/search/cta")
        confirm = input("Do you want to continue anyway? (y/n): ").strip().lower()
        if confirm != 'y':
            return False

    return url

def start_scraper():
    """
    Ask the user for a Craigslist URL and start the scraper.
    """
    print("\n" + "=" * 70)
    print("Craigslist Scraper - Phone Number Extractor".center(70))
    print("=" * 70)
    print("\nThis scraper will:")
    print("1. Visit Craigslist car listings")
    print("2. Click the 'Reply' button and wait 15 seconds")
    print("3. Click the 'Call' button and wait 10 seconds")
    print("4. Extract the phone number in format (XXX) XXX-XXXX")

    print("\nExample URLs:")
    print("- Los Angeles: https://losangeles.craigslist.org/search/cta#search=2~gallery~0")
    print("- New York:    https://newyork.craigslist.org/search/cta#search=2~gallery~0")
    print("- Chicago:     https://chicago.craigslist.org/search/cta#search=2~gallery~0")
    print("- For Toyota:  https://losangeles.craigslist.org/search/cta?query=toyota#search=2~gallery~0")

    print("\n" + "-" * 70)

    # Ask for URL
    url = input("\nEnter Craigslist URL to scrape: ").strip()

    # Validate URL
    valid_url = validate_url(url)
    if not valid_url:
        print("Exiting scraper due to invalid URL.")
        return

    # Confirm with user
    print(f"\nYou entered: {valid_url}")
    confirm = input("Start scraping with this URL? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Scraping cancelled.")
        return

    print("\n" + "=" * 70)
    print("Starting scraper...".center(70))
    print("=" * 70)
    print(f"\nURL: {valid_url}")
    print("The scraper will extract ONLY REAL PHONE NUMBERS in the format (XXX) XXX-XXXX")
    print("Post IDs and other non-phone number formats will be marked as N/A")
    print("\nSPECIAL FEATURE: Press Ctrl+C to skip to the next stage!")
    print("- Press Ctrl+C during Stage 1 to skip to Stage 2")
    print("- Press Ctrl+C during Stage 2 to skip to Stage 3")
    print("- And so on...")
    print("\n" + "-" * 70 + "\n")

    # Set up worker counts for optimal performance
    workers = {
        1: 3,  # Stage 1: Listing scraper
        2: 4,  # Stage 2: Link extractor
        3: 2,  # Stage 3: Detail scraper
        4: 4,  # Stage 4: Data extractor
        5: 1   # Stage 5: Data filter (single-threaded)
    }

    try:
        # Run the pipeline with all stages
        run_pipeline(
            base_url=valid_url,
            stages=[1, 2, 3, 4, 5],
            parallel=True,
            max_pages=None,  # No limit on pages
            workers=workers
        )

        # Show where to find the results
        print("\n" + "=" * 70)
        print("Scraping complete!".center(70))
        print("=" * 70)
        print("\nResults saved to:")
        print(f"- Raw data: {config.OUTPUT_CSV}")
        print(f"- Filtered data: {config.FILTERED_CSV}")
        print("\nThank you for using the Craigslist Scraper!")

    except KeyboardInterrupt:
        # This should not be triggered since KeyboardInterrupt is handled in run_pipeline
        # But just in case it bubbles up
        print("\n\nScraping interrupted by user at the main level.")
        print("Partial results may be available in the output directory.")
        print("\nResults may be found in:")
        print(f"- Raw data: {config.OUTPUT_CSV}")
        print(f"- Filtered data: {config.FILTERED_CSV}")
    except Exception as e:
        print(f"\n\nAn error occurred during scraping: {e}")
        print("Please check the log file for details.")
        print("\nPartial results may be found in:")
        print(f"- Raw data: {config.OUTPUT_CSV}")
        print(f"- Filtered data: {config.FILTERED_CSV}")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    start_scraper()
