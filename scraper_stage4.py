"""
Stage 4: Extract only title and phone number from HTML files and save to a text file.
Skip any entries without a valid phone number.
"""
import argparse
import concurrent.futures
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

import config
from utils import logger, save_to_file, get_progress_bar


class DataExtractor:
    """
    Extract title and phone number from HTML files scraped in Stage 3.
    Skip any entries without a valid phone number.
    """

    def __init__(
        self,
        input_dir: Path = config.MAIN_DATA_DIR,
        links_file: Path = config.LINKS_CSV,
        output_file: Path = Path("output/output_data.txt")
    ):
        """
        Initialize the extractor.

        Args:
            input_dir: Directory containing HTML files
            links_file: CSV file containing links
            output_file: Output text file for extracted data
        """
        self.input_dir = Path(input_dir)
        self.links_file = Path(links_file)
        self.output_file = Path(output_file)

        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_links(self) -> pd.DataFrame:
        """
        Load links and phone numbers from CSV file.

        Returns:
            pd.DataFrame: DataFrame containing links and phone numbers
        """
        try:
            df = pd.read_csv(self.links_file)
            logger.info(f"Loaded {len(df)} links from {self.links_file}")

            # Check if phone_number column exists
            if "phone_number" in df.columns:
                logger.info(f"Found {df['phone_number'].notna().sum()} phone numbers in CSV")

            return df
        except Exception as e:
            logger.error(f"Error loading links from {self.links_file}: {e}")
            return pd.DataFrame(columns=["link"])

    def extract_data_from_html(self, html_file: Path) -> Dict[str, str]:
        """
        Extract title and phone number from a single HTML file.

        Args:
            html_file: Path to HTML file

        Returns:
            Dict[str, str]: Extracted title and phone number
        """
        try:
            with open(html_file, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            # Initialize data dictionary
            data = {
                "title": "N/A",
                "phone_number": "N/A"
            }

            # Extract title
            title_tag = soup.find("span", {"id": "titletextonly"})
            if title_tag:
                data["title"] = title_tag.text.strip()

            # First, try to extract phone number from HTML comment
            html_str = str(soup)
            phone_comment_match = re.search(r'<!-- PHONE_NUMBER: ([\d\(\)\-\.\s]+) -->', html_str)
            if phone_comment_match:
                phone_text = phone_comment_match.group(1).strip()
                # Verify it's a proper phone number format (not a post ID)
                if re.match(r'\(\d{3}\)\s\d{3}-\d{4}', phone_text):
                    data["phone_number"] = phone_text
                    logger.info(f"Extracted valid phone number from comment: {data['phone_number']}")
                else:
                    logger.warning(f"Found phone number in comment but not valid format: {phone_text}")
            else:
                # If not found in comment, try to extract from the page content
                phone_tag = soup.select_one(".reply-content-phone a[href^='tel:']")
                if phone_tag:
                    phone_text = phone_tag.text.strip()
                    # Verify it's a proper phone number format
                    if re.match(r'\(\d{3}\)\s\d{3}-\d{4}', phone_text):
                        data["phone_number"] = phone_text
                        logger.info(f"Extracted valid phone number from page: {data['phone_number']}")
                    else:
                        logger.warning(f"Found phone element but not valid format: {phone_text}")
                else:
                    # Try to find any phone number pattern in the page
                    phone_pattern = re.compile(r'\(\d{3}\)\s\d{3}-\d{4}')
                    phone_matches = phone_pattern.findall(html_str)
                    if phone_matches:
                        data["phone_number"] = phone_matches[0]
                        logger.info(f"Extracted phone number using regex: {data['phone_number']}")
                    else:
                        # Try a more lenient pattern as last resort
                        phone_pattern = re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
                        phone_matches = phone_pattern.findall(html_str)
                        if phone_matches:
                            # Verify it's not a post ID (post IDs are usually all digits without formatting)
                            valid_phone = None
                            for match in phone_matches:
                                if '(' in match or ')' in match or '-' in match or ' ' in match:
                                    valid_phone = match
                                    break

                            if valid_phone:
                                data["phone_number"] = valid_phone
                                logger.info(f"Extracted phone number using lenient regex: {data['phone_number']}")

            return data

        except Exception as e:
            logger.error(f"Error processing {html_file}: {e}")
            return {
                "title": "ERROR",
                "phone_number": "N/A"
            }

    def extract_data_parallel(self, num_workers: int = 4) -> List[Dict[str, str]]:
        """
        Extract data using parallel processing for better performance.

        Args:
            num_workers: Number of parallel workers

        Returns:
            List[Dict[str, str]]: List of extracted data
        """
        # Load links and phone numbers
        links_df = self.load_links()

        # Prepare tasks
        tasks = []
        for idx in range(1, len(links_df) + 1):
            html_file = self.input_dir / f"listing_{idx}.html"
            if html_file.exists():
                # Include phone number in the task if available
                row = links_df.iloc[idx-1]
                phone_number = row.get("phone_number") if "phone_number" in row and pd.notna(row["phone_number"]) else None
                tasks.append((html_file, idx, row["link"], phone_number))

        if not tasks:
            logger.error(f"No HTML files found in {self.input_dir}")
            return []

        logger.info(f"Found {len(tasks)} HTML files to process")
        extracted_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.extract_data_from_html, file_path): (idx, link, phone_number)
                for file_path, idx, link, phone_number in tasks
            }

            # Process results as they complete
            with tqdm(total=len(tasks), desc="Extracting data") as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    idx, link, phone_number = future_to_file[future]
                    try:
                        data = future.result()

                        # Use phone number from CSV if available (higher priority)
                        if phone_number:
                            logger.info(f"Using phone number from CSV for listing {idx}: {phone_number}")
                            data["phone_number"] = phone_number

                        # Only include entries with a valid phone number
                        if data["phone_number"] != "N/A":
                            extracted_data.append(data)

                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing listing {idx}: {e}")
                        pbar.update(1)

        return extracted_data

    def save_data_to_txt(self, data: List[Dict[str, str]]) -> bool:
        """
        Save extracted data to text file.

        Args:
            data: List of extracted data

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(f"{item['title']}\n")
                    f.write("\n")
                    f.write(f"{item['phone_number']}\n")
                    f.write("\n")

            logger.info(f"Saved {len(data)} records to {self.output_file}")
            return True

        except Exception as e:
            logger.error(f"Error saving data to text file: {e}")
            return False

    def run(self, parallel: bool = True, num_workers: int = 4) -> bool:
        """
        Run the data extraction process.

        Args:
            parallel: Whether to use parallel processing
            num_workers: Number of parallel workers

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Starting title and phone number extraction")

        data = self.extract_data_parallel(num_workers) if parallel else []

        if not data:
            logger.error("No data extracted or no entries with valid phone numbers")
            return False

        logger.info(f"Extracted data from {len(data)} listings with valid phone numbers")
        return self.save_data_to_txt(data)


def main():
    """
    Main function to run the extractor from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Data Extractor - Stage 4")
    parser.add_argument("--input", default=str(config.MAIN_DATA_DIR), help="Input directory with HTML files")
    parser.add_argument("--links", default=str(config.LINKS_CSV), help="CSV file with links")
    parser.add_argument("--output", default="output/output_data.txt", help="Output text file for extracted data")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel processing")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")

    args = parser.parse_args()

    extractor = DataExtractor(
        input_dir=Path(args.input),
        links_file=Path(args.links),
        output_file=Path(args.output)
    )

    success = extractor.run(
        parallel=not args.no_parallel,
        num_workers=args.workers
    )

    if success:
        logger.info("Title and phone number extraction completed successfully")
    else:
        logger.error("Title and phone number extraction failed")


if __name__ == "__main__":
    main()
