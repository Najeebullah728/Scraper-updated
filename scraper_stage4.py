"""
Stage 4: Extract structured data from detailed listing pages.
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
    Extract structured data from HTML files scraped in Stage 3.
    """

    def __init__(
        self,
        input_dir: Path = config.MAIN_DATA_DIR,
        links_file: Path = config.LINKS_CSV,
        output_file: Path = config.OUTPUT_CSV
    ):
        """
        Initialize the data extractor.

        Args:
            input_dir: Directory containing HTML files
            links_file: CSV file containing links
            output_file: Output CSV file for extracted data
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

    def extract_data_from_html(self, html_file: Path) -> Dict[str, Any]:
        """
        Extract structured data from a single HTML file.

        Args:
            html_file: Path to HTML file

        Returns:
            Dict[str, Any]: Extracted data
        """
        try:
            with open(html_file, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            # Initialize data dictionary
            data = {
                "title": "N/A",
                "price": "N/A",
                "phone_number": "N/A",
                "location": "N/A",
                "post_date": "N/A",
                "attributes": {}
            }

            # Extract title
            title_tag = soup.find("span", {"id": "titletextonly"})
            if title_tag:
                data["title"] = title_tag.text.strip()

            # Extract price
            price_tag = soup.find("span", {"class": "price"})
            if price_tag:
                data["price"] = price_tag.text.strip()

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
                    data["phone_number"] = "N/A"
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
                        data["phone_number"] = "N/A"
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
                            else:
                                logger.warning("No valid phone number format found")
                                data["phone_number"] = "N/A"
                        else:
                            logger.warning("No phone number found")
                            data["phone_number"] = "N/A"

            # Extract location
            location_tag = soup.find("span", {"class": "postingtitletext"})
            if location_tag:
                location_span = location_tag.find("small")
                if location_span:
                    data["location"] = location_span.text.strip(" ()")

            # Extract post date
            date_tag = soup.find("time", {"class": "date timeago"})
            if date_tag:
                data["post_date"] = date_tag.get("datetime", "N/A")

            # Description extraction removed as requested

            # Extract attributes
            attrs_group = soup.find("p", {"class": "attrgroup"})
            if attrs_group:
                for span in attrs_group.find_all("span"):
                    if ":" in span.text:
                        key, value = span.text.split(":", 1)
                        data["attributes"][key.strip()] = value.strip()
                    else:
                        data["attributes"]["feature"] = span.text.strip()

            return data

        except Exception as e:
            logger.error(f"Error processing {html_file}: {e}")
            return {
                "title": "ERROR",
                "price": "ERROR",
                "phone_number": "ERROR",
                "location": "ERROR",
                "post_date": "ERROR",
                "attributes": {}
            }

    def extract_all_data(self) -> List[Dict[str, Any]]:
        """
        Extract data from all HTML files in the input directory.

        Returns:
            List[Dict[str, Any]]: List of extracted data
        """
        # Load links and phone numbers
        links_df = self.load_links()

        extracted_data = []

        for idx, row in enumerate(links_df.iterrows(), start=1):
            html_file = self.input_dir / f"listing_{idx}.html"
            row_data = row[1]  # Get the row data

            if html_file.exists():
                data = self.extract_data_from_html(html_file)

                # Add link from DataFrame
                data["link"] = row_data["link"]

                # Use phone number from CSV if available (higher priority)
                if "phone_number" in row_data and pd.notna(row_data["phone_number"]):
                    logger.info(f"Using phone number from CSV: {row_data['phone_number']}")
                    data["phone_number"] = row_data["phone_number"]

                extracted_data.append(data)

                # Show progress
                if idx % 10 == 0 or idx == len(links_df):
                    progress = get_progress_bar(idx, len(links_df))
                    logger.info(f"Progress: {progress}")
            else:
                logger.warning(f"File not found: {html_file}")

        return extracted_data

    def extract_data_parallel(self, num_workers: int = 4) -> List[Dict[str, Any]]:
        """
        Extract data using parallel processing for better performance.

        Args:
            num_workers: Number of parallel workers

        Returns:
            List[Dict[str, Any]]: List of extracted data
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
                        data["link"] = link

                        # Use phone number from CSV if available (higher priority)
                        if phone_number:
                            logger.info(f"Using phone number from CSV for listing {idx}: {phone_number}")
                            data["phone_number"] = phone_number

                        extracted_data.append(data)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing listing {idx}: {e}")
                        pbar.update(1)

        return extracted_data

    def save_data_to_csv(self, data: List[Dict[str, Any]]) -> bool:
        """
        Save extracted data to CSV file.

        Args:
            data: List of extracted data

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Flatten attributes if needed
            if "attributes" in df.columns:
                # Extract common attributes
                common_attrs = set()
                for attrs in df["attributes"]:
                    if isinstance(attrs, dict):
                        common_attrs.update(attrs.keys())

                # Add attribute columns
                for attr in common_attrs:
                    df[f"attr_{attr}"] = df["attributes"].apply(
                        lambda x: x.get(attr, "N/A") if isinstance(x, dict) else "N/A"
                    )

                # Drop attributes column
                df = df.drop(columns=["attributes"])

            # Save to CSV
            df.to_csv(self.output_file, index=False)
            logger.info(f"Saved {len(df)} records to {self.output_file}")
            return True

        except Exception as e:
            logger.error(f"Error saving data to CSV: {e}")
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
        logger.info("Starting data extraction")

        if parallel:
            data = self.extract_data_parallel(num_workers)
        else:
            data = self.extract_all_data()

        if not data:
            logger.error("No data extracted")
            return False

        logger.info(f"Extracted data from {len(data)} listings")
        return self.save_data_to_csv(data)


def main():
    """
    Main function to run the data extractor from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Data Extractor - Stage 4")
    parser.add_argument("--input", default=str(config.MAIN_DATA_DIR), help="Input directory with HTML files")
    parser.add_argument("--links", default=str(config.LINKS_CSV), help="CSV file with links")
    parser.add_argument("--output", default=str(config.OUTPUT_CSV), help="Output CSV file for extracted data")
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
        logger.info("Data extraction completed successfully")
    else:
        logger.error("Data extraction failed")


if __name__ == "__main__":
    main()
