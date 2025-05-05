"""
Stage 5: Filter and clean the extracted data.
"""
import argparse
import os
import re
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

import config
from utils import logger


class DataFilter:
    """
    Filter and clean the extracted data.
    """

    def __init__(self, input_file: Path = config.OUTPUT_CSV, output_file: Path = config.FILTERED_CSV, phone_required: bool = True):
        """
        Initialize the data filter.

        Args:
            input_file: Input CSV file with extracted data
            output_file: Output CSV file for filtered data
            phone_required: Whether to require a valid phone number
        """
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.phone_required = phone_required
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def is_valid_phone(self, phone: str) -> bool:
        """
        Check if a phone number is valid.

        Args:
            phone: Phone number to check

        Returns:
            bool: True if valid, False otherwise
        """
        if not phone or pd.isna(phone) or phone.lower() == 'n/a':
            return False

        # Check if it's a post ID (numeric only)
        if re.match(r'^\d+$', str(phone)):
            return False

        # Check if it's in the format (XXX) XXX-XXXX
        if re.match(r'^\(\d{3}\) \d{3}-\d{4}$', str(phone)):
            return True

        return False

    def run(self) -> bool:
        """
        Run the data filtering process.

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("==================================================")
        logger.info("Starting data filtering process")
        logger.info("==================================================")
        logger.info(f"Input file: {self.input_file}")
        logger.info(f"Output file: {self.output_file}")
        logger.info(f"Phone number required: {self.phone_required}")

        # Check if input file exists
        if not self.input_file.exists():
            logger.error(f"Input file does not exist: {self.input_file}")
            return False

        try:
            # Read the input CSV
            df = pd.read_csv(self.input_file)
            original_count = len(df)
            logger.info(f"Read {original_count} records from {self.input_file}")

            # Filter out records without valid phone numbers if required
            if self.phone_required:
                df['valid_phone'] = df['phone_number'].apply(self.is_valid_phone)
                df = df[df['valid_phone']]
                df = df.drop(columns=['valid_phone'])

            # Remove duplicates based on phone number
            if 'phone_number' in df.columns:
                df = df.drop_duplicates(subset=['phone_number'])

            # Save the filtered data
            df.to_csv(self.output_file, index=False)
            filtered_count = len(df)
            
            logger.info(f"Filtered {original_count - filtered_count} records")
            logger.info(f"Saved {filtered_count} records to {self.output_file}")
            logger.info("==================================================")
            
            return True
            
        except Exception as e:
            logger.error(f"Error filtering data: {e}")
            return False


def main():
    """
    Main function to run the data filter from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Data Filter - Stage 5")
    parser.add_argument("--input", default=str(config.OUTPUT_CSV), help="Input CSV file with extracted data")
    parser.add_argument("--output", default=str(config.FILTERED_CSV), help="Output CSV file for filtered data")
    parser.add_argument("--no-phone-required", action="store_true", help="Don't require a valid phone number")
    
    args = parser.parse_args()
    
    data_filter = DataFilter(
        input_file=Path(args.input),
        output_file=Path(args.output),
        phone_required=not args.no_phone_required
    )
    
    success = data_filter.run()
    
    if success:
        logger.info("Data filtering completed successfully")
    else:
        logger.error("Data filtering failed")


if __name__ == "__main__":
    main()
