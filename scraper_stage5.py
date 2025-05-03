"""
Stage 5: Filter and clean extracted data.
"""
import argparse
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

import config
from utils import logger


class DataFilter:
    """
    Filter and clean data extracted in Stage 4.
    """

    def __init__(
        self,
        input_file: Path = config.OUTPUT_CSV,
        output_file: Path = config.FILTERED_CSV,
        phone_required: bool = True
    ):
        """
        Initialize the data filter.

        Args:
            input_file: Input CSV file with extracted data
            output_file: Output CSV file for filtered data
            phone_required: Whether to require valid phone numbers
        """
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.phone_required = phone_required

        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_data(self) -> pd.DataFrame:
        """
        Load data from CSV file.

        Returns:
            pd.DataFrame: DataFrame containing data
        """
        try:
            df = pd.read_csv(self.input_file)
            logger.info(f"Loaded {len(df)} records from {self.input_file}")
            return df
        except Exception as e:
            logger.error(f"Error loading data from {self.input_file}: {e}")
            return pd.DataFrame()

    def clean_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize phone numbers.

        Args:
            df: DataFrame containing data

        Returns:
            pd.DataFrame: DataFrame with cleaned phone numbers
        """
        if "phone_number" not in df.columns and "Phone Number" not in df.columns:
            logger.warning("No phone number column found in the CSV")
            return df

        # Handle different column name conventions
        phone_col = "phone_number" if "phone_number" in df.columns else "Phone Number"

        # Make a copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Convert to string and strip whitespace
        df[phone_col] = df[phone_col].astype(str).str.strip()

        # Define a set of invalid phone number entries (case-insensitive)
        invalid_entries = {"N/A", "NA", "", "NULL", "ERROR", "NONE", "NAN"}

        # Create a mask for potentially valid phone numbers (not in invalid_entries)
        potential_valid_mask = ~df[phone_col].str.upper().isin(invalid_entries)

        # Log the number of potential valid entries
        logger.info(f"Found {potential_valid_mask.sum()} entries that are not marked as invalid")

        # Function to validate and standardize phone numbers
        def validate_and_standardize_phone(phone: str) -> str:
            """Validate and standardize phone numbers to (XXX) XXX-XXXX format."""
            if not isinstance(phone, str) or phone.upper() in invalid_entries:
                return "N/A"

            # Check if already in correct format
            if re.match(r'^\(\d{3}\)\s\d{3}-\d{4}$', phone):
                logger.debug(f"Phone number already in correct format: {phone}")
                return phone

            # Remove all non-digit characters
            digits = re.sub(r'\D', '', phone)

            # Check if we have a valid number of digits
            if len(digits) == 10:
                formatted = f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
                logger.debug(f"Standardized 10-digit number: {phone} -> {formatted}")
                return formatted
            elif len(digits) == 11 and digits[0] == '1':
                formatted = f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
                logger.debug(f"Standardized 11-digit number: {phone} -> {formatted}")
                return formatted
            else:
                logger.debug(f"Invalid phone number format: {phone} (digits: {digits})")
                return "N/A"

        # Apply validation and standardization
        df[phone_col] = df[phone_col].apply(validate_and_standardize_phone)

        # Create a mask for valid phone numbers in the correct format
        valid_format_mask = df[phone_col].str.match(r'^\(\d{3}\)\s\d{3}-\d{4}$')

        # Log the number of valid formatted entries
        logger.info(f"Found {valid_format_mask.sum()} phone numbers in valid (XXX) XXX-XXXX format")

        # Ensure column name is consistent
        if phone_col != "phone_number":
            df["phone_number"] = df[phone_col]

        return df

    def clean_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize prices.

        Args:
            df: DataFrame containing data

        Returns:
            pd.DataFrame: DataFrame with cleaned prices
        """
        if "price" not in df.columns:
            logger.warning("Column 'price' not found in the CSV")
            return df

        # Convert to string and strip whitespace
        df["price"] = df["price"].astype(str).str.strip()

        # Extract numeric values from price strings
        def extract_price(price_str: str) -> Optional[float]:
            if not isinstance(price_str, str) or price_str.upper() in {"N/A", "NA", "", "NULL", "ERROR"}:
                return None

            # Extract digits and decimal point
            match = re.search(r'(\d+(?:\.\d+)?)', price_str)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    return None
            return None

        # Add numeric price column
        df["price_numeric"] = df["price"].apply(extract_price)

        return df

    def filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter data based on criteria.

        Args:
            df: DataFrame containing data

        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        if df.empty:
            return df

        original_count = len(df)
        current_count = original_count

        # First, remove duplicates to avoid losing valid phone numbers
        if "link" in df.columns:
            df = df.drop_duplicates(subset=["link"])
            logger.info(f"Removed duplicates: {original_count} -> {len(df)} records")
            current_count = len(df)

        # Filter by phone number if required
        if self.phone_required:
            # Check for both column name conventions
            phone_col = None
            if "phone_number" in df.columns:
                phone_col = "phone_number"
            elif "Phone Number" in df.columns:
                phone_col = "Phone Number"

            if phone_col:
                # Only keep entries with valid phone number format (XXX) XXX-XXXX
                valid_format_mask = df[phone_col].str.match(r'^\(\d{3}\)\s\d{3}-\d{4}$')
                df = df[valid_format_mask]
                logger.info(f"Filtered by valid phone number format: {current_count} -> {len(df)} records")

                # Log some examples of valid phone numbers
                if not df.empty:
                    sample_size = min(5, len(df))
                    sample_phones = df[phone_col].sample(sample_size).tolist()
                    logger.info(f"Sample valid phone numbers: {sample_phones}")
            else:
                logger.warning("No phone number column found, skipping phone number filtering")

        return df

    def run(self) -> bool:
        """
        Run the data filtering process.

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("=" * 50)
        logger.info("Starting data filtering process")
        logger.info("=" * 50)
        logger.info(f"Input file: {self.input_file}")
        logger.info(f"Output file: {self.output_file}")
        logger.info(f"Phone number required: {self.phone_required}")

        # Check if input file exists
        if not self.input_file.exists():
            logger.error(f"Input file does not exist: {self.input_file}")
            return False

        # Load data
        df = self.load_data()
        if df.empty:
            logger.error("No data to filter or empty CSV file")
            return False

        # Log column names
        logger.info(f"CSV columns: {', '.join(df.columns.tolist())}")

        # Clean data
        logger.info("Cleaning and standardizing phone numbers...")
        df = self.clean_phone_numbers(df)

        logger.info("Cleaning and standardizing prices...")
        df = self.clean_prices(df)

        # Filter data
        logger.info("Filtering data based on criteria...")
        filtered_df = self.filter_data(df)

        if filtered_df.empty:
            logger.warning("No records left after filtering!")
            logger.warning("This could be because no valid phone numbers were found in the format (XXX) XXX-XXXX")

            # Create an empty file anyway
            filtered_df.to_csv(self.output_file, index=False)
            logger.info(f"Saved empty CSV to {self.output_file}")
            return True

        # Save filtered data
        try:
            # Make sure the output directory exists
            self.output_file.parent.mkdir(parents=True, exist_ok=True)

            # Save to CSV
            filtered_df.to_csv(self.output_file, index=False)

            # Log success message with details
            logger.info("=" * 50)
            logger.info(f"Successfully filtered data:")
            logger.info(f"- Original records: {len(df)}")
            logger.info(f"- Filtered records: {len(filtered_df)}")
            logger.info(f"- Records removed: {len(df) - len(filtered_df)}")
            logger.info(f"- Saved to: {self.output_file}")
            logger.info("=" * 50)

            return True
        except Exception as e:
            logger.error(f"Error saving filtered data to {self.output_file}: {e}")
            return False


def main():
    """
    Main function to run the data filter from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Data Filter - Stage 5")
    parser.add_argument("--input", default=str(config.OUTPUT_CSV), help="Input CSV file with extracted data")
    parser.add_argument("--output", default=str(config.FILTERED_CSV), help="Output CSV file for filtered data")
    parser.add_argument("--no-phone-required", action="store_true", help="Don't require valid phone numbers")

    args = parser.parse_args()

    filter = DataFilter(
        input_file=Path(args.input),
        output_file=Path(args.output),
        phone_required=not args.no_phone_required
    )

    success = filter.run()

    if success:
        logger.info("Data filtering completed successfully")
    else:
        logger.error("Data filtering failed")


if __name__ == "__main__":
    main()
