"""
Test script to verify that the phone number filtering works correctly.
"""
import os
import pandas as pd
from pathlib import Path

from scraper_stage5 import DataFilter
from utils import logger

def create_test_data():
    """Create a test CSV file with various phone number formats."""
    # Create test directory
    test_dir = Path("test_data")
    test_dir.mkdir(exist_ok=True)
    
    # Create test data
    data = {
        "Title": [
            "Car 1", "Car 2", "Car 3", "Car 4", "Car 5", 
            "Car 6", "Car 7", "Car 8", "Car 9", "Car 10"
        ],
        "Price": [
            "$10,000", "$15,000", "$20,000", "$25,000", "$30,000",
            "$35,000", "$40,000", "$45,000", "$50,000", "$55,000"
        ],
        "phone_number": [
            "(714) 760-4016",  # Valid format
            "7147604016",      # No formatting
            "714-760-4016",    # Different format
            "N/A",             # Invalid
            "(800) 555-1234",  # Valid format
            "12345",           # Post ID (invalid)
            "(213) 456-7890",  # Valid format
            "NA",              # Invalid
            "(310) 123-4567",  # Valid format
            "ERROR"            # Invalid
        ],
        "Link": [
            "https://example.com/1",
            "https://example.com/2",
            "https://example.com/3",
            "https://example.com/4",
            "https://example.com/5",
            "https://example.com/6",
            "https://example.com/7",
            "https://example.com/8",
            "https://example.com/9",
            "https://example.com/10"
        ]
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    test_file = test_dir / "test_input.csv"
    df.to_csv(test_file, index=False)
    
    print(f"Created test file: {test_file}")
    print(f"Test data contains {len(df)} records with various phone number formats")
    
    return test_file

def run_test():
    """Run the filter on test data and verify results."""
    # Create test data
    input_file = create_test_data()
    output_file = Path("test_data") / "test_output.csv"
    
    # Run filter
    print("\nRunning phone number filter...")
    filter = DataFilter(
        input_file=input_file,
        output_file=output_file,
        phone_required=True
    )
    
    success = filter.run()
    
    if success:
        print("\nFilter completed successfully!")
        
        # Load and display results
        if output_file.exists():
            df = pd.read_csv(output_file)
            print(f"\nFiltered data contains {len(df)} records")
            print("\nFiltered phone numbers:")
            for i, row in df.iterrows():
                print(f"  {i+1}. {row['phone_number']}")
                
            # Verify all phone numbers are in correct format
            valid_format = df["phone_number"].str.match(r'^\(\d{3}\)\s\d{3}-\d{4}$')
            if valid_format.all():
                print("\n✅ SUCCESS: All phone numbers are in the correct format (XXX) XXX-XXXX")
            else:
                print("\n❌ ERROR: Some phone numbers are not in the correct format")
                invalid_rows = df[~valid_format]
                for i, row in invalid_rows.iterrows():
                    print(f"  - Invalid format: {row['phone_number']}")
        else:
            print(f"\n❌ ERROR: Output file not found: {output_file}")
    else:
        print("\n❌ ERROR: Filter failed")

if __name__ == "__main__":
    run_test()
