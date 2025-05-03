# Optimized Craigslist Phone Number Scraper

An optimized, robust web scraper for extracting phone numbers from Craigslist car listings.

## Features

- **Multi-stage Pipeline**: Modular design with five specialized stages
- **Parallel Processing**: Significantly faster scraping with multi-threading
- **Ctrl+C Stage Skipping**: Press Ctrl+C to skip to the next stage
- **Phone Number Extraction**: Extracts phone numbers in format (XXX) XXX-XXXX
- **Robust Error Handling**: Automatic retries and comprehensive error recovery
- **Resumable Operations**: Can continue from where it left off if interrupted
- **Anti-Detection Measures**: Advanced techniques to avoid being blocked

## Quick Start

1. Double-click one of these batch files:
   - `run_scraper_with_input.bat` - Standard version that asks for a URL
   - `run_with_ctrl_c_skip.bat` - Version that allows skipping stages with Ctrl+C

2. When prompted, enter a Craigslist URL, for example:
   - `https://losangeles.craigslist.org/search/cta#search=2~gallery~0`

3. The scraper will:
   - Visit Craigslist car listings
   - Click the "Reply" button and wait 15 seconds
   - Click the "Call" button and wait 10 seconds
   - Extract phone numbers in format (XXX) XXX-XXXX

4. Results will be saved in:
   - `output/output_data.csv` - Raw data including all listings
   - `output/filtered_phone_numbers.csv` - Only listings with valid phone numbers

## Special Feature: Skip to Next Stage

When using `run_with_ctrl_c_skip.bat`, you can press Ctrl+C at any time to skip to the next stage:
- Press Ctrl+C during Stage 1 to skip to Stage 2
- Press Ctrl+C during Stage 2 to skip to Stage 3
- And so on...

This is useful if a stage is taking too long or if you want to test a specific stage.

## File Structure

### Main Scripts
- `run_scraper_with_input.bat` - Batch file to run the scraper with URL input
- `run_with_ctrl_c_skip.bat` - Batch file to run the scraper with stage skipping
- `start_scraper_with_input.py` - Python script that handles user input and runs the scraper
- `run_scraper.py` - Core script that runs the scraping pipeline

### Pipeline Stages
- `scraper_stage1.py` - Scrapes listing cards from search results pages
- `scraper_stage2.py` - Extracts links from the scraped listing cards
- `scraper_stage3.py` - Visits each link and scrapes detailed information
- `scraper_stage4.py` - Extracts structured data from the detailed pages
- `scraper_stage5.py` - Filters and cleans the extracted data

### Support Files
- `browser.py` - Browser automation module
- `config.py` - Configuration settings
- `utils.py` - Utility functions
- `requirements.txt` - Required Python packages

### Test Scripts
- `test_scraper.py` - Tests the browser and directory structure
- `test_filter.py` - Tests the phone number filtering

## Configuration

You can customize the scraper's behavior by modifying `config.py`. Key settings include:
- Wait times after clicking buttons
- Browser settings
- File paths and directories
- Retry settings

## For More Information

See `USER_GUIDE.md` for detailed instructions on using the scraper.

## Disclaimer

Web scraping may be against the terms of service of some websites. Use this tool responsibly and at your own risk.
