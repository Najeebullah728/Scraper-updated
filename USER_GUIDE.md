# Craigslist Scraper - User Guide

This guide will help you use the Craigslist scraper to extract phone numbers from car listings.

## Quick Start

1. Double-click the `run_scraper_with_input.bat` file
2. When prompted, enter a Craigslist URL (e.g., `https://losangeles.craigslist.org/search/cta#search=2~gallery~0`)
3. Confirm to start the scraping process
4. Wait for the scraper to complete
5. Find the results in the `output` folder

## Special Feature: Skip to Next Stage

You can use the `run_with_ctrl_c_skip.bat` file to run a version of the scraper that allows you to skip to the next stage by pressing Ctrl+C:

1. Double-click the `run_with_ctrl_c_skip.bat` file
2. When prompted, enter a Craigslist URL
3. During scraping, press Ctrl+C at any time to skip to the next stage:
   - Press Ctrl+C during Stage 1 to skip to Stage 2
   - Press Ctrl+C during Stage 2 to skip to Stage 3
   - And so on...

This is useful if:
- A stage is taking too long
- You've already completed some stages and want to continue from a later stage
- You want to test a specific stage without running the earlier stages completely

## What This Scraper Does

This scraper will:
1. Visit Craigslist car listings from the URL you provide
2. Click the "Reply" button and wait 15 seconds
3. Click the "Call" button and wait 10 seconds
4. Extract the phone number in format (XXX) XXX-XXXX
5. Save the results to CSV files

## Example URLs

Here are some example Craigslist URLs you can use:

- Los Angeles cars & trucks: `https://losangeles.craigslist.org/search/cta#search=2~gallery~0`
- New York cars & trucks: `https://newyork.craigslist.org/search/cta#search=2~gallery~0`
- Chicago cars & trucks: `https://chicago.craigslist.org/search/cta#search=2~gallery~0`

You can also search for specific makes/models by adding them to the URL, for example:
`https://losangeles.craigslist.org/search/cta?query=toyota#search=2~gallery~0`

## Where to Find the Results

After the scraper completes, you can find the results in:

- `output/output_data.csv` - Raw data including all listings
- `output/filtered_phone_numbers.csv` - Only listings with valid phone numbers

## Troubleshooting

If you encounter any issues:

1. Make sure you have Python 3.7 or higher installed
2. Check that Chrome browser is installed and up to date
3. Check the `scraper.log` file for detailed error messages
4. Try running with a different Craigslist URL
5. Make sure your internet connection is stable

## Advanced Usage

For advanced users, you can run the scraper from the command line:

```
python start_scraper_with_input.py
```

Or run specific stages of the scraper:

```
python run_scraper.py "YOUR_URL_HERE" --stages 1 2 3
```
