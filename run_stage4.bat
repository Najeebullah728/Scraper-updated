@echo off
echo Craigslist Title and Phone Number Extractor - Stage 4
echo ==================================================
echo.
echo This script will extract ONLY titles and phone numbers from the HTML files
echo and save them to a text file. Entries without a valid phone number will be skipped.
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH. Please install Python 3.7 or higher.
    pause
    exit /b 1
)

REM Check if the input directory exists
if not exist "main_data" (
    echo Error: Input directory "main_data" not found!
    echo Please run Stage 3 first to generate the HTML files.
    pause
    exit /b 1
)

REM Check if the links file exists
if not exist "output\craigslist_links.csv" (
    echo Error: Links file "output\craigslist_links.csv" not found!
    echo Please run Stage 2 first to generate the links file.
    pause
    exit /b 1
)

REM Check if any HTML files exist in the main_data directory
dir /b "main_data\*.html" >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: No HTML files found in "main_data" directory!
    echo Please run Stage 3 first to generate the HTML files.
    pause
    exit /b 1
)

REM Run the data extractor
echo Running title and phone number extractor...
python scraper_stage4.py

REM Check if the output file exists
if exist "output\output_data.txt" (
    echo.
    echo Data extraction completed successfully!
    echo Results saved to "output\output_data.txt"

    REM Count the number of records in the output file
    for /f %%a in ('python -c "count = 0; open('output/output_data.txt', 'r', encoding='utf-8').read().count('Title:'); print(count)"') do set count=%%a
    echo Extracted data from %count% listings with valid phone numbers.

    REM Show the first few entries
    echo.
    echo First few entries:
    echo -----------------
    type "output\output_data.txt" | findstr /n . | findstr /b "^[1-9]:" | findstr /v "^[1-9][0-9]:" | findstr /v "^[1-9][0-9][0-9]:"

    echo.
    echo NOTE: This modified version of Stage 4 extracts ONLY title and phone number
    echo and skips any entries without a valid phone number.
    echo The output is saved as a text file instead of CSV.
) else (
    echo.
    echo Error: Data extraction failed or no entries with valid phone numbers were found.
)

echo.
pause
