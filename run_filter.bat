@echo off
echo Craigslist Phone Number Filter
echo ==============================
echo.
echo This script will filter the scraped data to keep only valid phone numbers
echo in the format (XXX) XXX-XXXX.
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH. Please install Python 3.7 or higher.
    pause
    exit /b 1
)

REM Check if the input file exists
if not exist "output\output_data.csv" (
    echo Error: Input file "output\output_data.csv" not found!
    echo Please run the scraper first to generate the data.
    pause
    exit /b 1
)

REM Run the filter
echo Running phone number filter...
python scraper_stage5.py

REM Check if the output file exists
if exist "output\filtered_phone_numbers.csv" (
    echo.
    echo Filter completed successfully!
    echo Results saved to "output\filtered_phone_numbers.csv"
    
    REM Count the number of records in the output file
    for /f %%a in ('python -c "import pandas as pd; print(len(pd.read_csv('output/filtered_phone_numbers.csv')))"') do set count=%%a
    echo Found %count% records with valid phone numbers.
) else (
    echo.
    echo Error: Filter failed or no valid phone numbers found.
)

echo.
pause
