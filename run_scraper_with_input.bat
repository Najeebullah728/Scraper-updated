@echo off
title Craigslist Scraper - Phone Number Extractor

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python is not installed or not in PATH. Please install Python 3.7 or higher.
    pause
    exit /b 1
)

REM Check if requirements are installed
echo Checking and installing required packages...
pip install -r requirements.txt >nul 2>&1

REM Run the scraper with user input
python start_scraper_with_input.py

REM Exit
exit /b 0
