@echo off
title Craigslist Scraper - With Stage Skipping

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

echo.
echo ======================================================================
echo                Craigslist Scraper - With Stage Skipping
echo ======================================================================
echo.
echo This version of the scraper allows you to skip to the next stage
echo by pressing Ctrl+C during any stage.
echo.
echo For example:
echo - Press Ctrl+C during Stage 1 to skip to Stage 2
echo - Press Ctrl+C during Stage 2 to skip to Stage 3
echo - And so on...
echo.
echo This is useful if you want to skip a stage that's taking too long
echo or if you've already completed some stages and want to continue
echo from a later stage.
echo.
echo ======================================================================
echo.
pause

REM Run the scraper with user input
python start_scraper_with_input.py

REM Exit
exit /b 0
