@echo off
title Update Dependencies and Test Scraper

echo Updating dependencies...
pip install -r requirements.txt

echo.
echo Dependencies updated. Press any key to test the scraper...
pause > nul

echo.
echo Running test script...
python test_scraper.py

echo.
echo Test complete. Press any key to exit...
pause > nul
