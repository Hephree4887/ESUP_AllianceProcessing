@echo off
REM ====================================================================
REM JSON to MySQL Importer - Developer Launch Script
REM Version 2.0 (October 7, 2025)
REM ====================================================================

echo Checking development environment...
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.6 or higher from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)

REM Check Python version (needs 3.6 or higher)
python -c "import sys; sys.exit(0 if sys.version_info >= (3,6) else 1)" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python 3.6 or higher is required
    echo Current version:
    python --version
    echo.
    pause
    exit /b 1
)

REM Check if pip is available
pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pip is not installed or not in PATH
    echo Please install pip or repair your Python installation
    echo.
    pause
    exit /b 1
)

REM Check if requirements.txt exists
if not exist requirements.txt (
    echo [ERROR] requirements.txt not found
    echo Please ensure you're running this script from the project root directory
    echo.
    pause
    exit /b 1
)

REM Check if JSONtoMySQL.py exists
if not exist JSONtoMySQL.py (
    echo [ERROR] JSONtoMySQL.py not found
    echo Please ensure you're running this script from the project root directory
    echo.
    pause
    exit /b 1
)

echo Environment checks passed!
echo.

REM Check if mysql-connector-python is installed with correct version
pip show mysql-connector-python | findstr "Version: 8.4" >nul 2>&1
if errorlevel 1 (
    echo Installing/Updating required packages...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install requirements
        echo Please try running: pip install -r requirements.txt
        echo.
        pause
        exit /b 1
    )
)

echo.
echo ====================================================================
echo JSON to MySQL Importer
echo Developer Mode
echo ====================================================================
echo.
echo Starting application...
echo.

REM Launch the Python script
python JSONtoMySQL.py

REM Check for script errors
if errorlevel 1 (
    echo.
    echo ====================================================================
    echo Application closed with an error
    echo ====================================================================
    echo.
    echo If you see import errors, try:
    echo    1. pip install -r requirements.txt
    echo    2. Verify mysql-connector-python is version 8.4
    echo.
    echo For other issues, please check README.MD or contact support
    echo.
    pause
    exit /b 1
)

exit /b 0