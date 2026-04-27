@echo off
REM ============================================================
REM  Setup Windows Task Scheduler for daily City Dashboard scraper
REM  Run this ONCE as Administrator to create the daily task.
REM ============================================================

set TASK_NAME=CityDash_Daily_Scraper
set SCRIPT_PATH=%~dp0run_scraper.bat
set RUN_TIME=08:00

echo Creating scheduled task: %TASK_NAME%
echo Script: %SCRIPT_PATH%
echo Daily at: %RUN_TIME%
echo.

schtasks /create /tn "%TASK_NAME%" /tr "\"%SCRIPT_PATH%\"" /sc DAILY /st %RUN_TIME% /f /rl HIGHEST

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task created successfully!
    echo The scraper will run daily at %RUN_TIME%.
    echo.
    echo To verify:  schtasks /query /tn "%TASK_NAME%"
    echo To delete:  schtasks /delete /tn "%TASK_NAME%" /f
    echo To run now: schtasks /run /tn "%TASK_NAME%"
) else (
    echo.
    echo ERROR: Failed to create task. Try running this script as Administrator.
)

pause
