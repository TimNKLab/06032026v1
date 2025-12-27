@echo off
REM ETL Stack Management Script for Windows

setlocal enabledelayedexpansion

if "%1"=="" goto :help
if "%1"=="help" goto :help
if "%1"=="start" goto :start
if "%1"=="stop" goto :stop
if "%1"=="restart" goto :restart
if "%1"=="status" goto :status
if "%1"=="logs" goto :logs
if "%1"=="health" goto :health

:help
echo Usage: manage-etl.bat [command]
echo.
echo Commands:
echo   start    - Start the ETL stack
echo   stop     - Stop the ETL stack
echo   restart  - Restart the ETL stack
echo   status   - Check status of all services
echo   logs     - Show logs from all services
echo   health   - Check ETL health
echo   help     - Show this help message
echo.
goto :end

:start
echo Starting ETL stack...
docker-compose up -d
echo ETL stack started.
goto :end

:stop
echo Stopping ETL stack...
docker-compose down
echo ETL stack stopped.
goto :end

:restart
echo Restarting ETL stack...
docker-compose down
docker-compose up -d
echo ETL stack restarted.
goto :end

:status
echo Checking ETL stack status...
docker-compose ps
goto :end

:logs
echo Showing logs (press Ctrl+C to exit)...
docker-compose logs -f
goto :end

:health
echo Checking ETL health...
docker-compose exec celery-worker python -c "from etl_tasks import check_etl_health; print(check_etl_health())"
goto :end

:end
pause
