@echo off
echo Starting Video Recommendation System...
echo ==========================================

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo Error: Docker is not running. Please start Docker first.
    exit /b 1
)

REM Create necessary directories
echo Creating directories...
if not exist "data\models" mkdir "data\models"
if not exist "data\checkpoints" mkdir "data\checkpoints"
if not exist "logs" mkdir "logs"

REM Copy .env file if it doesn't exist
if not exist ".env" (
    echo Creating .env file from template...
    copy ".env.example" ".env"
)

REM Build and start services
echo Building Docker containers...
docker-compose build

echo Starting services...
docker-compose up -d zookeeper kafka redis mongodb

echo Waiting for Kafka and MongoDB to be ready...
timeout /t 20 /nobreak >nul

REM Create Kafka topics
echo Creating Kafka topics...
docker exec kafka kafka-topics --create --if-not-exists --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic user-interactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic video-metadata --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo Kafka topics created successfully!

REM Start Spark cluster
echo Starting Spark cluster...
docker-compose up -d spark-master spark-worker-1 spark-worker-2

timeout /t 10 /nobreak >nul

REM Start API service
echo Starting API service...
docker-compose up -d api-service

REM Start data generator
echo Starting data generator...
docker-compose up -d data-generator

echo.
echo ==========================================
echo System is starting up!
echo.
echo Access points:
echo   - API: http://localhost:5000
echo   - Dashboard: http://localhost:5000/static/index.html
echo   - Spark UI: http://localhost:8080
echo   - Kafka: localhost:9092
echo   - Redis: localhost:6379
echo   - MongoDB: localhost:27017
echo.
echo Check logs with: docker-compose logs -f [service-name]
echo Stop system with: docker-compose down
echo ==========================================
pause
