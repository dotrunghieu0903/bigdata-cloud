#!/bin/bash

echo "🚀 Starting Video Recommendation System..."
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/models
mkdir -p data/checkpoints
mkdir -p logs

# Copy .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
fi

# Build and start services
echo "🐳 Building Docker containers..."
docker-compose build

echo "🚀 Starting services..."
docker-compose up -d zookeeper kafka redis mongodb

echo "⏳ Waiting for Kafka and MongoDB to be ready..."
sleep 20

# Create Kafka topics
echo "📢 Creating Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic user-interactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --if-not-exists --topic video-metadata --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "✅ Kafka topics created successfully!"

# Start Spark cluster
echo "⚡ Starting Spark cluster..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2

sleep 10

# Start API service
echo "🌐 Starting API service..."
docker-compose up -d api-service

# Start data generator
echo "📊 Starting data generator..."
docker-compose up -d data-generator

echo ""
echo "=========================================="
echo "✅ System is starting up!"
echo ""
echo "📊 Access points:"
echo "  - API: http://localhost:5000"
echo "  - Dashboard: http://localhost:5000/static/index.html"
echo "  - Spark UI: http://localhost:8080"
echo "  - Kafka: localhost:9092"
echo "  - Redis: localhost:6379"
echo "  - MongoDB: localhost:27017"
echo ""
echo "📝 Check logs with: docker-compose logs -f [service-name]"
echo "🛑 Stop system with: docker-compose down"
echo "=========================================="
