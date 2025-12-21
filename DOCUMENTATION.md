## 📋 Yêu Cầu Hệ Thống

- Docker Desktop 4.0+
- Docker Compose 2.0+
- Minimum 8GB RAM (16GB recommended)
- 20GB free disk space
- Windows 10/11, macOS, hoặc Linux

## 🚀 Cài Đặt và Chạy

### 1. Chuẩn Bị

```bash
# Clone hoặc navigate đến thư mục dự án
cd bigdata-cloud
```

### 2. Cấu Hình Environment

```bash
# Copy file cấu hình mẫu
copy .env.example .env

# Chỉnh sửa .env nếu cần (optional)
notepad .env
```

### 3. Khởi Động Hệ Thống

**Windows:**
```bash
scripts\start.bat
```

**Linux/Mac:**
```bash
chmod +x scripts/start.sh
./scripts/start.sh
```

Script sẽ tự động:
- ✅ Build Docker images cho tất cả services
- ✅ Khởi động Kafka, Zookeeper, Redis, MongoDB
- ✅ Tạo Kafka topics (user-events, user-interactions, video-metadata)
- ✅ Initialize MongoDB với indexes
- ✅ Start Spark cluster (master + 2 workers)
- ✅ Start API service và Data Generator

### 4. Chạy Spark Streaming Job

Mở terminal mới và chạy:

```bash
python scripts/submit_spark_job.py
```

Job này sẽ:
- Consume events từ Kafka topics
- Process real-time user interactions
- Update user profiles trong Redis và MongoDB
- Calculate engagement metrics

### 5. Training Recommendation Model

```bash
python scripts/train_model.py
```

Training process:
- Build user-item interaction matrix
- Train ALS model với implicit feedback
- Calculate item-item similarity matrix
- Update trending videos
- Save model to disk

### 6. Kiểm Tra Hệ Thống

Truy cập các endpoint sau:

- **Web Dashboard**: http://localhost:5000/static/index.html
- **API Health Check**: http://localhost:5000/health
- **Spark UI**: http://localhost:8080
- **Get Recommendations**: http://localhost:5000/api/v1/recommendations/user_1

## 📊 Truy Cập Hệ Thống

| Service | URL/Host | Credentials |
|---------|----------|-------------|
| Web Dashboard | http://localhost:5000/static/index.html | - |
| REST API | http://localhost:5000 | - |
| Spark UI | http://localhost:8080 | - |
| Kafka | localhost:9092 | - |
| Redis | localhost:6379 | - |
| MongoDB | localhost:27017 | admin/admin123 |

## 🔌 API Endpoints

### 1. Health Check
```http
GET /health

Response:
{
  "status": "healthy",
  "timestamp": "2024-12-21T10:00:00",
  "services": {
    "kafka": "connected",
    "redis": "connected",
    "mongodb": "connected"
  }
}
```

### 2. Get Recommendations
```http
GET /api/v1/recommendations/{user_id}?n=20&method=als

Parameters:
- user_id: ID của user (required)
- n: Số lượng recommendations (default: 20)
- method: 'als' hoặc 'item_cf' (default: 'als')

Response:
{
  "user_id": "user_1",
  "recommendations": [
    {
      "video_id": "video_123",
      "score": 0.85,
      "method": "als",
      "title": "Video Title",
      "category": "comedy",
      "duration": 45.5
    },
    ...
  ],
  "count": 20
}
```

### 3. Track User Event
```http
POST /api/v1/events/track
Content-Type: application/json

Body:
{
  "event_type": "view",
  "user_id": "user_123",
  "video_id": "video_456",
  "watch_time": 30.5,
  "total_duration": 60.0
}

Response:
{
  "status": "success",
  "message": "Event tracked successfully"
}
```

### 4. Get Trending Videos
```http
GET /api/v1/trending?n=20

Response:
{
  "trending": [
    {
      "video_id": "video_789",
      "score": 1250.0,
      "method": "trending",
      "title": "Trending Video"
    },
    ...
  ],
  "count": 20
}
```

### 5. Get User Profile
```http
GET /api/v1/user/{user_id}/profile

Response:
{
  "profile": {
    "user_id": "user_1",
    "total_interactions": 150,
    "favorite_categories": ["comedy", "music"],
    "recent_videos": [...],
    "last_activity": "2024-12-21T10:00:00"
  },
  "recent_interactions": [...]
}
```

### 6. Get System Statistics
```http
GET /api/v1/stats

Response:
{
  "total_users": 10000,
  "total_videos": 50000,
  "total_interactions": 500000,
  "active_sessions": 1500,
  "timestamp": "2024-12-21T10:00:00"
}
```

## 📁 Cấu Trúc Thư Mục

```
bigdata-cloud/
├── docker-compose.yml          # Docker orchestration
├── requirements.txt            # Python dependencies
├── .env.example               # Environment variables template
│
├── services/
│   ├── api/                   # Flask REST API
│   │   ├── app.py            # Main API application
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── static/
│   │       └── index.html    # Web dashboard
│   │
│   └── data-generator/        # Event simulator
│       ├── event_generator.py # Generates fake user events
│       ├── Dockerfile
│       └── requirements.txt
│
├── spark-jobs/
│   ├── streaming_consumer.py  # Spark Streaming job
│   └── recommendation_engine.py  # ML recommendation models
│
├── models/
│   ├── __init__.py
│   ├── data_models.py         # Data class definitions
│   └── storage.py             # MongoDB & Redis operations
│
├── scripts/
│   ├── start.sh               # Startup script (Linux/Mac)
│   ├── start.bat              # Startup script (Windows)
│   ├── submit_spark_job.py    # Submit Spark streaming job
│   ├── train_model.py         # Train recommendation model
│   └── init-mongo.js          # MongoDB initialization
│
└── data/
    ├── models/                # Trained ML models
    └── checkpoints/           # Spark checkpoints
```

## 🔬 Thuật Toán Recommendation

### 1. Collaborative Filtering (ALS)
- **Matrix Factorization** sử dụng Alternating Least Squares
- **Implicit Feedback** từ watch time, likes, shares, comments
- **Hyperparameters**: 
  - Factors: 64
  - Iterations: 15
  - Regularization: 0.01

### 2. Item-based Collaborative Filtering
- **Cosine Similarity** giữa videos dựa trên user interaction patterns
- **Similarity Threshold**: 0.3
- Real-time updates từ Redis cache

### 3. Hybrid Approach
- Kết hợp ALS và item-based CF
- Weighted scoring based on event types
- Real-time preference updates

### 4. Event Weighting
```python
View with completion_rate > 0.8: weight = 5.0
View with completion_rate > 0.5: weight = 3.0
Like: weight = 10.0
Share: weight = 15.0
Comment: weight = 8.0
Skip: weight = -2.0
```

### 5. Cold Start Strategy
- **New Users**: Trending videos trong 24h
- **New Videos**: Category-based recommendations
- **Fallback**: Popular videos by engagement score

## 📈 Monitoring & Debugging

### View Service Logs
```bash
# Tất cả services
docker-compose logs -f

# Specific service
docker-compose logs -f api-service
docker-compose logs -f spark-master
docker-compose logs -f kafka
docker-compose logs -f data-generator
```

### Kafka Management
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec kafka kafka-topics --describe --topic user-events --bootstrap-server localhost:9092

# Consumer groups
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# View messages (debug)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic user-events --from-beginning --max-messages 10
```

### MongoDB Inspection
```bash
# Connect to MongoDB
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin

# Common queries
use video_recommendation
db.interactions.countDocuments()
db.users.find().limit(5).pretty()
db.videos.find().sort({engagement_score: -1}).limit(10)
db.user_profiles.findOne({user_id: "user_1"})
```

### Redis Inspection
```bash
# Connect to Redis
docker exec -it redis redis-cli

# Common commands
KEYS user:*
ZRANGE trending:videos:24h 0 10 WITHSCORES
HGETALL user:user_1:stats
GET recommendations:user_1
```

### Spark Job Status
```bash
# Check Spark master logs
docker logs spark-master

# Check worker logs
docker logs spark-worker-1
docker logs spark-worker-2

# Access Spark UI
# Navigate to http://localhost:8080
```

## 🧪 Testing

### Manual API Testing
```bash
# Health check
curl http://localhost:5000/health

# Get recommendations
curl "http://localhost:5000/api/v1/recommendations/user_1?n=10&method=als"

# Track event
curl -X POST http://localhost:5000/api/v1/events/track \
  -H "Content-Type: application/json" \
  -d "{\"event_type\":\"like\",\"user_id\":\"user_1\",\"video_id\":\"video_100\"}"

# Get trending
curl http://localhost:5000/api/v1/trending?n=20

# Get stats
curl http://localhost:5000/api/v1/stats
```

### Load Testing (Optional)
```bash
# Install Apache Bench
# Windows: Download from Apache website
# Linux: sudo apt-get install apache2-utils

# Run load test
ab -n 1000 -c 10 http://localhost:5000/api/v1/recommendations/user_1
```

## 🐛 Troubleshooting

### Problem: Kafka không start
```bash
# Solution: Xóa data và restart
docker-compose down -v
docker-compose up -d zookeeper
# Wait 10 seconds
docker-compose up -d kafka
```

### Problem: Spark job fails với OutOfMemory
```bash
# Solution: Tăng memory allocation
# Edit docker-compose.yml:
environment:
  - SPARK_WORKER_MEMORY=4G
  - SPARK_DRIVER_MEMORY=4G

# Hoặc giảm số workers
docker-compose up -d spark-master spark-worker-1
```

### Problem: MongoDB connection refused
```bash
# Solution: Check MongoDB status
docker-compose logs mongodb

# Restart MongoDB
docker-compose restart mongodb

# Check connection
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin
```

### Problem: Redis connection timeout
```bash
# Solution: Restart Redis
docker-compose restart redis

# Check Redis
docker exec -it redis redis-cli PING
```

### Problem: API returns 500 error
```bash
# Check API logs
docker-compose logs api-service

# Restart API
docker-compose restart api-service

# Verify all dependencies are running
docker-compose ps
```

## 🔄 Stopping and Cleanup

### Stop All Services
```bash
docker-compose down
```

### Stop and Remove All Data
```bash
docker-compose down -v
```

### Stop Specific Service
```bash
docker-compose stop api-service
```

### Restart Service
```bash
docker-compose restart api-service
```

### Rebuild After Code Changes
```bash
docker-compose build api-service
docker-compose up -d api-service
```

## ⚙️ Advanced Configuration

### Tăng Kafka Throughput
```bash
# Tăng số partitions
docker exec kafka kafka-topics --alter --topic user-events \
  --partitions 6 --bootstrap-server localhost:9092
```

### Spark Tuning
Edit [spark-jobs/streaming_consumer.py](spark-jobs/streaming_consumer.py):
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Redis Memory Management
```bash
docker exec redis redis-cli CONFIG SET maxmemory 2gb
docker exec redis redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

## 📚 Tài Liệu Tham Khảo

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Implicit Library (ALS)](https://implicit.readthedocs.io/)
- [MongoDB Best Practices](https://www.mongodb.com/docs/manual/administration/production-notes/)
- [Redis Commands Reference](https://redis.io/commands/)
- [Flask Documentation](https://flask.palletsprojects.com/)

## 🤝 Contributing

Contributions are welcome! Vui lòng tạo Pull Request hoặc Issue trên GitHub.

## 📄 License

This project is licensed under the MIT License.

## 👥 Authors

Project developed for Big Data & Cloud Computing course.

---

