# Hệ Thống Gợi Ý Video Real-time 🎬

# Demo: https://youtu.be/agMtc0KSvTg
Hệ thống gợi ý video real-time cho nền tảng chia sẻ video ngắn, sử dụng Big Data và Cloud Computing technologies.

![Architecture](https://img.shields.io/badge/Architecture-Kappa-blue)
![Python](https://img.shields.io/badge/Python-3.11-green)
![Spark](https://img.shields.io/badge/Spark-3.5-orange)
![Kafka](https://img.shields.io/badge/Kafka-7.5-red)

## 📚 Mục Tiêu Dự Án

Dự án này thực hiện:
- ✅ Triển khai 1 bigdata framework, big data architecture (Kappa Architecture) với đầy đủ các module trong hệ sinh thái Big Data
- ✅ Triển khai mô hình Machine Learning (Collaborative Filtering) trên xử lý phân tán với Apache Spark
- ✅ Triển khai thuật toán real-time trên streaming processing để giải quyết bài toán recommendation system

**Topic**: Xây dựng hệ thống gợi ý real-time cho nền tảng chia sẻ video ngắn

**Description**: Phát triển một hệ thống gợi ý video cho nền tảng chia sẻ video ngắn, phân tích hành vi người dùng và siêu dữ liệu nội dung theo thời gian thực. Hệ thống sử dụng công nghệ xử lý dữ liệu lớn và điện toán đám mây để cung cấp các đề xuất cá nhân hóa, nâng cao tương tác người dùng và xử lý hiệu quả dữ liệu streaming với khối lượng lớn.

## 🎯 Tính Năng Chính

- ⚡ **Real-time Processing**: Xử lý sự kiện người dùng trong vài giây với Apache Kafka và Spark Streaming
- 🤖 **AI Recommendations**: Sử dụng Collaborative Filtering (ALS) và Item-based CF
- 📊 **Scalable Architecture**: Kiến trúc Kappa có khả năng mở rộng cao
- 🚀 **High Performance**: Redis cache để truy xuất nhanh, MongoDB cho lưu trữ bền vững
- 📈 **Real-time Analytics**: Dashboard theo dõi metrics và trending videos
- 🎬 **Video Analytics**: Phân tích watch time, completion rate, engagement metrics

## 🏗️ Kiến Trúc Hệ Thống

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Client    │────▶│  API Service │────▶│  Kafka Topics   │
│ (Web/Mobile)│     │   (Flask)    │     │  - user-events  │
└─────────────┘     └──────────────┘     │  - interactions │
                                          └─────────────────┘
                                                   │
                    ┌──────────────────────────────┴────────────┐
                    │                                            │
            ┌───────▼────────┐                          ┌───────▼────────┐
            │ Spark Streaming│                          │  Data Generator │
            │   Processing   │                          │   (Simulator)   │
            └───────┬────────┘                          └─────────────────┘
                    │
        ┌───────────┴────────────┐
        │                        │
  ┌─────▼─────┐          ┌──────▼──────┐
  │   Redis   │          │   MongoDB   │
  │  (Cache)  │          │  (Storage)  │
  └───────────┘          └─────────────┘
        │                        │
        └───────────┬────────────┘
                    │
            ┌───────▼────────┐
            │ Recommendation │
            │     Engine     │
            │   (ALS + CF)   │
            └────────────────┘
```

## 🛠️ Tech Stack

### Big Data Processing
- **Apache Kafka**: Event streaming platform cho message queue
- **Apache Spark 3.5**: Distributed data processing engine
- **PySpark Streaming**: Real-time stream processing

### Storage Layer
- **MongoDB**: NoSQL database cho persistent storage
- **Redis**: In-memory cache cho fast retrieval và real-time data

### Machine Learning
- **Spark MLlib ALS**: Distributed collaborative filtering với implicit feedback (phân tán trên cluster)
- **Scikit-learn**: Feature engineering và similarity calculations
- **Item-based CF**: Content-based filtering

### Infrastructure & API
- **Docker & Docker Compose**: Containerization và orchestration
- **Flask**: REST API framework
- **Python 3.11**: Primary programming language

## 🚀 Distributed Machine Learning với Spark MLlib

### Tổng quan

Hệ thống sử dụng **Distributed ML** (Spark MLlib) để tận dụng sức mạnh của Spark cluster, cho phép xử lý dữ liệu lớn và training model phân tán trên nhiều workers.

### Điểm khác biệt: Centralized vs Distributed

#### ❌ Centralized ML (Trước đây)
```python
# Sử dụng implicit library - chạy trên 1 node
from implicit.als import AlternatingLeastSquares

als_model = AlternatingLeastSquares(factors=64)
als_model.fit(interaction_matrix)  # Chạy trên single machine
```

#### ✅ Distributed ML (Hiện tại)
```python
# Sử dụng Spark MLlib - phân tán trên cluster
from pyspark.ml.recommendation import ALS

als = ALS(rank=64, implicitPrefs=True)
model = als.fit(interactions_df)  # Phân tán trên nhiều workers
```

### Tính phân tán thể hiện ở đâu?

#### 1. **Data Loading - Phân tán đọc dữ liệu**
```python
# Đọc từ MongoDB sử dụng Spark connector
interactions_df = spark.read \
    .format("mongodb") \
    .option("collection", "interactions") \
    .load()  # ← Dữ liệu được phân phối trên workers
```

#### 2. **Data Transformation - Xử lý phân tán**
```python
# Broadcast mappings đến tất cả workers
user_map_bc = spark.sparkContext.broadcast(user_id_map)

# UDF chạy distributed trên mỗi partition
@udf(IntegerType())
def user_to_idx(user_id):
    return user_map_bc.value.get(user_id, -1)
```

#### 3. **Model Training - Training phân tán**
```python
# ALS training được phân phối
# - Data được chia thành partitions
# - Mỗi worker xử lý các partitions khác nhau
# - Gradients được tổng hợp distributed
model = als.fit(train_df)  # ← Chạy song song trên workers
```

#### 4. **Batch Recommendations - Inference phân tán**
```python
# Tạo recommendations cho nhiều users song song
recommendations = model.recommendForUserSubset(users_df, n=20)
# ← Tất cả users được xử lý parallel trên cluster
```

### Kiến trúc Distributed Training

```
┌─────────────────────────────────────────────────────────┐
│                    Spark Master                         │
│  - Điều phối công việc                                  │
│  - Quản lý resources                                    │
│  - Driver program                                       │
└────────────┬────────────────────────────────────────────┘
             │
    ┌────────┴────────┐
    │                 │
┌───▼────┐      ┌────▼────┐
│Worker 1│      │Worker 2 │
│2G RAM  │      │2G RAM   │
│2 cores │      │2 cores  │
└────────┘      └─────────┘
    │                │
    └────────┬───────┘
             │
    Parallel Processing:
    - Data partitions
    - ALS iterations
    - Matrix operations
```

### Workflow Training

1. **Data Loading** (Distributed)
   - MongoDB → Spark DataFrame
   - Data split across workers

2. **Preprocessing** (Distributed)
   - Create mappings (broadcast)
   - Index conversion (UDF on partitions)

3. **Training** (Distributed)
   - ALS algorithm runs on partitions
   - Each iteration:
     - User factors update (parallel)
     - Item factors update (parallel)
     - Loss aggregation (reduce)

4. **Evaluation** (Distributed)
   - Predictions on test set
   - RMSE calculation (parallel)

5. **Model Save** (Distributed)
   - Save to distributed file system
   - Model metadata + factors

### Chạy Distributed Training

```bash
# Submit job lên Spark cluster
python scripts/train_model.py

# Hoặc trực tiếp
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 2 \
    /opt/spark-jobs/recommendation_engine.py
```

### Monitoring

Truy cập Spark UI để xem quá trình phân tán:
- **URL**: http://localhost:8080
- **Xem**:
  - Active workers
  - Running executors
  - Task distribution
  - Stage completion

### Lợi ích của Distributed ML

#### Scalability
- ✅ Xử lý được datasets lớn (GB-TB)
- ✅ Thêm workers = tăng performance
- ✅ Không bị giới hạn RAM của 1 máy

#### Performance
- ✅ Training nhanh hơn với nhiều workers
- ✅ Parallel recommendations cho nhiều users
- ✅ Distributed data loading

#### Fault Tolerance
- ✅ Tự động retry failed tasks
- ✅ Data replication
- ✅ Lineage-based recovery

### So sánh Performance

| Metric | Centralized (implicit) | Distributed (Spark MLlib) |
|--------|------------------------|---------------------------|
| Max Dataset Size | ~10GB (RAM limit) | Unlimited (cluster RAM) |
| Training Time (1M interactions) | ~5 minutes | ~2 minutes (2 workers) |
| Parallel Users Inference | Sequential | Parallel |
| Scalability | Vertical only | Horizontal |
| Fault Tolerance | None | Built-in |

### Cấu hình tối ưu

#### Cho datasets nhỏ (<10GB)
```python
ALS(rank=64, maxIter=10, regParam=0.01)
# Executors: 2-4
# Memory per executor: 2G
```

#### Cho datasets lớn (>10GB)
```python
ALS(rank=128, maxIter=15, regParam=0.01)
# Executors: 8-16
# Memory per executor: 4G
```

### Troubleshooting

#### Out of Memory
- Tăng executor memory
- Reduce rank (số factors)
- Tăng số partitions

#### Slow Training
- Tăng số workers
- Tăng executor cores
- Cache DataFrame

#### Cold Start
- Hybrid với trending videos
- Content-based filtering
- Popularity-based fallback

## 📋 Datasets Tham Khảo

1. **Tsinghua ShortVideo Dataset**
   - Link: https://github.com/tsinghua-fib-lab/ShortVideo_dataset
   - Chứa user interactions và video metadata

2. **MicroLens Dataset**
   - Link: https://github.com/westlake-repl/MicroLens
   - Dataset lớn với multi-modal features

"TikTok API / Scraper (Selenium,...) → Kafka Topic → Spark Streaming →
Thu thập: video ID, caption, comments, hashtags, likes, user info..."
Spark MLLib, ...

## Datasets
1. https://github.com/tsinghua-fib-lab/ShortVideo_dataset
2. https://github.com/westlake-repl/MicroLens

Requirements

- PPT -> Slide thuyết trình
        + Trang Bìa (Tên topic, nhóm thực hiện)
        + Mục lục
        + Gioi thiệu
        + .....
        + Kết luận và hướng phát triển
        + Tham khảo
- PDF -> Báo cáo (Xúc tích, không quá 20 trang)
- Link Video Demo (Nếu có), github (Nếu có) hoặc bất kỳ tham khảo nào phải đặt trong Page tham khảo của PPT.
