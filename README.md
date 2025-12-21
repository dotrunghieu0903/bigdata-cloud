# Hệ Thống Gợi Ý Video Real-time 🎬

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
- **Implicit ALS**: Collaborative filtering với implicit feedback
- **Scikit-learn**: Feature engineering và similarity calculations
- **Item-based CF**: Content-based filtering

### Infrastructure & API
- **Docker & Docker Compose**: Containerization và orchestration
- **Flask**: REST API framework
- **Python 3.11**: Primary programming language

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