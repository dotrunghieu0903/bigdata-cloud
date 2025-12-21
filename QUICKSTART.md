# Quick Start Guide

## Quick StART (5 mins)

### Bước 1: Khởi động hệ thống
```bash
# Windows
scripts\start.bat

# Linux/Mac
./scripts/start.sh
```

### Bước 2: Chờ services khởi động (khoảng 30 giây)

### Bước 3: Truy cập Dashboard
Mở trình duyệt: http://localhost:5000/static/index.html

### Bước 4: Test thử nghiệm
1. Nhập User ID (vd: user_1) vào ô input
2. Click "Get Recommendations"
3. Xem kết quả gợi ý video

### Bước 5: Chạy Spark Streaming (optional)
```bash
python scripts/submit_spark_job.py
```

## Demo Flow

1. **Data Generator** tự động tạo fake events mỗi giây
2. Events được gửi vào **Kafka topics**
3. **Spark Streaming** xử lý events real-time
4. Cập nhật **Redis** (cache) và **MongoDB** (storage)
5. **API** trả về recommendations từ cache
6. **Dashboard** hiển thị kết quả

## Useful Commands

```bash
# Xem logs
docker-compose logs -f api-service

# Stop hệ thống
docker-compose down

# Restart service
docker-compose restart api-service

# Xem stats
curl http://localhost:5000/api/v1/stats
```

## Fast Troubleshooting

**Error: Docker didn't run**
→ Mở Docker Desktop

**Error: Port already in use**
→ Đổi port trong docker-compose.yml

**Error: Out of memory**
→ Tăng RAM cho Docker Desktop (Settings → Resources)

---
View [DOCUMENTATION.md](DOCUMENTATION.md) to know details.
