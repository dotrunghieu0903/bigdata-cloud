# 1. Download videos (test với 10 videos)
python scripts/download_videos.py --mode sample --num 10

# 2. Khởi động API
cd services/api && python app.py

# 3. Mở video player
# http://localhost:5000/static/videos.html

# 4. Train model với Spark MLlib
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  /opt/spark-jobs/video_recommendation_training.py