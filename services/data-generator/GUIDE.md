🚀Cách Sử Dụng
Bước 1: Download Dataset (Tùy chọn)
# Tạo sample dataset để test
python scripts/download_datasets.py --create-samples

# Hoặc download dataset thật (manual)
python scripts/download_datasets.py --dataset shortvideo

Bước 2: Cấu Hình Mode
Chỉnh sửa file .env:
# Chọn 1 trong 3:
DATA_MODE=fake          # Fake data (default)
DATA_MODE=shortvideo    # ShortVideo dataset
DATA_MODE=microlens     # MicroLens dataset

Bước 3: Restart Service
docker-compose restart data-generator

# Xem logs để verify
docker-compose logs -f data-generator

📊 Dataset Structure
ShortVideo Dataset
data/shortvideo/
├── video_info.pkl      # Video metadata
├── user_info.pkl       # User info
└── train.pkl          # Interactions

MicroLens Dataset
data/microlens/
├── videos.csv         # Video metadata
├── interactions.csv   # User interactions
└── users.csv         # User info (optional)

Test now:
# Tạo sample data
python scripts/download_datasets.py --create-samples

# Update .env
# DATA_MODE=shortvideo

# Copy .env
copy .env.example .env

# Restart
docker-compose restart data-generator

# Check logs - bạn sẽ thấy:
# "✅ shortvideo dataset loaded successfully!"
# "Using real video: sv_video_123"