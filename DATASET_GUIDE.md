# Dataset Integration Guide

Hướng dẫn tích hợp datasets thật vào hệ thống gợi ý video.

## 📊 Datasets Được Hỗ Trợ

### 1. ShortVideo Dataset (Tsinghua University)
- **Source**: https://github.com/tsinghua-fib-lab/ShortVideo_dataset
- **Download**: https://www.dropbox.com/scl/fo/5z7pwp6xjkrr1926vreu6/AFYter5C6BDTOCpxkxF0k9Y?dl=0&rlkey=p28j6u1fl1ubb7bufiq16onbl
- **Format**: Pickle files (.pkl)
- **Size**: ~100MB

### 2. MicroLens Dataset (Westlake University)
- **Source**: https://github.com/westlake-repl/MicroLens
- **Download**: https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/
- **Format**: CSV files
- **Size**: ~200MB (MicroLens-100k)

## 🚀 Quick Start

### Option 1: Fake Data (Default)
Không cần download gì, hệ thống tự generate fake data.

```bash
# .env file
DATA_MODE=fake
```

### Option 2: ShortVideo Dataset
```bash
# 1. Download dataset
python scripts/download_datasets.py --dataset shortvideo

# 2. Manual download từ Dropbox link
# 3. Extract files vào: data/shortvideo/

# 4. Update .env
DATA_MODE=shortvideo

# 5. Restart data generator
docker-compose restart data-generator
```

### Option 3: MicroLens Dataset
```bash
# 1. Download dataset
python scripts/download_datasets.py --dataset microlens

# 2. Manual download từ website
# 3. Extract files vào: data/microlens/

# 4. Update .env
DATA_MODE=microlens

# 5. Restart data generator
docker-compose restart data-generator
```

## 📁 File Structure

### ShortVideo Dataset
```
data/shortvideo/
├── video_info.pkl          # Video metadata
├── user_info.pkl           # User information
└── train.pkl              # User-video interactions
```

Expected fields in video_info.pkl:
- `video_id` or `id`: Unique video identifier
- `title` or `name`: Video title
- `author_id` or `creator_id`: Creator ID
- `category` or `tag`: Video category
- `tags` or `hashtags`: List of tags
- `duration`: Video length in seconds
- `view_count`, `like_count`, `share_count`, `comment_count`: Engagement metrics
- `upload_time` or `created_at`: Upload timestamp

### MicroLens Dataset
```
data/microlens/
├── videos.csv             # Video metadata
├── interactions.csv       # User interactions
└── users.csv             # User information (optional)
```

Expected columns in videos.csv:
- `video_id` or `item_id`: Unique identifier
- `title` or `name`: Video title
- `creator_id` or `author_id`: Creator
- `category` or `genre`: Category
- `duration` or `length`: Duration
- `view_count`, `like_count`: Metrics

## 🛠️ Testing Datasets

### Create Sample Datasets
```bash
# Generate sample datasets for testing
python scripts/download_datasets.py --create-samples

# Check if datasets are available
python scripts/download_datasets.py --check
```

### Verify Integration
```bash
# Check logs to see if real data is being used
docker-compose logs -f data-generator

# You should see:
# "Initializing shortvideo dataset..."
# "✅ shortvideo dataset loaded successfully!"
# "Using real video: sv_video_123"
```

## 🔧 Configuration

### Environment Variables

**DATA_MODE**
- `fake`: Generate fake data (default)
- `shortvideo`: Use ShortVideo dataset
- `microlens`: Use MicroLens dataset

**DATA_PATH**
- Default: `/data`
- Path to dataset directory in container

**EVENTS_PER_SECOND**
- Default: `10`
- Number of events to generate per second

### Docker Compose

```yaml
data-generator:
  environment:
    - DATA_MODE=shortvideo    # or 'microlens' or 'fake'
    - DATA_PATH=/data
    - EVENTS_PER_SECOND=10
  volumes:
    - ./data:/data            # Mount local data directory
```

## 📊 Data Flow

### Fake Mode
```
EventGenerator → Generate Random Data → Kafka
```

### Real Dataset Mode
```
Dataset Files → DatasetLoader → EventGenerator → Kafka
                     ↓
              (70% real data)
                     ↓
              (30% fake data - for diversity)
```

## 🔍 Troubleshooting

### Dataset Not Loading
```bash
# Check if files exist
ls -la data/shortvideo/
ls -la data/microlens/

# Check logs
docker-compose logs data-generator

# Verify file permissions
chmod -R 755 data/
```

### Fallback to Fake Data
If dataset loading fails, system automatically falls back to fake data.

Check logs:
```
⚠️ Failed to load shortvideo dataset, falling back to fake data
```

### File Format Issues

**ShortVideo**: Ensure files are pickle format (.pkl)
```python
# Verify file
import pickle
with open('data/shortvideo/video_info.pkl', 'rb') as f:
    data = pickle.load(f)
    print(len(data))
```

**MicroLens**: Ensure files are CSV format
```python
import pandas as pd
df = pd.read_csv('data/microlens/videos.csv')
print(df.head())
```

## 📝 Custom Dataset

To add your own dataset:

1. Create a new loader in `dataset_loader.py`:
```python
class CustomDatasetLoader:
    def __init__(self, data_path):
        self.data_path = data_path
    
    def load_dataset(self):
        # Load your data
        pass
    
    def get_random_video(self):
        # Return video dict
        pass
```

2. Update `DatasetManager`:
```python
elif dataset_type == 'custom':
    self.loader = CustomDatasetLoader(f"{data_path}/custom")
```

3. Use in .env:
```bash
DATA_MODE=custom
```

## 📚 References

- [ShortVideo Dataset Paper](https://github.com/tsinghua-fib-lab/ShortVideo_dataset)
- [MicroLens Dataset Paper](https://github.com/westlake-repl/MicroLens)
- [Dataset Loader Implementation](services/data-generator/dataset_loader.py)

## 🤝 Contributing

To add support for new datasets, please:
1. Create a loader class
2. Update documentation
3. Test with sample data
4. Submit PR

---

**Need Help?** Check the main [DOCUMENTATION.md](DOCUMENTATION.md) or create an issue.
