"""
Download datasets from external sources
"""

from pathlib import Path
import logging
import pandas as pd
import pickle
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_shortvideo_dataset():
    """
    Download ShortVideo dataset from Dropbox
    URL: https://www.dropbox.com/scl/fo/5z7pwp6xjkrr1926vreu6/AFYter5C6BDTOCpxkxF0k9Y?dl=0&rlkey=p28j6u1fl1ubb7bufiq16onbl
    """
    logger.info("=" * 60)
    logger.info("ShortVideo Dataset Download Instructions")
    logger.info("=" * 60)
    
    print("""
    To download the ShortVideo dataset:
    
    1. Visit the Dropbox URL:
       https://www.dropbox.com/scl/fo/5z7pwp6xjkrr1926vreu6/AFYter5C6BDTOCpxkxF0k9Y?dl=0&rlkey=p28j6u1fl1ubb7bufiq16onbl
    
    2. Download the dataset files manually
    
    3. Extract to: data/shortvideo/
    
    Expected files:
    - video_info.pkl
    - user_info.pkl
    - train.pkl (or interactions.pkl)
    
    GitHub Reference:
    https://github.com/tsinghua-fib-lab/ShortVideo_dataset
    """)
    
    # Create directory
    data_dir = Path('data/shortvideo')
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created directory: %s", data_dir)
    
    return str(data_dir)

def download_microlens_dataset():
    """
    Download MicroLens dataset from Westlake University
    URL: https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/
    """
    logger.info("=" * 60)
    logger.info("MicroLens Dataset Download Instructions")
    logger.info("=" * 60)
    
    print("""
    To download the MicroLens-100k dataset:
    
    1. Visit the official website:
       https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/
    
    2. Fill out the application form (if required)
    
    3. Download the dataset files
    
    4. Extract to: data/microlens/
    
    Expected files:
    - videos.csv (or items.csv)
    - interactions.csv (or ratings.csv)
    - users.csv (optional)
    
    GitHub Reference:
    https://github.com/westlake-repl/MicroLens
    """)
    
    # Create directory
    data_dir = Path('data/microlens')
    data_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created directory: %s", data_dir)
    
    # Try to download sample data if available
    try:
        logger.info("Attempting to download sample files...")
        # Note: Replace with actual download URLs if available
        # This is a placeholder
        logger.warning("Automatic download not available - please download manually")
    except Exception as e:
        logger.error("Download failed: %s", e)
    
    return str(data_dir)

def check_dataset_availability():
    """Check which datasets are available"""
    logger.info("\n%s", "=" * 60)
    logger.info("Dataset Availability Check")
    logger.info("%s", "=" * 60)
    
    # Check ShortVideo
    shortvideo_path = Path('data/shortvideo')
    shortvideo_files = list(shortvideo_path.glob('*.pkl'))
    
    if shortvideo_files:
        logger.info("✅ ShortVideo dataset found: %s files", len(shortvideo_files))
        for f in shortvideo_files:
            logger.info("   - %s", f.name)
    else:
        logger.warning("❌ ShortVideo dataset not found")
    
    # Check MicroLens
    microlens_path = Path('data/microlens')
    microlens_files = list(microlens_path.glob('*.csv'))
    
    if microlens_files:
        logger.info("✅ MicroLens dataset found: %s files", len(microlens_files))
        for f in microlens_files:
            logger.info("   - %s", f.name)
    else:
        logger.warning("❌ MicroLens dataset not found")
    
    logger.info("=" * 60)

def create_sample_datasets():
    """Create sample datasets for testing"""
    logger.info("Creating sample datasets for testing...")
    
    # Create sample ShortVideo data
    shortvideo_path = Path('data/shortvideo')
    shortvideo_path.mkdir(parents=True, exist_ok=True)
    
    sample_videos = []
    for i in range(100):
        sample_videos.append({
            'video_id': f'sv_video_{i}',
            'title': f'Sample Video {i}',
            'author_id': f'author_{i % 20}',
            'category': ['comedy', 'music', 'dance', 'tech'][i % 4],
            'tags': ['tag1', 'tag2'],
            'duration': 30 + i,
            'view_count': i * 100,
            'like_count': i * 10,
            'upload_time': '2024-01-01T00:00:00'
        })
    
    with open(shortvideo_path / 'video_info.pkl', 'wb') as f:
        pickle.dump(sample_videos, f)
    
    logger.info("✅ Created sample ShortVideo dataset: %s videos", len(sample_videos))
    
    # Create sample MicroLens data
    microlens_path = Path('data/microlens')
    microlens_path.mkdir(parents=True, exist_ok=True)
    
    videos_df = pd.DataFrame({
        'video_id': [f'ml_video_{i}' for i in range(100)],
        'title': [f'MicroLens Video {i}' for i in range(100)],
        'creator_id': [f'creator_{i % 20}' for i in range(100)],
        'category': [['comedy', 'music', 'dance', 'tech'][i % 4] for i in range(100)],
        'duration': [30 + i for i in range(100)],
        'view_count': [i * 100 for i in range(100)],
        'like_count': [i * 10 for i in range(100)]
    })
    
    videos_df.to_csv(microlens_path / 'videos.csv', index=False)
    
    logger.info("✅ Created sample MicroLens dataset: %s videos", len(videos_df))

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Download and manage datasets')
    parser.add_argument('--dataset', choices=['shortvideo', 'microlens', 'all'], 
                        default='all', help='Which dataset to download')
    parser.add_argument('--check', action='store_true', 
                        help='Check dataset availability')
    parser.add_argument('--create-samples', action='store_true',
                        help='Create sample datasets for testing')
    
    args = parser.parse_args()
    
    if args.check:
        check_dataset_availability()
        return
    
    if args.create_samples:
        create_sample_datasets()
        check_dataset_availability()
        return
    
    if args.dataset in ['shortvideo', 'all']:
        download_shortvideo_dataset()
    
    if args.dataset in ['microlens', 'all']:
        download_microlens_dataset()
    
    logger.info("\n✅ Dataset setup instructions displayed!")
    logger.info("Run with --create-samples to create test datasets")
    logger.info("Run with --check to verify dataset files")

if __name__ == "__main__":
    main()
