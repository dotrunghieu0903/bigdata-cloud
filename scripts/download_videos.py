"""
Download videos from MicroLens-100k dataset
Dataset URL: https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/
"""
import os
import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
import time

# Configuration
BASE_URL = "https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/MicroLens-100k_videos/"
DATA_DIR = Path(__file__).parent.parent / "data" / "MicroLens-100k"
VIDEOS_DIR = DATA_DIR / "videos"
COVERS_DIR = DATA_DIR / "covers"

# Create directories
VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
COVERS_DIR.mkdir(parents=True, exist_ok=True)

def get_video_ids():
    """Get list of video IDs from the dataset files"""
    pairs_file = DATA_DIR / "MicroLens-100k_pairs.csv"
    
    if not pairs_file.exists():
        print(f"Error: {pairs_file} not found!")
        return []
    
    # Read the pairs file to get all video IDs
    # CSV columns are: user, item, timestamp (item = videoID)
    df = pd.read_csv(pairs_file)
    video_ids = df['item'].unique().tolist()
    
    print(f"Found {len(video_ids)} unique videos in dataset")
    return video_ids

def download_video(video_id, max_retries=3):
    """Download a single video with retry logic"""
    video_filename = f"{video_id}.mp4"
    video_path = VIDEOS_DIR / video_filename
    
    # Skip if already downloaded
    if video_path.exists():
        return f"✓ Skipped (exists): {video_filename}"
    
    url = f"{BASE_URL}{video_filename}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                
                with open(video_path, 'wb') as f:
                    if total_size:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    else:
                        f.write(response.content)
                
                return f"✓ Downloaded: {video_filename}"
            elif response.status_code == 404:
                return f"✗ Not found: {video_filename}"
            else:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return f"✗ Failed ({response.status_code}): {video_filename}"
                
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return f"✗ Error: {video_filename} - {str(e)}"
    
    return f"✗ Failed after {max_retries} retries: {video_filename}"

def download_cover(video_id):
    """Download video cover/thumbnail"""
    cover_filename = f"{video_id}.jpg"
    cover_path = COVERS_DIR / cover_filename
    
    if cover_path.exists():
        return True
    
    cover_url = f"https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/MicroLens-100k_covers/{cover_filename}"
    
    try:
        response = requests.get(cover_url, timeout=10)
        if response.status_code == 200:
            with open(cover_path, 'wb') as f:
                f.write(response.content)
            return True
    except:
        pass
    
    return False

def download_batch(video_ids, max_workers=5, download_covers=True):
    """Download videos in parallel"""
    print(f"\nDownloading {len(video_ids)} videos...")
    print(f"Using {max_workers} parallel workers")
    print(f"Saving to: {VIDEOS_DIR}\n")
    
    results = {
        'downloaded': 0,
        'skipped': 0,
        'failed': 0,
        'not_found': 0
    }
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_video, vid): vid for vid in video_ids}
        
        with tqdm(total=len(video_ids), desc="Downloading") as pbar:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                
                if "Downloaded" in result:
                    results['downloaded'] += 1
                elif "Skipped" in result:
                    results['skipped'] += 1
                elif "Not found" in result:
                    results['not_found'] += 1
                else:
                    results['failed'] += 1
                
                pbar.set_postfix({
                    'DL': results['downloaded'],
                    'Skip': results['skipped'],
                    'Fail': results['failed'],
                    'N/F': results['not_found']
                })
                pbar.update(1)
    
    # Download covers if requested
    if download_covers and results['downloaded'] > 0:
        print("\nDownloading video covers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(tqdm(
                executor.map(download_cover, video_ids),
                total=len(video_ids),
                desc="Covers"
            ))
    
    return results

def download_sample(num_videos=10):
    """Download a sample of videos for testing"""
    video_ids = get_video_ids()
    if not video_ids:
        return
    
    sample_ids = video_ids[:num_videos]
    print(f"\nDownloading sample of {num_videos} videos...")
    return download_batch(sample_ids)

def download_all():
    """Download all videos from the dataset"""
    video_ids = get_video_ids()
    if not video_ids:
        return
    
    return download_batch(video_ids, max_workers=10)

def download_popular_videos(top_n=100):
    """Download most popular videos based on interaction count"""
    pairs_file = DATA_DIR / "MicroLens-100k_pairs.csv"
    
    if not pairs_file.exists():
        print(f"Error: {pairs_file} not found!")
        return
    
    df = pd.read_csv(pairs_file)
    
    # Count interactions per video (item column = videoID)
    video_counts = df['item'].value_counts()
    popular_videos = video_counts.head(top_n).index.tolist()
    
    print(f"\nDownloading top {top_n} most popular videos...")
    print(f"Most popular video has {video_counts.iloc[0]} interactions")
    
    return download_batch(popular_videos)

def show_stats():
    """Show download statistics"""
    video_files = list(VIDEOS_DIR.glob("*.mp4"))
    cover_files = list(COVERS_DIR.glob("*.jpg"))
    
    total_size = sum(f.stat().st_size for f in video_files)
    total_size_gb = total_size / (1024**3)
    
    print("\n" + "="*60)
    print("DOWNLOAD STATISTICS")
    print("="*60)
    print(f"Videos downloaded: {len(video_files)}")
    print(f"Covers downloaded: {len(cover_files)}")
    print(f"Total size: {total_size_gb:.2f} GB")
    print(f"Videos directory: {VIDEOS_DIR}")
    print(f"Covers directory: {COVERS_DIR}")
    print("="*60 + "\n")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Download MicroLens-100k videos")
    parser.add_argument(
        '--mode',
        choices=['sample', 'popular', 'all', 'stats'],
        default='sample',
        help='Download mode: sample (10 videos), popular (top 100), all, or stats (show current status)'
    )
    parser.add_argument(
        '--num',
        type=int,
        default=10,
        help='Number of videos for sample/popular mode'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'stats':
        show_stats()
    elif args.mode == 'sample':
        results = download_sample(args.num)
        show_stats()
    elif args.mode == 'popular':
        results = download_popular_videos(args.num)
        show_stats()
    elif args.mode == 'all':
        print("\n⚠️  WARNING: This will download ALL videos (~100GB+)")
        response = input("Continue? (yes/no): ")
        if response.lower() == 'yes':
            results = download_all()
            show_stats()
        else:
            print("Cancelled.")
