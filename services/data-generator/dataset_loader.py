"""
Dataset Loaders for Real Video Data
Supports ShortVideo and MicroLens datasets
"""

import os
import json
import pandas as pd
import pickle
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ShortVideoDatasetLoader:
    """
    Loader for ShortVideo dataset from Tsinghua University
    Dataset URL: https://www.dropbox.com/scl/fo/5z7pwp6xjkrr1926vreu6/AFYter5C6BDTOCpxkxF0k9Y?dl=0&rlkey=p28j6u1fl1ubb7bufiq16onbl
    GitHub: https://github.com/tsinghua-fib-lab/ShortVideo_dataset
    """
    
    def __init__(self, data_path: str = '/data/shortvideo'):
        self.data_path = Path(data_path)
        self.videos = []
        self.users = []
        self.interactions = []
        
    def load_dataset(self) -> bool:
        """Load ShortVideo dataset from files"""
        try:
            logger.info("Loading ShortVideo dataset...")
            
            # Expected file structure based on GitHub repo
            # - video_info.pkl: video metadata
            # - user_info.pkl: user information
            # - interactions.pkl or train.pkl: user-video interactions
            
            video_file = self.data_path / 'video_info.pkl'
            user_file = self.data_path / 'user_info.pkl'
            interaction_file = self.data_path / 'train.pkl'
            
            if not video_file.exists():
                logger.error(f"Video file not found: {video_file}")
                return False
            
            # Load video metadata
            with open(video_file, 'rb') as f:
                self.videos = pickle.load(f)
            logger.info(f"Loaded {len(self.videos)} videos")
            
            # Load user data if available
            if user_file.exists():
                with open(user_file, 'rb') as f:
                    self.users = pickle.load(f)
                logger.info(f"Loaded {len(self.users)} users")
            
            # Load interactions
            if interaction_file.exists():
                with open(interaction_file, 'rb') as f:
                    self.interactions = pickle.load(f)
                logger.info(f"Loaded {len(self.interactions)} interactions")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading ShortVideo dataset: {e}")
            return False
    
    def get_random_video(self) -> Optional[Dict]:
        """Get a random video from dataset"""
        if not self.videos:
            return None
        
        import random
        video = random.choice(self.videos)
        
        # Convert to standard format
        return {
            'video_id': str(video.get('video_id', video.get('id', ''))),
            'title': video.get('title', video.get('name', 'Untitled')),
            'creator_id': str(video.get('author_id', video.get('creator_id', 'unknown'))),
            'category': video.get('category', video.get('tag', 'general')),
            'tags': video.get('tags', video.get('hashtags', [])),
            'duration': float(video.get('duration', 30)),
            'view_count': int(video.get('view_count', 0)),
            'like_count': int(video.get('like_count', 0)),
            'share_count': int(video.get('share_count', 0)),
            'comment_count': int(video.get('comment_count', 0)),
            'created_at': video.get('upload_time', video.get('created_at', '')),
            'description': video.get('description', ''),
            'resolution': video.get('resolution', '1080p'),
            'language': video.get('language', 'en')
        }
    
    def get_random_interaction(self) -> Optional[Dict]:
        """Get a random user-video interaction"""
        if not self.interactions:
            return None
        
        import random
        interaction = random.choice(self.interactions)
        
        return {
            'user_id': str(interaction.get('user_id', '')),
            'video_id': str(interaction.get('video_id', interaction.get('item_id', ''))),
            'timestamp': interaction.get('timestamp', ''),
            'watch_time': float(interaction.get('watch_time', 0)),
            'interaction_type': interaction.get('type', 'view')
        }
    
    def get_video_by_id(self, video_id: str) -> Optional[Dict]:
        """Get specific video by ID"""
        for video in self.videos:
            if str(video.get('video_id', video.get('id', ''))) == video_id:
                return self.get_random_video()  # Use same conversion
        return None

class MicroLensDatasetLoader:
    """
    Loader for MicroLens dataset from Westlake University
    Dataset URL: https://recsys.westlake.edu.cn/MicroLens-100k-Dataset/
    GitHub: https://github.com/westlake-repl/MicroLens
    """
    
    def __init__(self, data_path: str = '/data/microlens'):
        self.data_path = Path(data_path)
        self.videos_df = None
        self.interactions_df = None
        self.users_df = None
        
    def load_dataset(self) -> bool:
        """Load MicroLens dataset from CSV files"""
        try:
            logger.info("Loading MicroLens dataset...")
            
            # Expected file structure based on dataset documentation
            # - videos.csv or items.csv: video metadata
            # - interactions.csv or ratings.csv: user interactions
            # - users.csv: user information (optional)
            
            # Try different possible filenames
            video_files = ['videos.csv', 'items.csv', 'video_features.csv']
            interaction_files = ['interactions.csv', 'ratings.csv', 'train.csv']
            user_files = ['users.csv', 'user_features.csv']
            
            # Load videos
            for fname in video_files:
                video_file = self.data_path / fname
                if video_file.exists():
                    self.videos_df = pd.read_csv(video_file)
                    logger.info(f"Loaded {len(self.videos_df)} videos from {fname}")
                    break
            
            if self.videos_df is None:
                logger.error("Video metadata file not found")
                return False
            
            # Load interactions
            for fname in interaction_files:
                interaction_file = self.data_path / fname
                if interaction_file.exists():
                    self.interactions_df = pd.read_csv(interaction_file)
                    logger.info(f"Loaded {len(self.interactions_df)} interactions from {fname}")
                    break
            
            # Load users (optional)
            for fname in user_files:
                user_file = self.data_path / fname
                if user_file.exists():
                    self.users_df = pd.read_csv(user_file)
                    logger.info(f"Loaded {len(self.users_df)} users from {fname}")
                    break
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading MicroLens dataset: {e}")
            return False
    
    def get_random_video(self) -> Optional[Dict]:
        """Get a random video from dataset"""
        if self.videos_df is None or len(self.videos_df) == 0:
            return None
        
        video = self.videos_df.sample(n=1).iloc[0]
        
        # Convert to standard format
        # Adjust field names based on actual dataset schema
        return {
            'video_id': str(video.get('video_id', video.get('item_id', video.name))),
            'title': video.get('title', video.get('name', 'Untitled')),
            'creator_id': str(video.get('creator_id', video.get('author_id', 'unknown'))),
            'category': video.get('category', video.get('genre', 'general')),
            'tags': self._parse_tags(video.get('tags', video.get('genres', ''))),
            'duration': float(video.get('duration', video.get('length', 30))),
            'view_count': int(video.get('view_count', video.get('views', 0))),
            'like_count': int(video.get('like_count', video.get('likes', 0))),
            'share_count': int(video.get('share_count', 0)),
            'comment_count': int(video.get('comment_count', 0)),
            'created_at': str(video.get('upload_time', video.get('created_at', ''))),
            'description': video.get('description', ''),
            'resolution': '1080p',
            'language': 'en'
        }
    
    def get_random_interaction(self) -> Optional[Dict]:
        """Get a random user-video interaction"""
        if self.interactions_df is None or len(self.interactions_df) == 0:
            return None
        
        interaction = self.interactions_df.sample(n=1).iloc[0]
        
        return {
            'user_id': str(interaction.get('user_id', '')),
            'video_id': str(interaction.get('video_id', interaction.get('item_id', ''))),
            'timestamp': str(interaction.get('timestamp', '')),
            'watch_time': float(interaction.get('watch_time', interaction.get('duration', 0))),
            'rating': float(interaction.get('rating', 1.0)),
            'interaction_type': self._get_interaction_type(interaction)
        }
    
    def _parse_tags(self, tags_str) -> List[str]:
        """Parse tags from string format"""
        if pd.isna(tags_str) or not tags_str:
            return []
        
        if isinstance(tags_str, str):
            # Handle different formats: "tag1|tag2" or "tag1,tag2" or "['tag1', 'tag2']"
            if '|' in tags_str:
                return tags_str.split('|')
            elif ',' in tags_str:
                return [t.strip() for t in tags_str.split(',')]
            else:
                return [tags_str]
        
        return []
    
    def _get_interaction_type(self, interaction) -> str:
        """Determine interaction type from rating or other fields"""
        if 'interaction_type' in interaction:
            return str(interaction['interaction_type'])
        
        # Infer from rating if available
        rating = interaction.get('rating', 0)
        if rating >= 4:
            return 'like'
        elif rating >= 3:
            return 'view'
        else:
            return 'skip'
    
    def get_video_by_id(self, video_id: str) -> Optional[Dict]:
        """Get specific video by ID"""
        if self.videos_df is None:
            return None
        
        video_col = 'video_id' if 'video_id' in self.videos_df.columns else 'item_id'
        video_row = self.videos_df[self.videos_df[video_col] == video_id]
        
        if len(video_row) == 0:
            return None
        
        return self.get_random_video()  # Use same conversion

class DatasetManager:
    """
    Manager to handle multiple datasets and provide unified interface
    """
    
    def __init__(self, dataset_type: str = 'fake', data_path: str = '/data'):
        self.dataset_type = dataset_type
        self.loader = None
        
        if dataset_type == 'shortvideo':
            self.loader = ShortVideoDatasetLoader(f"{data_path}/shortvideo")
        elif dataset_type == 'microlens':
            self.loader = MicroLensDatasetLoader(f"{data_path}/microlens")
        
    def initialize(self) -> bool:
        """Initialize the dataset"""
        if self.loader is None:
            return True  # Fake mode doesn't need initialization
        
        return self.loader.load_dataset()
    
    def get_random_video(self) -> Optional[Dict]:
        """Get a random video"""
        if self.loader is None:
            return None
        
        return self.loader.get_random_video()
    
    def get_random_interaction(self) -> Optional[Dict]:
        """Get a random interaction"""
        if self.loader is None:
            return None
        
        return self.loader.get_random_interaction()
    
    def is_available(self) -> bool:
        """Check if dataset is available"""
        if self.dataset_type == 'fake':
            return True
        
        return self.loader is not None and (
            (hasattr(self.loader, 'videos') and len(self.loader.videos) > 0) or
            (hasattr(self.loader, 'videos_df') and self.loader.videos_df is not None)
        )
