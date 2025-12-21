"""
Storage Layer - MongoDB and Redis Operations
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
import redis
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBStorage:
    """MongoDB storage operations"""
    
    def __init__(self, connection_uri: str, database: str = 'video_recommendation'):
        self.client = MongoClient(connection_uri)
        self.db = self.client[database]
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Ensure all necessary indexes exist"""
        try:
            # Users collection
            self.db.users.create_index([("user_id", ASCENDING)], unique=True)
            
            # Videos collection
            self.db.videos.create_index([("video_id", ASCENDING)], unique=True)
            self.db.videos.create_index([("category", ASCENDING)])
            self.db.videos.create_index([("engagement_score", DESCENDING)])
            
            # Interactions collection
            self.db.interactions.create_index([
                ("user_id", ASCENDING),
                ("timestamp", DESCENDING)
            ])
            self.db.interactions.create_index([("video_id", ASCENDING)])
            
            # User profiles collection
            self.db.user_profiles.create_index([("user_id", ASCENDING)], unique=True)
            
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def insert_user(self, user_data: Dict) -> bool:
        """Insert a new user"""
        try:
            self.db.users.insert_one(user_data)
            return True
        except DuplicateKeyError:
            logger.warning(f"User {user_data.get('user_id')} already exists")
            return False
    
    def insert_video(self, video_data: Dict) -> bool:
        """Insert a new video"""
        try:
            self.db.videos.insert_one(video_data)
            return True
        except DuplicateKeyError:
            logger.warning(f"Video {video_data.get('video_id')} already exists")
            return False
    
    def insert_interaction(self, interaction_data: Dict) -> bool:
        """Insert a user interaction"""
        try:
            self.db.interactions.insert_one(interaction_data)
            return True
        except Exception as e:
            logger.error(f"Error inserting interaction: {e}")
            return False
    
    def update_user_profile(self, user_id: str, profile_data: Dict) -> bool:
        """Update user profile"""
        try:
            self.db.user_profiles.update_one(
                {'user_id': user_id},
                {'$set': profile_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating user profile: {e}")
            return False
    
    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """Get user profile"""
        return self.db.user_profiles.find_one({'user_id': user_id})
    
    def get_video_metadata(self, video_id: str) -> Optional[Dict]:
        """Get video metadata"""
        return self.db.videos.find_one({'video_id': video_id})
    
    def get_user_interactions(self, user_id: str, limit: int = 100) -> List[Dict]:
        """Get user's recent interactions"""
        return list(self.db.interactions.find(
            {'user_id': user_id}
        ).sort('timestamp', DESCENDING).limit(limit))
    
    def get_trending_videos(self, hours: int = 24, limit: int = 20) -> List[Dict]:
        """Get trending videos based on recent interactions"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        pipeline = [
            {
                '$match': {
                    'timestamp': {'$gte': cutoff_time.isoformat()}
                }
            },
            {
                '$group': {
                    '_id': '$video_id',
                    'interaction_count': {'$sum': 1},
                    'total_weight': {'$sum': '$weight'}
                }
            },
            {
                '$sort': {'total_weight': -1}
            },
            {
                '$limit': limit
            }
        ]
        
        return list(self.db.interactions.aggregate(pipeline))
    
    def get_popular_by_category(self, category: str, limit: int = 20) -> List[Dict]:
        """Get popular videos in a specific category"""
        return list(self.db.videos.find(
            {'category': category}
        ).sort('engagement_score', DESCENDING).limit(limit))

class RedisCache:
    """Redis caching operations"""
    
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
    
    def cache_user_recommendations(self, user_id: str, recommendations: List[Dict], ttl: int = 300):
        """Cache user recommendations (5 minutes default)"""
        key = f"recommendations:{user_id}"
        self.client.setex(
            key,
            ttl,
            json.dumps(recommendations)
        )
    
    def get_cached_recommendations(self, user_id: str) -> Optional[List[Dict]]:
        """Get cached recommendations"""
        key = f"recommendations:{user_id}"
        data = self.client.get(key)
        
        if data:
            return json.loads(data)
        return None
    
    def add_user_interaction(self, user_id: str, video_id: str, weight: float):
        """Add user interaction to sorted set"""
        key = f"user:{user_id}:recent_videos"
        self.client.zadd(key, {video_id: weight})
        self.client.expire(key, 86400)  # 24 hours
    
    def get_user_recent_videos(self, user_id: str, limit: int = 50) -> List[tuple]:
        """Get user's recent videos"""
        key = f"user:{user_id}:recent_videos"
        return self.client.zrange(key, 0, limit-1, desc=True, withscores=True)
    
    def update_video_stats(self, video_id: str, stat_type: str, increment: int = 1):
        """Update video statistics"""
        key = f"video:{video_id}:stats"
        self.client.hincrby(key, stat_type, increment)
        self.client.expire(key, 86400)
    
    def get_video_stats(self, video_id: str) -> Dict:
        """Get video statistics"""
        key = f"video:{video_id}:stats"
        stats = self.client.hgetall(key)
        return {k: int(v) for k, v in stats.items()}
    
    def update_trending(self, video_id: str, score: float):
        """Update trending videos sorted set"""
        key = "trending:videos:24h"
        self.client.zadd(key, {video_id: score})
        self.client.expire(key, 3600)  # 1 hour
    
    def get_trending(self, limit: int = 20) -> List[tuple]:
        """Get trending videos"""
        key = "trending:videos:24h"
        return self.client.zrange(key, 0, limit-1, desc=True, withscores=True)
    
    def cache_user_profile(self, user_id: str, profile: Dict, ttl: int = 600):
        """Cache user profile (10 minutes default)"""
        key = f"profile:{user_id}"
        self.client.setex(key, ttl, json.dumps(profile))
    
    def get_cached_profile(self, user_id: str) -> Optional[Dict]:
        """Get cached user profile"""
        key = f"profile:{user_id}"
        data = self.client.get(key)
        
        if data:
            return json.loads(data)
        return None
    
    def increment_counter(self, counter_name: str, value: int = 1):
        """Increment a counter"""
        self.client.incr(counter_name, value)
    
    def get_counter(self, counter_name: str) -> int:
        """Get counter value"""
        value = self.client.get(counter_name)
        return int(value) if value else 0

class StorageManager:
    """Unified storage manager combining MongoDB and Redis"""
    
    def __init__(self, mongodb_uri: str, redis_host: str = 'localhost', redis_port: int = 6379):
        self.mongodb = MongoDBStorage(mongodb_uri)
        self.redis = RedisCache(redis_host, redis_port)
    
    def get_or_create_user_profile(self, user_id: str) -> Dict:
        """Get user profile from cache or database"""
        # Try cache first
        profile = self.redis.get_cached_profile(user_id)
        if profile:
            return profile
        
        # Try database
        profile = self.mongodb.get_user_profile(user_id)
        if profile:
            # Cache it
            self.redis.cache_user_profile(user_id, profile)
            return profile
        
        # Create new profile
        new_profile = {
            'user_id': user_id,
            'total_interactions': 0,
            'favorite_categories': [],
            'recent_videos': [],
            'created_at': datetime.utcnow().isoformat()
        }
        
        self.mongodb.update_user_profile(user_id, new_profile)
        self.redis.cache_user_profile(user_id, new_profile)
        
        return new_profile
    
    def record_interaction(self, user_id: str, video_id: str, event_type: str, weight: float):
        """Record a user interaction"""
        interaction = {
            'user_id': user_id,
            'video_id': video_id,
            'event_type': event_type,
            'weight': weight,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Store in MongoDB
        self.mongodb.insert_interaction(interaction)
        
        # Update Redis cache
        self.redis.add_user_interaction(user_id, video_id, weight)
        self.redis.update_video_stats(video_id, event_type, 1)
        
        # Invalidate cached recommendations
        self.redis.client.delete(f"recommendations:{user_id}")
