"""
Collaborative Filtering Recommendation Engine
Uses item-based and user-based collaborative filtering with implicit feedback
"""

import numpy as np
from scipy.sparse import csr_matrix
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking
from sklearn.metrics.pairwise import cosine_similarity
import redis
from pymongo import MongoClient
import pickle
import os

class CollaborativeFilteringEngine:
    """
    Hybrid Collaborative Filtering Engine for Video Recommendation
    Combines ALS and item-based CF for better recommendations
    """
    
    def __init__(self, redis_host='redis', redis_port=6379, mongodb_uri=None):
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=False
        )
        
        if mongodb_uri:
            self.mongo_client = MongoClient(mongodb_uri)
            self.db = self.mongo_client.video_recommendation
        
        # ALS model for matrix factorization
        self.als_model = AlternatingLeastSquares(
            factors=64,
            regularization=0.01,
            iterations=15,
            calculate_training_loss=True
        )
        
        # User and item mappings
        self.user_id_map = {}
        self.video_id_map = {}
        self.reverse_user_map = {}
        self.reverse_video_map = {}
        
        # Similarity matrices
        self.item_similarity = None
        
    def build_interaction_matrix(self, min_interactions=5):
        """
        Build user-item interaction matrix from MongoDB
        """
        print("Building interaction matrix from MongoDB...")
        
        # Fetch interactions from MongoDB
        interactions = list(self.db.interactions.find(
            {},
            {'user_id': 1, 'video_id': 1, 'weight': 1}
        ))
        
        if not interactions:
            print("No interactions found!")
            return None
        
        # Create mappings
        unique_users = list(set([i['user_id'] for i in interactions]))
        unique_videos = list(set([i['video_id'] for i in interactions]))
        
        self.user_id_map = {uid: idx for idx, uid in enumerate(unique_users)}
        self.video_id_map = {vid: idx for idx, vid in enumerate(unique_videos)}
        self.reverse_user_map = {idx: uid for uid, idx in self.user_id_map.items()}
        self.reverse_video_map = {idx: vid for vid, idx in self.video_id_map.items()}
        
        # Build sparse matrix
        n_users = len(unique_users)
        n_videos = len(unique_videos)
        
        rows = []
        cols = []
        data = []
        
        for interaction in interactions:
            user_idx = self.user_id_map.get(interaction['user_id'])
            video_idx = self.video_id_map.get(interaction['video_id'])
            weight = interaction.get('weight', 1.0)
            
            if user_idx is not None and video_idx is not None:
                rows.append(user_idx)
                cols.append(video_idx)
                data.append(weight)
        
        interaction_matrix = csr_matrix(
            (data, (rows, cols)), 
            shape=(n_users, n_videos)
        )
        
        print(f"Matrix shape: {interaction_matrix.shape}")
        print(f"Sparsity: {1 - (interaction_matrix.nnz / (n_users * n_videos)):.4f}")
        
        return interaction_matrix
    
    def train_als_model(self, interaction_matrix):
        """Train ALS model on interaction matrix"""
        print("Training ALS model...")
        
        # ALS expects item-user matrix
        item_user_matrix = interaction_matrix.T.tocsr()
        
        self.als_model.fit(item_user_matrix)
        print("ALS model trained successfully!")
        
        # Save model
        self.save_model()
    
    def calculate_item_similarity(self, interaction_matrix):
        """Calculate item-item similarity matrix"""
        print("Calculating item-item similarity...")
        
        # Normalize by user
        normalized_matrix = interaction_matrix.copy()
        
        # Calculate cosine similarity between items
        self.item_similarity = cosine_similarity(
            normalized_matrix.T, 
            dense_output=False
        )
        
        print(f"Item similarity matrix shape: {self.item_similarity.shape}")
    
    def get_user_recommendations(self, user_id, n=20, use_als=True):
        """
        Get recommendations for a specific user
        
        Args:
            user_id: User ID
            n: Number of recommendations
            use_als: Use ALS model or item-based CF
        """
        user_idx = self.user_id_map.get(user_id)
        
        if user_idx is None:
            # Cold start: return trending videos
            return self.get_trending_videos(n)
        
        if use_als:
            # Get recommendations from ALS model
            video_scores = self.als_model.recommend(
                user_idx,
                None,  # Use all user interactions
                N=n,
                filter_already_liked_items=True
            )
            
            recommendations = [
                {
                    'video_id': self.reverse_video_map[vid_idx],
                    'score': float(score),
                    'method': 'als'
                }
                for vid_idx, score in video_scores
            ]
        else:
            # Item-based collaborative filtering
            recommendations = self._item_based_recommendations(user_id, n)
        
        return recommendations
    
    def _item_based_recommendations(self, user_id, n=20):
        """Item-based collaborative filtering recommendations"""
        
        # Get user's recent interactions from Redis
        redis_key = f"user:{user_id}:recent_videos"
        recent_videos = self.redis_client.zrange(redis_key, 0, -1, withscores=True)
        
        if not recent_videos:
            return self.get_trending_videos(n)
        
        # Calculate scores for all videos based on similarity
        video_scores = {}
        
        for video_id, weight in recent_videos:
            video_id = video_id.decode('utf-8')
            video_idx = self.video_id_map.get(video_id)
            
            if video_idx is not None and self.item_similarity is not None:
                # Get similar videos
                similar_videos = self.item_similarity[video_idx].toarray().flatten()
                
                for idx, similarity in enumerate(similar_videos):
                    if similarity > 0.3:  # Threshold
                        vid = self.reverse_video_map[idx]
                        if vid != video_id:  # Don't recommend same video
                            video_scores[vid] = video_scores.get(vid, 0) + similarity * weight
        
        # Sort and return top N
        sorted_videos = sorted(
            video_scores.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:n]
        
        recommendations = [
            {
                'video_id': vid,
                'score': float(score),
                'method': 'item_cf'
            }
            for vid, score in sorted_videos
        ]
        
        return recommendations
    
    def get_trending_videos(self, n=20):
        """Get trending videos from Redis"""
        trending_key = "trending:videos:24h"
        trending = self.redis_client.zrange(trending_key, 0, n-1, desc=True, withscores=True)
        
        if not trending:
            # Fallback to MongoDB
            trending_docs = self.db.videos.find().sort('view_count', -1).limit(n)
            return [
                {
                    'video_id': doc['video_id'],
                    'score': doc.get('view_count', 0),
                    'method': 'trending_fallback'
                }
                for doc in trending_docs
            ]
        
        return [
            {
                'video_id': vid.decode('utf-8'),
                'score': float(score),
                'method': 'trending'
            }
            for vid, score in trending
        ]
    
    def update_trending_videos(self):
        """Update trending videos based on recent activity"""
        # Aggregate views/interactions from last 24 hours
        from datetime import datetime, timedelta
        
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        pipeline = [
            {
                '$match': {
                    'timestamp': {'$gte': cutoff_time.isoformat()}
                }
            },
            {
                '$group': {
                    '_id': '$video_id',
                    'total_score': {'$sum': '$weight'}
                }
            },
            {
                '$sort': {'total_score': -1}
            },
            {
                '$limit': 100
            }
        ]
        
        trending = list(self.db.interactions.aggregate(pipeline))
        
        # Update Redis
        trending_key = "trending:videos:24h"
        self.redis_client.delete(trending_key)
        
        for item in trending:
            self.redis_client.zadd(
                trending_key,
                {item['_id']: item['total_score']}
            )
        
        self.redis_client.expire(trending_key, 3600)  # 1 hour
        
        print(f"Updated {len(trending)} trending videos")
    
    def save_model(self, path='/opt/data/models'):
        """Save trained model and mappings"""
        os.makedirs(path, exist_ok=True)
        
        # Save ALS model
        with open(f'{path}/als_model.pkl', 'wb') as f:
            pickle.dump(self.als_model, f)
        
        # Save mappings
        with open(f'{path}/mappings.pkl', 'wb') as f:
            pickle.dump({
                'user_id_map': self.user_id_map,
                'video_id_map': self.video_id_map,
                'reverse_user_map': self.reverse_user_map,
                'reverse_video_map': self.reverse_video_map
            }, f)
        
        print(f"Model saved to {path}")
    
    def load_model(self, path='/opt/data/models'):
        """Load trained model and mappings"""
        try:
            with open(f'{path}/als_model.pkl', 'rb') as f:
                self.als_model = pickle.load(f)
            
            with open(f'{path}/mappings.pkl', 'rb') as f:
                mappings = pickle.load(f)
                self.user_id_map = mappings['user_id_map']
                self.video_id_map = mappings['video_id_map']
                self.reverse_user_map = mappings['reverse_user_map']
                self.reverse_video_map = mappings['reverse_video_map']
            
            print(f"Model loaded from {path}")
            return True
        except Exception as e:
            print(f"Error loading model: {e}")
            return False

def train_recommendation_model():
    """Training script to be run periodically"""
    mongodb_uri = os.getenv('MONGODB_URI', 
        'mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin')
    
    engine = CollaborativeFilteringEngine(mongodb_uri=mongodb_uri)
    
    # Build interaction matrix
    interaction_matrix = engine.build_interaction_matrix()
    
    if interaction_matrix is not None:
        # Train ALS model
        engine.train_als_model(interaction_matrix)
        
        # Calculate item similarity
        engine.calculate_item_similarity(interaction_matrix)
        
        # Update trending videos
        engine.update_trending_videos()
        
        print("Training completed successfully!")
    else:
        print("Not enough data to train model")

if __name__ == "__main__":
    train_recommendation_model()
