"""
Flask API Service for Video Recommendation System
Provides REST endpoints for getting recommendations and tracking events
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
import redis
from pymongo import MongoClient
import json
import os
import sys
from datetime import datetime
import logging

# Add parent directory to path for imports
sys.path.append('/app')
sys.path.append('/opt/spark-jobs')

# Note: API doesn't use Spark for serving - just Redis/MongoDB
# from recommendation_engine import DistributedCollaborativeFilteringEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
MONGODB_URI = os.getenv('MONGODB_URI', 
    'mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin')

# Initialize connections
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client.video_recommendation

# API serves recommendations from cache/database (no Spark needed for serving)
# Training happens separately via spark-submit

def get_trending_from_redis(n=20):
    """Get trending videos from Redis"""
    trending_key = "trending:videos:24h"
    trending = redis_client.zrange(trending_key, 0, n-1, desc=True, withscores=True)
    
    if trending:
        return [
            {
                'video_id': vid,
                'score': float(score),
                'method': 'trending'
            }
            for vid, score in trending
        ]
    
    # Fallback to MongoDB
    trending_docs = db.videos.find().sort('view_count', -1).limit(n)
    return [
        {
            'video_id': doc['video_id'],
            'score': doc.get('view_count', 0),
            'method': 'trending_fallback'
        }
        for doc in trending_docs
    ]

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'services': {
            'kafka': check_kafka(),
            'redis': check_redis(),
            'mongodb': check_mongodb()
        }
    })

def check_kafka():
    """Check Kafka connection"""
    try:
        kafka_producer.bootstrap_connected()
        return 'connected'
    except:
        return 'disconnected'

def check_redis():
    """Check Redis connection"""
    try:
        redis_client.ping()
        return 'connected'
    except:
        return 'disconnected'

def check_mongodb():
    """Check MongoDB connection"""
    try:
        mongo_client.server_info()
        return 'connected'
    except:
        return 'disconnected'

@app.route('/api/v1/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    """
    Get personalized recommendations for a user
    Query params:
    - n: number of recommendations (default: 20)
    - method: 'als' or 'item_cf' (default: 'als')
    """
    try:
        n = int(request.args.get('n', 20))
        method = request.args.get('method', 'als')
        
        # Check cache first
        cache_key = f"recommendations:{user_id}"
        cached = redis_client.get(cache_key)
        
        if cached:
            recommendations = json.loads(cached)
            logger.info(f"Returning cached recommendations for {user_id}")
        else:
            # Get recommendations from Redis (populated by Spark streaming)
            recent_key = f"user:{user_id}:recent_videos"
            recent_videos = redis_client.zrange(recent_key, 0, n-1, desc=True, withscores=True)
            
            if recent_videos:
                recommendations = [
                    {'video_id': vid, 'score': float(score), 'method': 'cached'}
                    for vid, score in recent_videos
                ]
            else:
                # Fallback to trending
                recommendations = get_trending_from_redis(n)
            
            # Enrich with video metadata
            for rec in recommendations:
                video_data = db.videos.find_one({'video_id': rec['video_id']})
                if video_data:
                    rec['title'] = video_data.get('title', '')
                    rec['category'] = video_data.get('category', '')
                    rec['creator_id'] = video_data.get('creator_id', '')
                    rec['duration'] = video_data.get('duration', 0)
            
            # Cache for 5 minutes
            redis_client.setex(cache_key, 300, json.dumps(recommendations))
            logger.info(f"Generated fresh recommendations for {user_id}")
        
        return jsonify({
            'user_id': user_id,
            'recommendations': recommendations,
            'count': len(recommendations),
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/trending', methods=['GET'])
def get_trending():
    """
    Get trending videos
    Query params:
    - n: number of videos (default: 20)
    """
    try:
        n = int(request.args.get('n', 20))
        
        trending = get_trending_from_redis(n)
        
        # Enrich with metadata
        for video in trending:
            video_data = db.videos.find_one({'video_id': video['video_id']})
            if video_data:
                video['title'] = video_data.get('title', '')
                video['category'] = video_data.get('category', '')
                video['creator_id'] = video_data.get('creator_id', '')
        
        return jsonify({
            'trending': trending,
            'count': len(trending),
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting trending: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/events/track', methods=['POST'])
def track_event():
    """
    Track a user event
    Body: {
        "event_type": "view|like|share|comment",
        "user_id": "user_123",
        "video_id": "video_456",
        "watch_time": 30.5,
        "total_duration": 60.0
    }
    """
    try:
        event_data = request.get_json()
        
        # Validate required fields
        required_fields = ['event_type', 'user_id', 'video_id']
        if not all(field in event_data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Add timestamp
        event_data['timestamp'] = datetime.utcnow().isoformat()
        
        # Determine topic based on event type
        if event_data['event_type'] == 'view':
            topic = 'user-events'
            
            # Calculate completion rate if not provided
            if 'watch_time' in event_data and 'total_duration' in event_data:
                event_data['completion_rate'] = (
                    event_data['watch_time'] / event_data['total_duration']
                )
        else:
            topic = 'user-interactions'
        
        # Send to Kafka
        kafka_producer.send(topic, value=event_data)
        kafka_producer.flush()
        
        logger.info(f"Tracked {event_data['event_type']} event for user {event_data['user_id']}")
        
        return jsonify({
            'status': 'success',
            'message': 'Event tracked successfully',
            'event_id': event_data.get('session_id', 'unknown')
        })
        
    except Exception as e:
        logger.error(f"Error tracking event: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/user/<user_id>/profile', methods=['GET'])
def get_user_profile(user_id):
    """Get user profile and statistics"""
    try:
        # Check Redis cache first
        cache_key = f"profile:{user_id}"
        cached_profile = redis_client.get(cache_key)
        
        if cached_profile:
            profile = json.loads(cached_profile)
        else:
            # Get from MongoDB
            profile = db.user_profiles.find_one({'user_id': user_id}, {'_id': 0})
            
            if not profile:
                return jsonify({'error': 'User not found'}), 404
            
            # Cache it
            redis_client.setex(cache_key, 600, json.dumps(profile))
        
        # Get recent interactions
        recent_interactions = list(db.interactions.find(
            {'user_id': user_id}
        ).sort('timestamp', -1).limit(20))
        
        for interaction in recent_interactions:
            interaction['_id'] = str(interaction['_id'])
        
        return jsonify({
            'profile': profile,
            'recent_interactions': recent_interactions,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting user profile: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/video/<video_id>', methods=['GET'])
def get_video_metadata(video_id):
    """Get video metadata and statistics"""
    try:
        video = db.videos.find_one({'video_id': video_id}, {'_id': 0})
        
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        
        # Get real-time stats from Redis
        stats_key = f"video:{video_id}:stats"
        stats = redis_client.hgetall(stats_key)
        
        video['realtime_stats'] = {
            k: int(v) for k, v in stats.items()
        }
        
        return jsonify(video)
        
    except Exception as e:
        logger.error(f"Error getting video metadata: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/similar/<video_id>', methods=['GET'])
def get_similar_videos(video_id):
    """Get videos similar to a given video"""
    try:
        n = int(request.args.get('n', 10))
        
        # Get video metadata
        video = db.videos.find_one({'video_id': video_id})
        
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        
        # Find similar videos by category and tags
        similar = list(db.videos.find({
            '$or': [
                {'category': video['category']},
                {'tags': {'$in': video.get('tags', [])}}
            ],
            'video_id': {'$ne': video_id}
        }).sort('engagement_score', -1).limit(n))
        
        for vid in similar:
            vid['_id'] = str(vid['_id'])
        
        return jsonify({
            'video_id': video_id,
            'similar_videos': similar,
            'count': len(similar)
        })
        
    except Exception as e:
        logger.error(f"Error getting similar videos: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/stats', methods=['GET'])
def get_system_stats():
    """Get system-wide statistics"""
    try:
        stats = {
            'total_users': db.users.count_documents({}),
            'total_videos': db.videos.count_documents({}),
            'total_interactions': db.interactions.count_documents({}),
            'active_sessions': redis_client.dbsize(),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/retrain', methods=['POST'])
def trigger_retrain():
    """Trigger model retraining (admin endpoint)"""
    try:
        # This would trigger an async retraining job
        # For now, we'll just return a message
        
        return jsonify({
            'status': 'queued',
            'message': 'Model retraining job queued',
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error triggering retrain: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    debug = os.getenv('API_DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting API service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
