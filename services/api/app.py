"""
Flask API Service for Video Recommendation System
Provides REST endpoints for getting recommendations and tracking events
"""

from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS
import json
import os
import sys
from datetime import datetime
import logging
from pathlib import Path

# Optional imports - gracefully handle missing dependencies
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Warning: kafka-python not available. Event tracking will be disabled.")

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("Warning: redis not available. Caching will be disabled.")

try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    print("Warning: pymongo not available. Database features will be disabled.")

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

# Video storage paths
VIDEOS_DIR = Path('/data/MicroLens-100k/videos') if os.path.exists('/data') else Path('../../data/MicroLens-100k/videos')
COVERS_DIR = Path('/data/MicroLens-100k/covers') if os.path.exists('/data') else Path('../../data/MicroLens-100k/covers')

# Initialize connections
kafka_producer = None
if KAFKA_AVAILABLE:
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Warning: Could not connect to Kafka: {e}")

redis_client = None
if REDIS_AVAILABLE:
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    if redis_client:
        try:
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
        except Exception as e:
            print(f"Redis error: {e}")
    
    # Fallback to MongoDB
    if db:
        try:
            trending_docs = db.videos.find().sort('view_count', -1).limit(n)
            return [
                {
                    'video_id': doc['video_id'],
                    'score': doc.get('view_count', 0),
                    'method': 'trending_fallback'
                }
                for doc in trending_docs
            ]
        except Exception as e:
            print(f"MongoDB error: {e}")
    
    # Return mock data if no services available
    return [        {
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
    if not KAFKA_AVAILABLE or not kafka_producer:
        return 'unavailable'
    try:
        kafka_producer.bootstrap_connected()
        return 'connected'
    except:
        return 'disconnected'

def check_redis():
    """Check Redis connection"""
    if not REDIS_AVAILABLE or not redis_client:
        return 'unavailable'
    try:
        redis_client.ping()
        return 'connected'
    except:
        return 'disconnected'

def check_mongodb():
    """Check MongoDB connection"""
    if not MONGODB_AVAILABLE or not mongo_client:
        return 'unavailable'

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
        recommendations = []
        
        # Check cache first
        if redis_client:
            try:
                cache_key = f"recommendations:{user_id}"
                cached = redis_client.get(cache_key)
                
                if cached:
                    recommendations = json.loads(cached)
                    logger.info(f"Returning cached recommendations for {user_id}")
            except Exception as e:
                logger.warning(f"Redis error: {e}")
        
        if not recommendations:
            # Get recommendations from Redis (populated by Spark streaming)
            if redis_client:
                try:
                    recent_key = f"user:{user_id}:recent_videos"
                    recent_videos = redis_client.zrange(recent_key, 0, n-1, desc=True, withscores=True)
                    
                    if recent_videos:
                        recommendations = [
                            {'video_id': vid, 'score': float(score), 'method': 'cached'}
                            for vid, score in recent_videos
                        ]
                except Exception as e:
                    logger.warning(f"Redis error: {e}")
            
            if not recommendations:
                # Fallback to trending
                recommendations = get_trending_from_redis(n)
            
            # Enrich with video metadata
            if db:
                try:
                    for rec in recommendations:
                        video_data = db.videos.find_one({'video_id': rec['video_id']})
                        if video_data:
                            rec['title'] = video_data.get('title', '')
                            rec['category'] = video_data.get('category', '')
                            rec['creator_id'] = video_data.get('creator_id', '')
                            rec['duration'] = video_data.get('duration', 0)
                except Exception as e:
                    logger.warning(f"MongoDB error: {e}")
            
            # Cache for 5 minutes
            if redis_client and recommendations:
                try:
                    redis_client.setex(cache_key, 300, json.dumps(recommendations))
                except:
                    passhed'}
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
        if db:
            try:
                for video in trending:
                    video_data = db.videos.find_one({'video_id': video['video_id']})
                    if video_data:
                        video['title'] = video_data.get('title', '')
                        video['category'] = video_data.get('category', '')
                        video['creator_id'] = video_data.get('creator_id', '')
            except Exception as e:
                logger.warning(f"MongoDB error: {e}"
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
    """ if available
        if kafka_producer:
            try:
                kafka_producer.send(topic, value=event_data)
                kafka_producer.flush()
            except Exception as e:
                logger.warning(f"Kafka error: {e}"get_json()
        
        # Validate required fields
        required_fields = ['event_type', 'user_id', 'video_id']
        if not all(field in event_data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Add timestamp
        event_data['timestamp'] = datetime.utcnow().isoformat()
        
        # Determine topic based on event type
        if event_data['event_type'] == 'view':
            topic = 'user-events'
            
        profile = None
        
        # Check Redis cache first
        if redis_client:
            try:
                cache_key = f"profile:{user_id}"
                cached_profile = redis_client.get(cache_key)
                if cached_profile:
                    profile = json.loads(cached_profile)
            except Exception as e:
                logger.warning(f"Redis error: {e}")
        
        if not profile and db:
            try:
                # Get from MongoDB
                profile = db.user_profiles.find_one({'user_id': user_id}, {'_id': 0})
                
                if not profile:
                    return jsonify({'error': 'User not found'}), 404
                
                # Cache it
                if redis_client:
                    try:
                        redis_client.setex(cache_key, 600, json.dumps(profile))
                    except:
                        pass
            except Exception as e:
                logger.warning(f"MongoDB error: {e}")
                return jsonify({'error': 'Database unavailable'}), 503
        
        if not profile:
            return jsonify({'error': 'User not found'}), 404
        
        # Get recent interactions
        recent_interactions = []
        if db:
            try:
                recent_interactions = list(db.interactions.find(
                    {'user_id': user_id}
                ).sort('timestamp', -1).limit(20))
                
                for interaction in recent_interactions:
                    interaction['_id'] = str(interaction['_id'])
            except Exception as e:
                logger.warning(f"MongoDB error: {e}"

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
            profNone
        
        if db:
            try:
                video = db.videos.find_one({'video_id': video_id}, {'_id': 0})
            except Exception as e:
                logger.warning(f"MongoDB error: {e}")
        
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        
        # Get real-time stats from Redis
        if redis_client:
            try:
                stats_key = f"video:{video_id}:stats"
                stats = redis_client.hgetall(stats_key)
                
                video['realtime_stats'] = {
                    k: int(v) for k, v in stats.items()
                }
            except Exception as e:
                logger.warning(f"Redis error: {e}")
                video['realtime_stats'] = {).sort('timestamp', -1).limit(20))
        
        for interaction in recent_interactions:
        if not db:
            return jsonify({'error': 'Database unavailable'}), 503
        
        # Get video metadata
        try:
            video = db.videos.find_one({'video_id': video_id})
        except Exception as e:
            logger.warning(f"MongoDB error: {e}")
            return jsonify({'error': 'Database error'}), 500
        
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        
        # Find similar videos by category and tags
        similar = []
        try:
            similar = list(db.videos.find({
                '$or': [
                    {'category': video['category']},
                    {'tags': {'$in': video.get('tags', [])}}
                ],
                'video_id': {'$ne': video_id}
            }).sort('engagement_score', -1).limit(n))
            
            for vid in similar:
                vid['_id'] = str(vid['_id'])
        except Exception as e:
            logger.warning(f"MongoDB error: {e}"deo_id': video_id}, {'_id': 0})
        
        if not video:
            return jsonify({'error': 'Video not found'}), 404
        
        # Get real-time stats from Redis
        stats_key = f"video:{video_id}:stats"
        stats = redis_client.hgetall(stats_key)
        
        video['realtime_stats'] = {
            k: int(v) for k0,
            'total_videos': 0,
            'total_interactions': 0,
            'active_sessions': 0,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if db:
            try:
                stats['total_users'] = db.users.count_documents({})
                stats['total_videos'] = db.videos.count_documents({})
                stats['total_interactions'] = db.interactions.count_documents({})
            except Exception as e:
                logger.warning(f"MongoDB error: {e}")
        
        if redis_client:
            try:
                stats['active_sessions'] = redis_client.dbsize()
            except Exception as e:
                logger.warning(f"Redis error: {e}")t Exception as e:
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

@app.route('/api/v1/videos/<video_id>/stream', methods=['GET'])
def stream_video(video_id):
    """Stream video file"""
    try:
        video_path = VIDEOS_DIR / f"{video_id}.mp4"
        
        if not video_path.exists():
            return jsonify({'error': 'Video file not found'}), 404
        
        # Support range requests for video seeking
        file_size = video_path.stat().st_size
        range_header = request.headers.get('Range', None)
        
        if range_header:
            byte_range = range_header.replace('bytes=', '').split('-')
            start = int(byte_range[0])
            end = int(byte_range[1]) if byte_range[1] else file_size - 1
            length = end - start + 1
            
            with open(video_path, 'rb') as f:
                f.seek(start)
                data = f.read(length)
            
            response = Response(
                data,
                206,  # Partial Content
                mimetype='video/mp4',
                direct_passthrough=True
            )
            response.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
            response.headers.add('Accept-Ranges', 'bytes')
            response.headers.add('Content-Length', str(length))
            return response
        else:
            return send_from_directory(VIDEOS_DIR, f"{video_id}.mp4", mimetype='video/mp4')
        
    except Exception as e:
        logger.error(f"Error streaming video: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/videos/<video_id>/cover', methods=['GET'])
def get_video_cover(video_id):
    """Get video thumbnail/cover"""
    try:
        cover_path = COVERS_DIR / f"{video_id}.jpg"
        
        if not cover_path.exists():
            # Return a default placeholder
            return jsonify({'error': 'Cover not found'}), 404
        
        return send_from_directory(COVERS_DIR, f"{video_id}.jpg", mimetype='image/jpeg')
        
    except Exception as e:
        logger.error(f"Error getting cover: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/videos/list', methods=['GET'])
def list_available_videos():
    """List all available videos"""
    try:
        if not VIDEOS_DIR.exists():
            return jsonify({'videos': [], 'count': 0})
        
        video_files = list(VIDEOS_DIR.glob('*.mp4'))
        video_ids = [f.stem for f in video_files]
        
        # Get metadata from database
        videos = []
        for video_id in video_ids:
            video_data = db.videos.find_one({'video_id': video_id}, {'_id': 0})
            if video_data:
                video_data['has_file'] = True
                video_data['stream_url'] = f"/api/v1/videos/{video_id}/stream"
                video_data['cover_url'] = f"/api/v1/videos/{video_id}/cover"
                videos.append(video_data)
            else:
                # Video file exists but no metadata
                videos.append({
                    'video_id': video_id,
                    'has_file': True,
                    'stream_url': f"/api/v1/videos/{video_id}/stream",
                    'cover_url': f"/api/v1/videos/{video_id}/cover"
                })
        
        return jsonify({
            'videos': videos,
            'count': len(videos),
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error listing videos: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    debug = os.getenv('API_DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting API service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
