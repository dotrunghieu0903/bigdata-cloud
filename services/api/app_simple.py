"""
Simple Flask API for Video Streaming
Minimal version without Kafka/Redis/MongoDB dependencies
"""

from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS
import json
import os
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Video storage paths
VIDEOS_DIR = Path('../../data/MicroLens-100k/videos')
COVERS_DIR = Path('../../data/MicroLens-100k/covers')
DATA_DIR = Path('../../data/MicroLens-100k')

# Ensure absolute paths
VIDEOS_DIR = VIDEOS_DIR.resolve()
COVERS_DIR = COVERS_DIR.resolve()
DATA_DIR = DATA_DIR.resolve()

logger.info(f"Videos directory: {VIDEOS_DIR}")
logger.info(f"Covers directory: {COVERS_DIR}")

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'videos_available': VIDEOS_DIR.exists(),
        'covers_available': COVERS_DIR.exists()
    })

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
        
        # Load metadata from CSV if available
        metadata = {}
        title_file = DATA_DIR / 'MicroLens-100k_title_en.csv'
        if title_file.exists():
            import pandas as pd
            try:
                df = pd.read_csv(title_file)
                # Assuming columns: item, title
                for _, row in df.iterrows():
                    metadata[str(row.get('item', row.get('videoID', '')))] = {
                        'title': row.get('title', row.get('Title', ''))
                    }
            except Exception as e:
                logger.warning(f"Could not load titles: {e}")
        
        videos = []
        for video_file in video_files:
            video_id = video_file.stem
            video_data = {
                'video_id': video_id,
                'has_file': True,
                'stream_url': f"/api/v1/videos/{video_id}/stream",
                'cover_url': f"/api/v1/videos/{video_id}/cover",
                'file_size': video_file.stat().st_size
            }
            
            # Add metadata if available
            if video_id in metadata:
                video_data.update(metadata[video_id])
            
            videos.append(video_data)
        
        return jsonify({
            'videos': videos,
            'count': len(videos),
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error listing videos: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    """Get recommendations - returns available videos as fallback"""
    try:
        n = int(request.args.get('n', 20))
        
        # For now, just return random available videos
        if not VIDEOS_DIR.exists():
            return jsonify({'recommendations': [], 'count': 0})
        
        import random
        video_files = list(VIDEOS_DIR.glob('*.mp4'))
        random.shuffle(video_files)
        
        recommendations = []
        for video_file in video_files[:n]:
            video_id = video_file.stem
            recommendations.append({
                'video_id': video_id,
                'score': random.uniform(0.5, 1.0),
                'method': 'random',
                'stream_url': f"/api/v1/videos/{video_id}/stream",
                'cover_url': f"/api/v1/videos/{video_id}/cover"
            })
        
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
    """Get trending videos - returns available videos"""
    try:
        n = int(request.args.get('n', 20))
        
        if not VIDEOS_DIR.exists():
            return jsonify({'trending': [], 'count': 0})
        
        video_files = list(VIDEOS_DIR.glob('*.mp4'))
        
        trending = []
        for video_file in video_files[:n]:
            video_id = video_file.stem
            trending.append({
                'video_id': video_id,
                'score': len(video_files) - len(trending),  # Simple ranking
                'method': 'available',
                'stream_url': f"/api/v1/videos/{video_id}/stream",
                'cover_url': f"/api/v1/videos/{video_id}/cover"
            })
        
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
    """Track event - just log it for now"""
    try:
        event_data = request.get_json()
        logger.info(f"Event tracked: {event_data.get('event_type')} - User: {event_data.get('user_id')} - Video: {event_data.get('video_id')}")
        
        return jsonify({
            'status': 'success',
            'message': 'Event logged (not persisted)'
        })
        
    except Exception as e:
        logger.error(f"Error tracking event: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/stats', methods=['GET'])
def get_system_stats():
    """Get system statistics"""
    try:
        video_count = len(list(VIDEOS_DIR.glob('*.mp4'))) if VIDEOS_DIR.exists() else 0
        
        return jsonify({
            'total_users': 0,
            'total_videos': video_count,
            'total_interactions': 0,
            'active_sessions': 0,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

# Serve static files
@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

@app.route('/')
def index():
    """Redirect to video player"""
    return send_from_directory('static', 'videos.html')

if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))
    
    logger.info(f"Starting Simple Video API on port {port}")
    logger.info(f"Video streaming endpoint: http://localhost:{port}/api/v1/videos/VIDEO_ID/stream")
    logger.info(f"Video player: http://localhost:{port}/static/videos.html")
    
    app.run(host='0.0.0.0', port=port, debug=True)
