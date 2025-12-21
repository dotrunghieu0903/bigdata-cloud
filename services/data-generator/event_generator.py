"""
Data Generator Service
Simulates user events and publishes to Kafka for real-time processing
Supports both fake data generation and real dataset loading
"""

import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import logging
from dataset_loader import DatasetManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class VideoEventGenerator:
    def __init__(self, bootstrap_servers, data_mode='fake', data_path='/data'):
        """
        Initialize event generator
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            data_mode: 'fake', 'shortvideo', or 'microlens'
            data_path: Path to dataset files
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Data mode configuration
        self.data_mode = data_mode
        self.dataset_manager = DatasetManager(data_mode, data_path)
        
        # Initialize dataset if using real data
        if data_mode != 'fake':
            logger.info(f"Initializing {data_mode} dataset...")
            if self.dataset_manager.initialize():
                logger.info(f"✅ {data_mode} dataset loaded successfully!")
            else:
                logger.warning(f"⚠️ Failed to load {data_mode} dataset, falling back to fake data")
                self.data_mode = 'fake'
        
        # Predefined data for realistic simulation (fake mode)
        self.categories = [
            'comedy', 'dance', 'music', 'cooking', 'tech', 
            'sports', 'education', 'gaming', 'travel', 'fashion'
        ]
        
        self.event_types = ['view', 'like', 'share', 'comment', 'skip']
        self.event_weights = [0.5, 0.2, 0.1, 0.15, 0.05]  # Probabilities
        
    def generate_user_id(self):
        """Generate a user ID (simulate 10k users)"""
        return f"user_{random.randint(1, 10000)}"
    
    def generate_video_id(self):
        """Generate a video ID (simulate 50k videos)"""
        return f"video_{random.randint(1, 50000)}"
    
    def generate_view_event(self):
        """Generate a video view event"""
        user_id = self.generate_user_id()
        video_id = self.generate_video_id()
        
        watch_time = random.uniform(1, 60)  # seconds
        total_duration = random.uniform(watch_time, 60)
        
        event = {
            'event_type': 'view',
            'user_id': user_id,
            'video_id': video_id,
            'timestamp': datetime.utcnow().isoformat(),
            'watch_time': round(watch_time, 2),
            'total_duration': round(total_duration, 2),
            'completion_rate': round(watch_time / total_duration, 2),
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'session_id': f"session_{random.randint(1, 100000)}"
        }
        return event
    
    def generate_interaction_event(self):
        """Generate a user interaction event (like, share, comment)"""
        user_id = self.generate_us (real or fake)"""
        # Try to get real video data if available
        if self.data_mode != 'fake' and self.dataset_manager.is_available():
            real_video = self.dataset_manager.get_random_video()
            if real_video:
                # Add timestamp if not present
                if 'created_at' not in real_video or not real_video['created_at']:
                    real_video['created_at'] = datetime.utcnow().isoformat()
                logger.debug(f"Using real video: {real_video['video_id']}")
                return real_video
        
        # Fallback to fake data
        video_id = self.generate_video_id()
        
        metadata = {
            'video_id': video_id,
            'title': fake.sentence(nb_words=6),
            'creator_id': f"creator_{random.randint(1, 5000)}",
            'category': random.choice(self.categories),
            'tags': random.sample(
                ['trending', 'viral', 'funny', 'educational', 'tutorial', 
                 'music', 'dance', 'challenge', 'diy', 'review'],
                k=random.randint(2, 5)
            ),
            'duration': round(random.uniform(15, 60), 2),
            'created_at': datetime.utcnow().isoformat(),
            'resolution': random.choice(['720p', '1080p', '4k']),
            'language': random.choice(['en', 'vi', 'es', 'fr', 'ja'])
        }
        return metadata
    
    def generate_event_from_real_interaction(self):
        """Generate event from real dataset interaction"""
        if self.data_mode == 'fake' or not self.dataset_manager.is_available():
            return None
        
        interaction = self.dataset_manager.get_random_interaction()
        if not interaction:
            return None
        
        # Convert to event format
        event = {
            'event_type': interaction.get('interaction_type', 'view'),
            'user_id': interaction['user_id'],
            'video_id': interaction['video_id'],
            'timestamp': interaction.get('timestamp', datetime.utcnow().isoformat()),
            'session_id': f"session_{random.randint(1, 100000)}"
        }
        
        # Add watch time if available
        if 'watch_time' in interaction:
            event['watch_time'] = interaction['watch_time']
            event['total_duration'] = interaction.get('duration', 60)
            event['completion_rate'] = min(1.0, event['watch_time'] / event['total_duration'])
        
        logger.info(f"Data mode: {self.data_mode}")
        
        try:
            while True:
                # Try to use real data interactions if available
                if self.data_mode != 'fake' and random.random() < 0.7:  # 70% real data
                    real_event = self.generate_event_from_real_interaction()
                    if real_event:
                        topic = 'user-events' if real_event['event_type'] == 'view' else 'user-interactions'
                        self.send_event(topic, real_event, real_event['user_id'])
                        continue
                metadata(self):
        """Generate video metadata"""
        video_id = self.generate_video_id()
        
        metadata = {
            'video_id': video_id,
            'title': fake.sentence(nb_words=6),
            'creator_id': f"creator_{random.randint(1, 5000)}",
            'category': random.choice(self.categories),
            'tags': random.sample(
                ['trending', 'viral', 'funny', 'educational', 'tutorial', 
                 'music', 'dance', 'challenge', 'diy', 'review'],
                k=random.randint(2, 5)
            ),
            'duration': round(random.uniform(15, 60), 2),
            'created_at': datetime.utcnow().isoformat(),
            'resolution': random.choice(['720p', '1080p', '4k']),
            'language': random.choice(['en', 'vi', 'es', 'fr', 'ja'])
        }
        return metadata
    
    def send_event(self, topic, event, key=None):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent to {record_metadata.topic}:{record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error sending event: {e}")
            return False
    
    def run(self, events_per_second=10):
        """Generate and send events continuously"""
        logger.info(f"Starting event generation at {events_per_second} events/second")
        
        try:
            while True:
                # Generate view events (60% of traffic)
                for _ in range(int(events_per_second * 0.6)):
                    event = self.generate_view_event()
                    self.send_event('user-events', event, event['user_id'])
                
                # Generate interaction events (40% of traffic)
                for _ in range(int(events_per_second * 0.4)):
                    event = self.generate_interaction_event()
                    self.send_event('user-interactions', event, event['user_id'])
                
                # Occasionally send video metadata
                if random.random() < 0.05:
                    metadata = self.generate_video_metadata()
                    self.send_event('video-metadata', metadata, metadata['video_id'])
                
                time.sleep(1)
                
    # Data mode: 'fake', 'shortvideo', or 'microlens'
    data_mode = os.getenv('DATA_MODE', 'fake')
    data_path = os.getenv('DATA_PATH', '/data')
    
    logger.info(f"Initializing generator with mode: {data_mode}")
    generator = VideoEventGenerator(kafka_servers, data_mode=data_mode, data_path=data_path
            logger.info("Stopping event generation...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    events_per_second = int(os.getenv('EVENTS_PER_SECOND', '10'))
    
    generator = VideoEventGenerator(kafka_servers)
    generator.run(events_per_second)
