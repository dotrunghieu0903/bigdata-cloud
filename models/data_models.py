"""
Data Models for Video Recommendation System
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum

class EventType(Enum):
    """User event types"""
    VIEW = "view"
    LIKE = "like"
    SHARE = "share"
    COMMENT = "comment"
    SKIP = "skip"

class DeviceType(Enum):
    """Device types"""
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"

@dataclass
class UserEvent:
    """User event data model"""
    event_type: str
    user_id: str
    video_id: str
    timestamp: str
    watch_time: Optional[float] = None
    total_duration: Optional[float] = None
    completion_rate: Optional[float] = None
    device_type: Optional[str] = None
    session_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'event_type': self.event_type,
            'user_id': self.user_id,
            'video_id': self.video_id,
            'timestamp': self.timestamp,
            'watch_time': self.watch_time,
            'total_duration': self.total_duration,
            'completion_rate': self.completion_rate,
            'device_type': self.device_type,
            'session_id': self.session_id
        }

@dataclass
class UserProfile:
    """User profile data model"""
    user_id: str
    total_interactions: int = 0
    favorite_categories: List[str] = field(default_factory=list)
    recent_videos: List[Dict] = field(default_factory=list)
    last_activity: Optional[str] = None
    preferences: Dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'total_interactions': self.total_interactions,
            'favorite_categories': self.favorite_categories,
            'recent_videos': self.recent_videos,
            'last_activity': self.last_activity,
            'preferences': self.preferences,
            'created_at': self.created_at
        }

@dataclass
class VideoMetadata:
    """Video metadata model"""
    video_id: str
    title: str
    creator_id: str
    category: str
    tags: List[str]
    duration: float
    created_at: str
    view_count: int = 0
    like_count: int = 0
    share_count: int = 0
    comment_count: int = 0
    engagement_score: float = 0.0
    resolution: str = "1080p"
    language: str = "en"
    
    def calculate_engagement_score(self) -> float:
        """Calculate engagement score based on interactions"""
        # Weighted formula for engagement
        score = (
            self.view_count * 1 +
            self.like_count * 5 +
            self.share_count * 10 +
            self.comment_count * 8
        ) / max(1, (datetime.utcnow() - datetime.fromisoformat(self.created_at)).days + 1)
        
        self.engagement_score = score
        return score
    
    def to_dict(self) -> Dict:
        return {
            'video_id': self.video_id,
            'title': self.title,
            'creator_id': self.creator_id,
            'category': self.category,
            'tags': self.tags,
            'duration': self.duration,
            'created_at': self.created_at,
            'view_count': self.view_count,
            'like_count': self.like_count,
            'share_count': self.share_count,
            'comment_count': self.comment_count,
            'engagement_score': self.engagement_score,
            'resolution': self.resolution,
            'language': self.language
        }

@dataclass
class Recommendation:
    """Recommendation result model"""
    user_id: str
    video_id: str
    score: float
    method: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'video_id': self.video_id,
            'score': self.score,
            'method': self.method,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }
