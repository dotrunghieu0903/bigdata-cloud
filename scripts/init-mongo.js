// Initialize MongoDB with collections and indexes

db = db.getSiblingDB('video_recommendation');

// Create collections
db.createCollection('users');
db.createCollection('videos');
db.createCollection('interactions');
db.createCollection('user_profiles');
db.createCollection('video_metadata');

// Create indexes for users collection
db.users.createIndex({ "user_id": 1 }, { unique: true });
db.users.createIndex({ "created_at": 1 });

// Create indexes for videos collection
db.videos.createIndex({ "video_id": 1 }, { unique: true });
db.videos.createIndex({ "category": 1 });
db.videos.createIndex({ "tags": 1 });
db.videos.createIndex({ "created_at": 1 });
db.videos.createIndex({ "view_count": -1 });

// Create indexes for interactions collection
db.interactions.createIndex({ "user_id": 1, "video_id": 1, "timestamp": -1 });
db.interactions.createIndex({ "user_id": 1, "timestamp": -1 });
db.interactions.createIndex({ "video_id": 1, "timestamp": -1 });
db.interactions.createIndex({ "event_type": 1 });

// Create indexes for user_profiles collection
db.user_profiles.createIndex({ "user_id": 1 }, { unique: true });
db.user_profiles.createIndex({ "last_updated": -1 });

// Create indexes for video_metadata collection
db.video_metadata.createIndex({ "video_id": 1 }, { unique: true });
db.video_metadata.createIndex({ "engagement_score": -1 });

print('MongoDB initialization completed successfully!');
