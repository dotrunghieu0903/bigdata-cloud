"""
Spark Streaming Job for Real-time Video Recommendation
Processes user events from Kafka and updates user profiles in real-time
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_EVENTS = 'user-events'
KAFKA_TOPIC_INTERACTIONS = 'user-interactions'

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin')

def create_spark_session():
    """Create Spark session with Kafka integration"""
    spark = SparkSession.builder \
        .appName("VideoRecommendationStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_user_events(spark):
    """Process user view events stream"""
    
    # Define schema for user events
    event_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("watch_time", DoubleType(), True),
        StructField("total_duration", DoubleType(), True),
        StructField("completion_rate", DoubleType(), True),
        StructField("device_type", StringType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # Read from Kafka
    events_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_EVENTS) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON and extract fields
    parsed_df = events_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")
    
    # Add processing timestamp
    enriched_df = parsed_df.withColumn("processing_time", current_timestamp())
    
    return enriched_df

def process_user_interactions(spark):
    """Process user interaction events (like, share, comment)"""
    
    interaction_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("comment_length", IntegerType(), True)
    ])
    
    interactions_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_INTERACTIONS) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = interactions_df.select(
        from_json(col("value").cast("string"), interaction_schema).alias("data")
    ).select("data.*")
    
    return parsed_df

def calculate_user_preferences(events_df, interactions_df):
    """Calculate user preferences in real-time"""
    
    # Combine view and interaction events
    # Weight different event types
    weighted_events = events_df.withColumn("weight", 
        when(col("completion_rate") > 0.8, 5.0)
        .when(col("completion_rate") > 0.5, 3.0)
        .when(col("completion_rate") > 0.3, 1.0)
        .otherwise(0.5)
    )
    
    weighted_interactions = interactions_df.withColumn("weight",
        when(col("event_type") == "like", 10.0)
        .when(col("event_type") == "share", 15.0)
        .when(col("event_type") == "comment", 8.0)
        .when(col("event_type") == "skip", -2.0)
        .otherwise(1.0)
    )
    
    return weighted_events, weighted_interactions

def update_user_profile_batch(batch_df, batch_id):
    """Update user profiles in Redis and MongoDB for each micro-batch"""
    try:
        import redis
        from pymongo import MongoClient
        
        # Connect to Redis
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        
        # Connect to MongoDB
        mongo_client = MongoClient(MONGODB_URI)
        db = mongo_client.video_recommendation
        
        # Process each row in the batch
        for row in batch_df.collect():
            user_id = row['user_id']
            video_id = row['video_id']
            weight = row['weight']
            timestamp = row['timestamp']
            
            # Update Redis (fast cache)
            # Store recent interactions
            redis_key = f"user:{user_id}:recent_videos"
            redis_client.zadd(redis_key, {video_id: weight})
            redis_client.expire(redis_key, 86400)  # 24 hours
            
            # Update interaction count
            redis_client.hincrby(f"user:{user_id}:stats", "total_interactions", 1)
            
            # Update MongoDB (persistent storage)
            db.interactions.insert_one({
                'user_id': user_id,
                'video_id': video_id,
                'weight': weight,
                'timestamp': timestamp,
                'processed_at': datetime.utcnow()
            })
            
            # Update user profile
            db.user_profiles.update_one(
                {'user_id': user_id},
                {
                    '$inc': {'total_interactions': 1},
                    '$set': {'last_activity': timestamp},
                    '$push': {
                        'recent_videos': {
                            '$each': [{'video_id': video_id, 'weight': weight}],
                            '$slice': -100  # Keep only last 100
                        }
                    }
                },
                upsert=True
            )
        
        print(f"Batch {batch_id}: Processed {batch_df.count()} events")
        
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")

def calculate_video_trending_scores(spark):
    """Calculate trending scores for videos in real-time"""
    
    # This would be a separate streaming query that aggregates
    # video interactions over time windows
    pass

def main():
    """Main execution function"""
    print("Starting Spark Streaming job for Video Recommendation...")
    
    spark = create_spark_session()
    
    # Process different event streams
    events_df = process_user_events(spark)
    interactions_df = process_user_interactions(spark)
    
    # Calculate preferences
    weighted_events, weighted_interactions = calculate_user_preferences(
        events_df, interactions_df
    )
    
    # Write events stream to MongoDB and Redis
    events_query = weighted_events \
        .writeStream \
        .foreachBatch(update_user_profile_batch) \
        .outputMode("append") \
        .start()
    
    # Write interactions stream
    interactions_query = weighted_interactions \
        .writeStream \
        .foreachBatch(update_user_profile_batch) \
        .outputMode("append") \
        .start()
    
    # Console output for debugging
    console_query = events_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()
    
    print("Streaming queries started. Waiting for events...")
    
    # Wait for all queries to finish
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
