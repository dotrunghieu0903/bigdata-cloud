"""
Load MicroLens-100k Real Dataset into MongoDB
Loads actual video metadata and user interactions from MicroLens dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("LoadMicroLensDataset") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.mongodb.write.connection.uri",
                "mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin") \
        .getOrCreate()

def load_video_metadata(spark, data_path="/opt/data/MicroLens-100k"):
    """
    Load video metadata from MicroLens-100k dataset
    Combines titles, likes/views, and tags
    """
    
    print("\n" + "="*80)
    print("LOADING VIDEO METADATA")
    print("="*80)
    
    # 1. Load titles
    print("\n1. Loading video titles...")
    titles_df = spark.read.csv(
        f"{data_path}/MicroLens-100k_title_en.csv",
        header=False,
        inferSchema=True
    ).select(
        col("_c0").cast("string").alias("video_id"),
        col("_c1").alias("title")
    )
    
    print(f"   Loaded {titles_df.count()} video titles")
    titles_df.show(5, truncate=False)
    
    # 2. Load likes and views
    print("\n2. Loading likes and views...")
    likes_views_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("likes", IntegerType(), True),
        StructField("views", IntegerType(), True)
    ])
    
    likes_views_df = spark.read.csv(
        f"{data_path}/MicroLens-100k_likes_and_views.txt",
        sep="\t",
        schema=likes_views_schema
    )
    
    print(f"   Loaded {likes_views_df.count()} video stats")
    likes_views_df.show(5)
    
    # 3. Load tags
    print("\n3. Loading video tags...")
    tags_df = spark.read.csv(
        f"{data_path}/tags_to_summary.csv",
        header=False,
        inferSchema=True
    )
    
    # Rename columns to match
    tags_df = tags_df.select(
        col("_c0").cast("string").alias("video_id"),
        col("_c1").alias("tags")
    )
    
    print(f"   Loaded {tags_df.count()} video tags")
    tags_df.show(5, truncate=False)
    
    # 4. Combine all metadata
    print("\n4. Combining metadata...")
    video_metadata = titles_df \
        .join(likes_views_df, "video_id", "left") \
        .join(tags_df, "video_id", "left")
    
    # Add calculated fields
    video_metadata = video_metadata \
        .withColumn("engagement_score", 
                   (col("likes") / (col("views") + 1)) * 100) \
        .withColumn("created_at", lit(datetime.utcnow().isoformat())) \
        .withColumn("video_url", concat(lit("/api/v1/videos/"), col("video_id"), lit("/stream"))) \
        .withColumn("cover_url", concat(lit("/api/v1/videos/"), col("video_id"), lit("/cover")))
    
    print(f"\n✓ Combined metadata for {video_metadata.count()} videos")
    
    # Show sample
    print("\nSample video metadata:")
    video_metadata.select("video_id", "title", "likes", "views", "engagement_score").show(10, truncate=False)
    
    return video_metadata

def load_user_interactions(spark, data_path="/opt/data/MicroLens-100k"):
    """
    Load user-video interactions from MicroLens-100k_pairs.csv
    Converts to interaction events (views)
    """
    
    print("\n" + "="*80)
    print("LOADING USER INTERACTIONS")
    print("="*80)
    
    # Load interaction pairs
    print("\n1. Loading interaction data...")
    pairs_df = spark.read.csv(
        f"{data_path}/MicroLens-100k_pairs.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"   Loaded {pairs_df.count()} raw interactions")
    pairs_df.show(5)
    
    # Transform to interaction events
    interactions_df = pairs_df.select(
        col("user").cast("string").alias("user_id"),
        col("item").cast("string").alias("video_id"),
        col("timestamp").cast("long").alias("timestamp")
    )
    
    # Add event details
    interactions_df = interactions_df \
        .withColumn("event_type", lit("view")) \
        .withColumn("watch_time", (rand() * 60 + 10)) \
        .withColumn("total_duration", lit(60.0)) \
        .withColumn("completion_rate", col("watch_time") / col("total_duration")) \
        .withColumn("weight", lit(1.0)) \
        .withColumn("created_at", from_unixtime(col("timestamp") / 1000)) \
        .select("user_id", "video_id", "event_type", "timestamp", "weight", 
                "watch_time", "total_duration", "completion_rate", "created_at")
    
    print(f"\n✓ Processed {interactions_df.count()} view interactions")
    
    # Show sample
    print("\nSample interactions:")
    interactions_df.select("user_id", "video_id", "event_type", "completion_rate").show(10)
    
    return interactions_df

def load_comments(spark, data_path="/opt/data/MicroLens-100k"):
    """
    Load comments from MicroLens-100k_comment_en.txt
    Creates comment interaction events
    """
    
    print("\n" + "="*80)
    print("LOADING COMMENTS")
    print("="*80)
    
    # Schema: userID, videoID, commentContent
    comment_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("video_id", StringType(), True),
        StructField("comment_content", StringType(), True)
    ])
    
    comments_df = spark.read.csv(
        f"{data_path}/MicroLens-100k_comment_en.txt",
        sep="\t",
        schema=comment_schema
    )
    
    print(f"   Loaded {comments_df.count()} comments")
    
    # Convert to interaction events
    comment_interactions = comments_df \
        .withColumn("event_type", lit("comment")) \
        .withColumn("timestamp", (unix_timestamp() * 1000).cast("long")) \
        .withColumn("comment_length", length(col("comment_content"))) \
        .withColumn("weight", lit(3.0)) \
        .withColumn("created_at", from_unixtime(unix_timestamp())) \
        .withColumn("watch_time", lit(None).cast("double")) \
        .withColumn("total_duration", lit(None).cast("double")) \
        .withColumn("completion_rate", lit(None).cast("double")) \
        .select("user_id", "video_id", "event_type", "timestamp", "weight", 
                "watch_time", "total_duration", "completion_rate", "created_at")
    
    print(f"\n✓ Processed {comment_interactions.count()} comment interactions")
    
    # Show sample
    print("\nSample comments:")
    comment_interactions.show(5)
    
    return comment_interactions

def generate_likes_and_shares(spark, interactions_df):
    """
    Generate like and share events based on view interactions
    Higher completion rate → higher chance of like/share
    """
    
    print("\n" + "="*80)
    print("GENERATING ENGAGEMENT EVENTS (LIKES & SHARES)")
    print("="*80)
    
    # Sample 30% of high-completion views as likes
    likes_df = interactions_df \
        .filter((col("completion_rate") >= 0.6) & (rand() < 0.3)) \
        .select("user_id", "video_id", "timestamp") \
        .withColumn("event_type", lit("like")) \
        .withColumn("weight", lit(2.0)) \
        .withColumn("watch_time", lit(None).cast("double")) \
        .withColumn("total_duration", lit(None).cast("double")) \
        .withColumn("completion_rate", lit(None).cast("double")) \
        .withColumn("created_at", from_unixtime(col("timestamp") / 1000)) \
        .select("user_id", "video_id", "event_type", "timestamp", "weight", 
                "watch_time", "total_duration", "completion_rate", "created_at")
    
    print(f"✓ Generated {likes_df.count()} like interactions")
    
    # Sample 10% of high-completion views as shares
    shares_df = interactions_df \
        .filter((col("completion_rate") >= 0.8) & (rand() < 0.1)) \
        .select("user_id", "video_id", "timestamp") \
        .withColumn("event_type", lit("share")) \
        .withColumn("weight", lit(4.0)) \
        .withColumn("watch_time", lit(None).cast("double")) \
        .withColumn("total_duration", lit(None).cast("double")) \
        .withColumn("completion_rate", lit(None).cast("double")) \
        .withColumn("created_at", from_unixtime(col("timestamp") / 1000)) \
        .select("user_id", "video_id", "event_type", "timestamp", "weight", 
                "watch_time", "total_duration", "completion_rate", "created_at")
    
    print(f"✓ Generated {shares_df.count()} share interactions")
    
    return likes_df, shares_df

def save_to_mongodb(spark, df, collection_name, mode="overwrite"):
    """Save DataFrame to MongoDB using PyMongo (avoids Spark connector version issues)"""
    
    print(f"\nSaving to MongoDB collection: {collection_name}")
    
    # Convert to Pandas and save using PyMongo
    from pymongo import MongoClient, ASCENDING
    import builtins
    
    # Collect data to driver (for large datasets, consider partitioned writes)
    pandas_df = df.toPandas()
    
    # Convert to dict records
    records = pandas_df.to_dict('records')
    
    print(f"   Converted {len(records)} records to dict format")
    
    # Connect to MongoDB
    mongo_uri = "mongodb://admin:admin123@mongodb:27017/"
    client = MongoClient(mongo_uri)
    db = client["video_recommendation"]
    collection = db[collection_name]
    
    # Clear collection if mode is overwrite
    if mode == "overwrite":
        collection.delete_many({})
        print(f"   Cleared existing data from {collection_name}")
    
    # Insert in batches
    batch_size = 1000
    total = len(records)
    
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        collection.insert_many(batch)
        inserted_count = builtins.min(i+batch_size, total)
        print(f"   Inserted {inserted_count}/{total} documents")
    
    client.close()
    
    print(f"✓ Saved {total} documents to {collection_name}")

def create_indexes(spark):
    """Create indexes for better query performance"""
    
    print("\n" + "="*80)
    print("CREATING INDEXES")
    print("="*80)
    
    from pymongo import MongoClient, ASCENDING, DESCENDING
    
    client = MongoClient("mongodb://admin:admin123@mongodb:27017/?authSource=admin")
    db = client.video_recommendation
    
    # Videos collection indexes
    db.videos.create_index([("video_id", ASCENDING)], unique=True)
    db.videos.create_index([("engagement_score", DESCENDING)])
    db.videos.create_index([("views", DESCENDING)])
    
    # Interactions collection indexes
    db.interactions.create_index([("user_id", ASCENDING)])
    db.interactions.create_index([("video_id", ASCENDING)])
    db.interactions.create_index([("event_type", ASCENDING)])
    db.interactions.create_index([("user_id", ASCENDING), ("video_id", ASCENDING)])
    
    print("✓ Created indexes for videos and interactions collections")
    
    client.close()

def main():
    """Main execution"""
    
    print("\n" + "="*80)
    print("LOAD MICROLENS-100K DATASET TO MONGODB")
    print("="*80)
    print(f"Start Time: {datetime.utcnow().isoformat()}\n")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    data_path = "/opt/data/MicroLens-100k"
    
    try:
        # 1. Load video metadata
        video_metadata = load_video_metadata(spark, data_path)
        save_to_mongodb(spark, video_metadata, "videos", mode="overwrite")
        
        # 2. Load user interactions (views)
        view_interactions = load_user_interactions(spark, data_path)
        
        # 3. Load comments
        comment_interactions = load_comments(spark, data_path)
        
        # 4. Generate likes and shares
        likes_df, shares_df = generate_likes_and_shares(spark, view_interactions)
        
        # 5. Combine all interactions
        print("\n" + "="*80)
        print("COMBINING ALL INTERACTIONS")
        print("="*80)
        
        # Prepare all interaction types with consistent schema
        view_final = view_interactions.select(
            "user_id", "video_id", "event_type", "timestamp", "weight", 
            "watch_time", "total_duration", "completion_rate", "created_at"
        )
        
        comment_final = comment_interactions.select(
            "user_id", "video_id", "event_type", "timestamp", "weight", "created_at"
        ).withColumn("watch_time", lit(None).cast("double")) \
         .withColumn("total_duration", lit(None).cast("double")) \
         .withColumn("completion_rate", lit(None).cast("double"))
        
        like_final = likes_df.select(
            "user_id", "video_id", "event_type", "timestamp", "weight", "created_at"
        ).withColumn("watch_time", lit(None).cast("double")) \
         .withColumn("total_duration", lit(None).cast("double")) \
         .withColumn("completion_rate", lit(None).cast("double"))
        
        share_final = shares_df.select(
            "user_id", "video_id", "event_type", "timestamp", "weight", "created_at"
        ).withColumn("watch_time", lit(None).cast("double")) \
         .withColumn("total_duration", lit(None).cast("double")) \
         .withColumn("completion_rate", lit(None).cast("double"))
        
        # Union all
        all_interactions = view_final \
            .union(comment_final) \
            .union(like_final) \
            .union(share_final)
        
        print(f"\nTotal interactions: {all_interactions.count()}")
        
        # Show breakdown by type
        print("\nInteraction breakdown:")
        all_interactions.groupBy("event_type").count().show()
        
        # Save to MongoDB
        save_to_mongodb(spark, all_interactions, "interactions", mode="overwrite")
        
        # 6. Create indexes
        create_indexes(spark)
        
        # 7. Summary statistics
        print("\n" + "="*80)
        print("DATASET SUMMARY")
        print("="*80)
        
        stats = {
            "total_videos": video_metadata.count(),
            "total_interactions": all_interactions.count(),
            "unique_users": all_interactions.select("user_id").distinct().count(),
            "unique_videos_with_interactions": all_interactions.select("video_id").distinct().count(),
            "avg_views_per_video": all_interactions.filter(col("event_type") == "view").count() / video_metadata.count(),
            "avg_likes_per_video": all_interactions.filter(col("event_type") == "like").count() / video_metadata.count(),
        }
        
        print(f"\nTotal Videos: {stats['total_videos']}")
        print(f"Total Interactions: {stats['total_interactions']}")
        print(f"Unique Users: {stats['unique_users']}")
        print(f"Unique Videos with Interactions: {stats['unique_videos_with_interactions']}")
        print(f"Avg Views per Video: {stats['avg_views_per_video']:.2f}")
        print(f"Avg Likes per Video: {stats['avg_likes_per_video']:.2f}")
        
        print("\n" + "="*80)
        print("✓ DATASET LOADED SUCCESSFULLY")
        print("="*80)
        print("\nNext steps:")
        print("1. Run user profiling: /opt/spark-jobs/user_profiling.py")
        print("2. Run recommendation pipeline: /opt/spark-jobs/complete_recommendation_pipeline.py")
        print(f"\nEnd Time: {datetime.utcnow().isoformat()}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
