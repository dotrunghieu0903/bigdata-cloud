"""
User Profiling and Behavioral Analysis System
Distributed processing with Spark for user profile scoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import json
from datetime import datetime, timedelta

class DistributedUserProfiler:
    """
    Distributed User Profiling System
    Analyzes user behavior (likes, comments, views) to create user profiles
    Uses Spark for distributed processing across cluster
    """
    
    def __init__(self, spark, mongodb_uri, redis_client=None):
        self.spark = spark
        self.mongodb_uri = mongodb_uri
        self.redis_client = redis_client
    
    def _save_to_mongodb(self, df, collection_name, mode="overwrite"):
        """Helper to save DataFrame to MongoDB using PyMongo"""
        from pymongo import MongoClient
        import builtins
        
        print(f"Saving to MongoDB collection: {collection_name}")
        
        # Convert to Pandas
        pandas_df = df.toPandas()
        records = pandas_df.to_dict('records')
        
        print(f"   Converted {len(records)} records")
        
        # Connect to MongoDB
        client = MongoClient(self.mongodb_uri)
        db = client["video_recommendation"]
        collection = db[collection_name]
        
        # Clear collection if mode is overwrite
        if mode == "overwrite":
            collection.delete_many({})
            print(f"   Cleared existing data")
        
        # Insert in batches
        batch_size = 1000
        total = len(records)
        
        for i in range(0, total, batch_size):
            batch = records[i:i+batch_size]
            collection.insert_many(batch)
            if (i + batch_size) % 10000 == 0:  # Print every 10k
                print(f"   Inserted {builtins.min(i+batch_size, total)}/{total}")
        
        client.close()
        print(f"✓ Saved {total} documents to {collection_name}")
    
    def load_user_interactions(self):
        """Load all user interactions from MongoDB using PyMongo (avoids connector issues)"""
        from pymongo import MongoClient
        import pandas as pd
        
        print("Loading interactions from MongoDB...")
        
        # Connect to MongoDB
        client = MongoClient(self.mongodb_uri)
        db = client["video_recommendation"]
        collection = db["interactions"]
        
        # Get all interactions
        interactions = list(collection.find({}, {"_id": 0}))
        client.close()
        
        print(f"   Loaded {len(interactions)} interactions from MongoDB")
        
        # Convert to Pandas then Spark DataFrame
        pandas_df = pd.DataFrame(interactions)
        interactions_df = self.spark.createDataFrame(pandas_df)
        
        print(f"✓ Created Spark DataFrame with {interactions_df.count()} interactions")
        return interactions_df
    
    def load_video_metadata(self, data_path="/opt/data/MicroLens-100k"):
        """Load video metadata from MicroLens dataset"""
        
        # Load titles (no header, format: video_id, title)
        titles_df = self.spark.read.csv(
            f"{data_path}/MicroLens-100k_title_en.csv",
            header=False,
            inferSchema=True
        ).select(
            col("_c0").cast("string").alias("video_id"),
            col("_c1").alias("title")
        )
        
        # Load likes and views
        likes_views_schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("likes", IntegerType(), True),
            StructField("views", IntegerType(), True)
        ])
        
        likes_views_df = self.spark.read.csv(
            f"{data_path}/MicroLens-100k_likes_and_views.txt",
            sep="\t",
            schema=likes_views_schema
        )
        
        # Load tags (no header, format: video_id, tags)
        tags_df = self.spark.read.csv(
            f"{data_path}/tags_to_summary.csv",
            header=False,
            inferSchema=True
        ).select(
            col("_c0").cast("string").alias("video_id"),
            col("_c1").alias("tags")
        )
        
        # Join all metadata
        video_metadata = titles_df \
            .join(likes_views_df, "video_id", "left") \
            .join(tags_df, "video_id", "left")
        
        print(f"Loaded metadata for {video_metadata.count()} videos")
        return video_metadata
    
    def calculate_user_profile_scores(self, interactions_df, video_metadata):
        """
        Calculate comprehensive user profile scores
        Distributed across Spark cluster
        """
        
        # Join interactions with video metadata and drop duplicate video_id column
        enriched_interactions = interactions_df.join(
            video_metadata,
            "video_id",
            "left"
        )
        
        # Calculate user-level metrics
        user_profiles = enriched_interactions.groupBy("user_id").agg(
            # View metrics
            count(when(col("event_type") == "view", 1)).alias("total_views"),
            avg(when(col("event_type") == "view", col("watch_time"))).alias("avg_watch_time"),
            avg(when(col("event_type") == "view", col("completion_rate"))).alias("avg_completion_rate"),
            
            # Engagement metrics
            count(when(col("event_type") == "like", 1)).alias("total_likes"),
            count(when(col("event_type") == "comment", 1)).alias("total_comments"),
            count(when(col("event_type") == "share", 1)).alias("total_shares"),
            count(when(col("event_type") == "collect", 1)).alias("total_collects"),
            
            # Diversity metrics
            countDistinct("video_id").alias("unique_videos_watched"),
            countDistinct("tags").alias("unique_categories"),
            
            # Temporal metrics
            min("timestamp").alias("first_interaction"),
            max("timestamp").alias("last_interaction"),
            
            # Content preferences
            collect_list("tags").alias("watched_tags"),
            collect_list("tags").alias("watched_categories")
        )
        
        # Calculate derived metrics
        user_profiles = user_profiles.withColumn(
            "engagement_rate",
            (col("total_likes") + col("total_comments") + col("total_shares")) / 
            (col("total_views") + 1)  # Avoid division by zero
        ).withColumn(
            "active_days",
            datediff(
                from_unixtime(col("last_interaction")).cast("date"),
                from_unixtime(col("first_interaction")).cast("date")
            ) + 1
        ).withColumn(
            "activity_frequency",
            col("total_views") / (col("active_days") + 1)
        )
        
        # Calculate user type score (0-100)
        user_profiles = user_profiles.withColumn(
            "user_score",
            least(lit(100), (
                (col("total_views") * 0.3) +
                (col("total_likes") * 2.0) +
                (col("total_comments") * 3.0) +
                (col("total_shares") * 4.0) +
                (col("engagement_rate") * 50.0) +
                (col("avg_completion_rate") * 30.0)
            ))
        )
        
        # User type classification based on behavior
        user_profiles = user_profiles.withColumn(
            "user_type",
            when(col("user_score") >= 80, "power_user")
            .when(col("user_score") >= 50, "active_user")
            .when(col("user_score") >= 20, "casual_user")
            .otherwise("new_user")
        )
        
        return user_profiles
    
    def extract_user_preferences(self, interactions_df, video_metadata):
        """
        Extract user content preferences using distributed processing
        Returns tag and tag preferences with weights
        """
        
        # Calculate tag preferences
        category_prefs = interactions_df \
            .join(video_metadata, "video_id") \
            .groupBy("user_id", "tags") \
            .agg(
                count("*").alias("view_count"),
                avg("watch_time").alias("avg_watch_time"),
                avg("completion_rate").alias("avg_completion_rate")
            ) \
            .withColumn(
                "tags_score",
                col("view_count") * col("avg_completion_rate") * 100
            )
        
        # Rank tags per user
        from pyspark.sql.window import Window
        user_window = Window.partitionBy("user_id").orderBy(col("tags_score").desc())
        
        top_categories = category_prefs \
            .withColumn("rank", row_number().over(user_window)) \
            .filter(col("rank") <= 5) \
            .groupBy("user_id") \
            .agg(
                collect_list(
                    struct("tags", "tags_score", "rank")
                ).alias("top_categories")
            )
        
        # Calculate tag preferences (from tags_to_summary.csv)
        tag_prefs = interactions_df \
            .join(video_metadata, "video_id") \
            .filter(col("tags").isNotNull()) \
            .select("user_id", explode(split(col("tags"), ",")).alias("tag")) \
            .groupBy("user_id", "tag") \
            .agg(count("*").alias("tag_count")) \
            .withColumn("rank", row_number().over(
                Window.partitionBy("user_id").orderBy(col("tag_count").desc())
            )) \
            .filter(col("rank") <= 10) \
            .groupBy("user_id") \
            .agg(
                collect_list(
                    struct("tag", "tag_count")
                ).alias("preferred_tags")
            )
        
        return top_categories, tag_prefs
    
    def cluster_users(self, user_profiles):
        """
        Cluster users into segments using K-Means (distributed)
        Helps identify similar user groups
        """
        
        # Select features for clustering
        feature_cols = [
            "total_views", "total_likes", "total_comments", 
            "engagement_rate", "avg_completion_rate", "activity_frequency"
        ]
        
        # Fill null values
        user_profiles_clean = user_profiles.na.fill(0, feature_cols)
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        features_df = assembler.transform(user_profiles_clean)
        
        # Standardize features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(features_df)
        scaled_df = scaler_model.transform(features_df)
        
        # K-Means clustering
        kmeans = KMeans(
            featuresCol="scaled_features",
            k=5,  # 5 user segments
            seed=42
        )
        
        model = kmeans.fit(scaled_df)
        clustered_users = model.transform(scaled_df)
        
        # Add cluster labels
        clustered_users = clustered_users.withColumn(
            "user_segment",
            when(col("prediction") == 0, "casual_browsers")
            .when(col("prediction") == 1, "engaged_viewers")
            .when(col("prediction") == 2, "power_users")
            .when(col("prediction") == 3, "new_users")
            .otherwise("churned_users")
        )
        
        return clustered_users
    
    def save_user_profiles(self, user_profiles, preferences):
        """Save user profiles to MongoDB and Redis for fast lookup"""
        
        # Combine profiles with preferences
        category_prefs, tag_prefs = preferences
        
        full_profiles = user_profiles \
            .join(category_prefs, "user_id", "left") \
            .join(tag_prefs, "user_id", "left")
        
        # Drop vector columns (DenseVector can't be saved to MongoDB)
        columns_to_drop = ["features", "scaled_features"]
        for col_name in columns_to_drop:
            if col_name in full_profiles.columns:
                full_profiles = full_profiles.drop(col_name)
        
        # Save to MongoDB using PyMongo
        self._save_to_mongodb(full_profiles, "user_profiles", mode="overwrite")
        
        # Cache hot profiles in Redis (top users)
        if self.redis_client:
            top_users = full_profiles \
                .orderBy(col("user_score").desc()) \
                .limit(1000) \
                .collect()
            
            for row in top_users:
                profile_key = f"profile:{row['user_id']}"
                profile_data = {
                    'user_score': float(row['user_score']),
                    'user_type': row['user_type'],
                    'engagement_rate': float(row['engagement_rate']),
                    'total_views': int(row['total_views']),
                    'total_likes': int(row['total_likes'])
                }
                
                self.redis_client.setex(
                    profile_key,
                    3600,  # 1 hour cache
                    json.dumps(profile_data)
                )
            
            print(f"Cached {len(top_users)} top user profiles in Redis")
    
    def generate_behavioral_insights(self, user_profiles):
        """Generate insights about user behavior patterns"""
        
        # Overall statistics
        stats = user_profiles.agg(
            avg("user_score").alias("avg_user_score"),
            avg("engagement_rate").alias("avg_engagement_rate"),
            avg("total_views").alias("avg_views_per_user"),
            count("user_id").alias("total_users")
        ).collect()[0]
        
        # User type distribution
        user_type_dist = user_profiles.groupBy("user_type") \
            .agg(count("*").alias("count")) \
            .collect()
        
        insights = {
            "generated_at": datetime.utcnow().isoformat(),
            "total_users": stats["total_users"],
            "avg_user_score": float(stats["avg_user_score"]),
            "avg_engagement_rate": float(stats["avg_engagement_rate"]),
            "avg_views_per_user": float(stats["avg_views_per_user"]),
            "user_type_distribution": {
                row["user_type"]: row["count"] for row in user_type_dist
            }
        }
        
        return insights

def run_user_profiling(spark, mongodb_uri, redis_client=None):
    """Main function to run user profiling pipeline"""
    
    print("\n" + "="*80)
    print("DISTRIBUTED USER PROFILING & BEHAVIORAL ANALYSIS")
    print("="*80)
    
    profiler = DistributedUserProfiler(spark, mongodb_uri, redis_client)
    
    # Load data
    interactions = profiler.load_user_interactions()
    video_metadata = profiler.load_video_metadata()
    
    # Calculate profiles
    user_profiles = profiler.calculate_user_profile_scores(interactions, video_metadata)
    
    # Extract preferences
    preferences = profiler.extract_user_preferences(interactions, video_metadata)
    
    # Cluster users
    clustered_profiles = profiler.cluster_users(user_profiles)
    
    # Save results
    profiler.save_user_profiles(clustered_profiles, preferences)
    
    # Generate insights
    insights = profiler.generate_behavioral_insights(clustered_profiles)
    
    print("\n" + "="*80)
    print("USER PROFILING INSIGHTS")
    print("="*80)
    print(json.dumps(insights, indent=2))
    
    return clustered_profiles, insights
