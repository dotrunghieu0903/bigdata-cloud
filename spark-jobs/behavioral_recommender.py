"""
Behavioral-based Recommendation System
Uses user behavior (likes, comments, views) to generate personalized recommendations
Distributed processing with Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

class BehavioralRecommender:
    """
    Behavioral-based Recommendation Engine
    Recommends videos based on:
    - Similar user behaviors (collaborative)
    - User's past interactions (personalized)
    - Content metadata (content-based)
    """
    
    def __init__(self, spark, mongodb_uri, redis_client=None):
        self.spark = spark
        self.mongodb_uri = mongodb_uri
        self.redis_client = redis_client
    
    def _load_from_mongodb(self, collection_name):
        """Helper to load data from MongoDB using PyMongo"""
        from pymongo import MongoClient
        import pandas as pd
        
        print(f"Loading {collection_name} from MongoDB...")
        
        client = MongoClient(self.mongodb_uri)
        db = client["video_recommendation"]
        collection = db[collection_name]
        
        # Get all documents
        documents = list(collection.find({}, {"_id": 0}))
        client.close()
        
        print(f"   Loaded {len(documents)} documents")
        
        # Convert to Spark DataFrame
        if documents:
            pandas_df = pd.DataFrame(documents)
            spark_df = self.spark.createDataFrame(pandas_df)
        else:
            # Empty DataFrame
            spark_df = self.spark.createDataFrame([], StructType([]))
        
        return spark_df
    
    def _save_to_mongodb(self, df, collection_name, mode="overwrite"):
        """Helper to save DataFrame to MongoDB using PyMongo"""
        from pymongo import MongoClient
        import builtins
        
        print(f"Saving to MongoDB collection: {collection_name}")
        
        pandas_df = df.toPandas()
        records = pandas_df.to_dict('records')
        
        print(f"   Converted {len(records)} records")
        
        client = MongoClient(self.mongodb_uri)
        db = client["video_recommendation"]
        collection = db[collection_name]
        
        if mode == "overwrite":
            collection.delete_many({})
            print(f"   Cleared existing data")
        
        batch_size = 1000
        total = len(records)
        
        for i in range(0, total, batch_size):
            batch = records[i:i+batch_size]
            collection.insert_many(batch)
            if (i + batch_size) % 10000 == 0:
                print(f"   Inserted {builtins.min(i+batch_size, total)}/{total}")
        
        client.close()
        print(f"✓ Saved {total} documents to {collection_name}")
    
    def load_data(self):
        """Load interactions and user profiles"""
        
        # Load using PyMongo helper
        interactions = self._load_from_mongodb("interactions")
        user_profiles = self._load_from_mongodb("user_profiles")
        videos = self._load_from_mongodb("videos")
        
        return interactions, user_profiles, videos
    
    def calculate_video_scores(self, interactions):
        """
        Calculate video quality scores based on engagement
        Videos with high likes/comments/shares get higher scores
        """
        
        video_stats = interactions.groupBy("video_id").agg(
            count(when(col("event_type") == "view", 1)).alias("view_count"),
            count(when(col("event_type") == "like", 1)).alias("like_count"),
            count(when(col("event_type") == "comment", 1)).alias("comment_count"),
            count(when(col("event_type") == "share", 1)).alias("share_count"),
            avg(when(col("event_type") == "view", col("completion_rate"))).alias("avg_completion_rate"),
            countDistinct("user_id").alias("unique_viewers")
        )
        
        # Calculate engagement score
        video_stats = video_stats.withColumn(
            "engagement_score",
            (
                (col("like_count") * 2.0) +
                (col("comment_count") * 3.0) +
                (col("share_count") * 4.0)
            ) / (col("view_count") + 1)
        ).withColumn(
            "quality_score",
            (col("avg_completion_rate") * 50) +
            (col("engagement_score") * 30) +
            (log1p(col("unique_viewers")) * 10)
        )
        
        return video_stats
    
    def recommend_based_on_likes(self, user_id, interactions, video_stats, n=20):
        """
        Recommend videos based on what users with similar likes watched
        Collaborative filtering based on like behavior
        """
        
        # Get videos this user liked
        user_likes = interactions \
            .filter((col("user_id") == user_id) & (col("event_type") == "like")) \
            .select("video_id")
        
        if user_likes.count() == 0:
            return None
        
        # Find other users who liked the same videos
        similar_users = interactions \
            .join(user_likes, "video_id") \
            .filter((col("user_id") != user_id) & (col("event_type") == "like")) \
            .groupBy("user_id") \
            .agg(count("*").alias("common_likes")) \
            .orderBy(col("common_likes").desc()) \
            .limit(50)
        
        # Get videos these similar users liked (but current user hasn't seen)
        user_watched = interactions \
            .filter(col("user_id") == user_id) \
            .select("video_id").distinct()
        
        recommendations = interactions \
            .join(similar_users, "user_id") \
            .filter(col("event_type") == "like") \
            .join(user_watched, "video_id", "left_anti") \
            .groupBy("video_id") \
            .agg(
                count("*").alias("recommendation_count"),
                sum("common_likes").alias("similarity_score")
            ) \
            .join(video_stats, "video_id") \
            .withColumn(
                "final_score",
                (col("recommendation_count") * col("similarity_score") * 0.7) +
                (col("quality_score") * 0.3)
            ) \
            .orderBy(col("final_score").desc()) \
            .limit(n) \
            .select("video_id", "final_score")
        
        return recommendations
    
    def recommend_based_on_comments(self, user_id, interactions, video_stats, n=20):
        """
        Recommend based on comment behavior
        Users who comment are highly engaged
        """
        
        user_comments = interactions \
            .filter((col("user_id") == user_id) & (col("event_type") == "comment")) \
            .select("video_id")
        
        if user_comments.count() == 0:
            return None
        
        # Find users with similar comment patterns
        similar_commenters = interactions \
            .join(user_comments, "video_id") \
            .filter((col("user_id") != user_id) & (col("event_type") == "comment")) \
            .groupBy("user_id") \
            .agg(count("*").alias("common_commented")) \
            .orderBy(col("common_commented").desc()) \
            .limit(30)
        
        # Get their liked/commented videos
        user_watched = interactions \
            .filter(col("user_id") == user_id) \
            .select("video_id").distinct()
        
        recommendations = interactions \
            .join(similar_commenters, "user_id") \
            .filter(col("event_type").isin(["like", "comment"])) \
            .join(user_watched, "video_id", "left_anti") \
            .groupBy("video_id") \
            .agg(
                count("*").alias("engagement_count"),
                max("common_commented").alias("user_similarity")
            ) \
            .join(video_stats, "video_id") \
            .withColumn(
                "final_score",
                (col("engagement_count") * col("user_similarity") * 0.6) +
                (col("quality_score") * 0.4)
            ) \
            .orderBy(col("final_score").desc()) \
            .limit(n) \
            .select("video_id", "final_score")
        
        return recommendations
    
    def recommend_based_on_watch_patterns(self, user_id, interactions, video_stats, n=20):
        """
        Recommend based on watch completion patterns
        Users who fully watch videos show strong interest
        """
        
        # Get videos user watched with high completion rate
        high_completion_videos = interactions \
            .filter(
                (col("user_id") == user_id) & 
                (col("event_type") == "view") &
                (col("completion_rate") >= 0.7)
            ) \
            .select("video_id", "completion_rate")
        
        if high_completion_videos.count() == 0:
            return None
        
        # Find users with similar high-completion patterns
        similar_viewers = interactions \
            .join(high_completion_videos, "video_id") \
            .filter(
                (col("user_id") != user_id) & 
                (col("event_type") == "view") &
                (col("completion_rate") >= 0.7)
            ) \
            .groupBy("user_id") \
            .agg(
                count("*").alias("similar_watches"),
                avg("completion_rate").alias("avg_completion")
            ) \
            .orderBy(col("similar_watches").desc()) \
            .limit(40)
        
        # Get videos they completed
        user_watched = interactions \
            .filter(col("user_id") == user_id) \
            .select("video_id").distinct()
        
        recommendations = interactions \
            .join(similar_viewers, "user_id") \
            .filter((col("event_type") == "view") & (col("completion_rate") >= 0.6)) \
            .join(user_watched, "video_id", "left_anti") \
            .groupBy("video_id") \
            .agg(
                count("*").alias("watch_count"),
                avg("completion_rate").alias("avg_completion"),
                max("similar_watches").alias("similarity")
            ) \
            .join(video_stats, "video_id") \
            .withColumn(
                "final_score",
                (col("watch_count") * col("similarity") * 0.5) +
                (col("avg_completion") * 30) +
                (col("quality_score") * 0.3)
            ) \
            .orderBy(col("final_score").desc()) \
            .limit(n) \
            .select("video_id", "final_score")
        
        return recommendations
    
    def hybrid_recommendations(self, user_id, interactions, user_profiles, video_stats, n=20):
        """
        Combine multiple recommendation strategies
        Hybrid approach for better coverage
        """
        
        # Get recommendations from different methods
        like_recs = self.recommend_based_on_likes(user_id, interactions, video_stats, n)
        comment_recs = self.recommend_based_on_comments(user_id, interactions, video_stats, n)
        watch_recs = self.recommend_based_on_watch_patterns(user_id, interactions, video_stats, n)
        
        # Get user profile to determine weights
        user_profile = user_profiles.filter(col("user_id") == user_id).first()
        
        if user_profile:
            user_type = user_profile["user_type"]
            
            # Adjust weights based on user type
            if user_type == "power_user":
                weights = {"likes": 0.4, "comments": 0.4, "watch": 0.2}
            elif user_type == "active_user":
                weights = {"likes": 0.35, "comments": 0.35, "watch": 0.3}
            else:
                weights = {"likes": 0.3, "comments": 0.2, "watch": 0.5}
        else:
            weights = {"likes": 0.33, "comments": 0.33, "watch": 0.34}
        
        # Combine recommendations
        all_recs = None
        
        if like_recs:
            like_recs = like_recs.withColumn("method", lit("likes")) \
                .withColumn("weighted_score", col("final_score") * weights["likes"])
            all_recs = like_recs
        
        if comment_recs:
            comment_recs = comment_recs.withColumn("method", lit("comments")) \
                .withColumn("weighted_score", col("final_score") * weights["comments"])
            all_recs = comment_recs if all_recs is None else all_recs.union(comment_recs)
        
        if watch_recs:
            watch_recs = watch_recs.withColumn("method", lit("watch_patterns")) \
                .withColumn("weighted_score", col("final_score") * weights["watch"])
            all_recs = watch_recs if all_recs is None else all_recs.union(watch_recs)
        
        if all_recs is None:
            return None
        
        # Aggregate scores for videos appearing in multiple methods
        final_recs = all_recs.groupBy("video_id") \
            .agg(
                sum("weighted_score").alias("total_score"),
                collect_list("method").alias("recommendation_methods"),
                count("*").alias("method_count")
            ) \
            .withColumn(
                "diversity_bonus",
                when(col("method_count") >= 3, 10)
                .when(col("method_count") == 2, 5)
                .otherwise(0)
            ) \
            .withColumn(
                "final_score",
                col("total_score") + col("diversity_bonus")
            ) \
            .orderBy(col("final_score").desc()) \
            .limit(n)
        
        return final_recs
    
    def generate_recommendations_for_all_users(self, interactions, user_profiles, video_stats):
        """
        Generate recommendations for all users (distributed)
        """
        
        unique_users = interactions.select("user_id").distinct()
        
        # This would be done in batches in production
        # For now, focusing on the algorithm
        
        print(f"Generating recommendations for {unique_users.count()} users...")
        
        return unique_users

# Main execution function
def run_behavioral_recommendations(spark, mongodb_uri, redis_client=None):
    """Run behavioral recommendation pipeline"""
    
    print("\n" + "="*80)
    print("BEHAVIORAL RECOMMENDATION SYSTEM")
    print("="*80)
    
    recommender = BehavioralRecommender(spark, mongodb_uri, redis_client)
    
    # Load data
    interactions, user_profiles, videos = recommender.load_data()
    
    # Calculate video scores
    video_stats = recommender.calculate_video_scores(interactions)
    
    print(f"\nProcessed {interactions.count()} interactions")
    print(f"Analyzed {video_stats.count()} videos")
    
    return recommender, video_stats
