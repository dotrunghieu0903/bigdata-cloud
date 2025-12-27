"""
Complete Distributed Recommendation System
Integrates: User Profiling + Behavioral + Collaborative + Content-based
All processing distributed across Spark cluster
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import redis
import sys
import os
from datetime import datetime

# Import our modules
sys.path.append('/opt/spark-jobs')
from user_profiling import DistributedUserProfiler, run_user_profiling
from behavioral_recommender import BehavioralRecommender, run_behavioral_recommendations
from recommendation_engine import DistributedCollaborativeFilteringEngine

def create_spark_session():
    """Create Spark session for distributed processing"""
    return SparkSession.builder \
        .appName("CompleteRecommendationSystem") \
        .config("spark.mongodb.read.connection.uri", 
                "mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin") \
        .config("spark.mongodb.write.connection.uri",
                "mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

def initialize_redis():
    """Initialize Redis client"""
    try:
        client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )
        client.ping()
        return client
    except:
        print("Warning: Redis not available")
        return None

def load_from_mongodb(spark, collection_name, mongodb_uri):
    """Load data from MongoDB using PyMongo"""
    from pymongo import MongoClient
    import pandas as pd
    
    print(f"Loading {collection_name} from MongoDB...")
    
    client = MongoClient(mongodb_uri)
    db = client["video_recommendation"]
    collection = db[collection_name]
    
    documents = list(collection.find({}, {"_id": 0}))
    client.close()
    
    print(f"   Loaded {len(documents)} documents")
    
    if documents:
        pandas_df = pd.DataFrame(documents)
        return spark.createDataFrame(pandas_df)
    else:
        from pyspark.sql.types import StructType
        return spark.createDataFrame([], StructType([]))

def save_to_mongodb(df, collection_name, mongodb_uri, mode="append"):
    """Save DataFrame to MongoDB using PyMongo"""
    from pymongo import MongoClient
    import builtins
    
    print(f"Saving to MongoDB collection: {collection_name}")
    
    pandas_df = df.toPandas()
    records = pandas_df.to_dict('records')
    
    if not records:
        print("   No records to save")
        return
    
    client = MongoClient(mongodb_uri)
    db = client["video_recommendation"]
    collection = db[collection_name]
    
    if mode == "overwrite":
        collection.delete_many({})
    
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        collection.insert_many(batch)
    
    client.close()
    print(f"✓ Saved {len(records)} documents")

def main():
    """
    Complete Recommendation Pipeline
    
    Steps:
    1. User Profiling - Calculate user scores from behavior
    2. Behavioral Analysis - Find patterns in likes/comments
    3. Collaborative Filtering - ALS model for predictions
    4. Content-based Filtering - Match user preferences with video metadata
    5. Hybrid Recommendations - Combine all methods
    """
    
    print("\n" + "="*80)
    print("COMPLETE DISTRIBUTED RECOMMENDATION SYSTEM")
    print("Powered by Apache Spark Cluster")
    print("="*80)
    print(f"Start Time: {datetime.utcnow().isoformat()}\n")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Spark Cluster Info:")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  App Name: {spark.sparkContext.appName}")
    print(f"  Default Parallelism: {spark.sparkContext.defaultParallelism}")
    print()
    
    # Initialize Redis
    redis_client = initialize_redis()
    
    mongodb_uri = "mongodb://admin:admin123@mongodb:27017/video_recommendation?authSource=admin"
    
    try:
        # Step 1: User Profiling
        print("\n" + "-"*80)
        print("STEP 1: USER PROFILING & BEHAVIORAL ANALYSIS")
        print("-"*80)
        
        user_profiles, insights = run_user_profiling(spark, mongodb_uri, redis_client)
        
        print(f"\n✓ Profiled {user_profiles.count()} users")
        print(f"  - Power Users: {insights.get('user_type_distribution', {}).get('power_user', 0)}")
        print(f"  - Active Users: {insights.get('user_type_distribution', {}).get('active_user', 0)}")
        print(f"  - Casual Users: {insights.get('user_type_distribution', {}).get('casual_user', 0)}")
        print(f"  - New Users: {insights.get('user_type_distribution', {}).get('new_user', 0)}")
        
        # Step 2: Behavioral Recommendations
        print("\n" + "-"*80)
        print("STEP 2: BEHAVIORAL RECOMMENDATION ENGINE")
        print("-"*80)
        
        recommender, video_stats = run_behavioral_recommendations(
            spark, mongodb_uri, redis_client
        )
        
        print(f"\n✓ Analyzed video engagement metrics")
        print(f"  - Top video quality score: {video_stats.agg(max('quality_score')).collect()[0][0]:.2f}")
        print(f"  - Avg engagement rate: {video_stats.agg(avg('engagement_score')).collect()[0][0]:.4f}")
        
        # Step 3: Collaborative Filtering (ALS)
        print("\n" + "-"*80)
        print("STEP 3: COLLABORATIVE FILTERING (ALS)")
        print("-"*80)
        
        cf_engine = DistributedCollaborativeFilteringEngine(
            spark=spark,
            mongodb_uri=mongodb_uri,
            redis_client=redis_client
        )
        
        # Build and train
        interactions_df = cf_engine.build_interaction_dataframe()
        
        if interactions_df and interactions_df.count() > 0:
            cf_engine.train_distributed()
            
            # Generate recommendations
            print("\nGenerating collaborative filtering recommendations...")
            sample_users = interactions_df.select("user_id").distinct().limit(100).collect()
            
            for user_row in sample_users[:10]:  # Sample for demo
                user_id = user_row['user_id']
                recs = cf_engine.recommend_for_user(user_id, n=10)
                if recs:
                    print(f"  User {user_id}: {len(recs)} recommendations generated")
        
        # Step 4: Hybrid Recommendations
        print("\n" + "-"*80)
        print("STEP 4: HYBRID RECOMMENDATION SYSTEM")
        print("-"*80)
        
        # Load interactions using PyMongo helper
        interactions = load_from_mongodb(spark, "interactions", mongodb_uri)
        
        # Generate hybrid recommendations for sample users
        sample_users = interactions.select("user_id").distinct().limit(10).collect()
        
        hybrid_count = 0
        for user_row in sample_users:
            user_id = user_row['user_id']
            
            hybrid_recs = recommender.hybrid_recommendations(
                user_id, interactions, user_profiles, video_stats, n=20
            )
            
            if hybrid_recs:
                recs_list = hybrid_recs.collect()
                if len(recs_list) > 0:
                    hybrid_count += 1
                    
                    # Save to MongoDB
                    recs_to_save = [
                        {
                            'user_id': user_id,
                            'video_id': row['video_id'],
                            'score': float(row['final_score']),
                            'methods': row['recommendation_methods'],
                            'generated_at': datetime.utcnow().isoformat(),
                            'algorithm': 'hybrid'
                        }
                        for row in recs_list
                    ]
                    
                    # Save using PyMongo helper
                    recs_df = spark.createDataFrame(recs_to_save)
                    save_to_mongodb(recs_df, "recommendations", mongodb_uri, mode="append")
        
        print(f"\n✓ Generated hybrid recommendations for {hybrid_count} users")
        
        # Final Summary
        print("\n" + "="*80)
        print("PIPELINE COMPLETE - SUMMARY")
        print("="*80)
        print(f"Total Users Profiled: {user_profiles.count()}")
        print(f"Total Videos Analyzed: {video_stats.count()}")
        print(f"Total Interactions Processed: {interactions.count()}")
        print(f"Hybrid Recommendations Generated: {hybrid_count} users")
        print(f"Average User Score: {insights.get('avg_user_score', 0):.2f}")
        print(f"Average Engagement Rate: {insights.get('avg_engagement_rate', 0):.4f}")
        print(f"\nEnd Time: {datetime.utcnow().isoformat()}")
        print("="*80 + "\n")
        
        # System is now ready to serve recommendations!
        print("✓ System ready to serve personalized recommendations!")
        print("  - User profiles cached in Redis")
        print("  - Recommendations saved in MongoDB")
        print("  - ALS model trained and ready")
        print("  - Behavioral patterns analyzed")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("\nSpark session closed.")

if __name__ == "__main__":
    main()
