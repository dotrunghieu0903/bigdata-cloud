"""
Video Recommendation Training with Spark MLlib
Uses MicroLens-100k dataset to train collaborative filtering model
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import RankingMetrics
import sys
import os
from datetime import datetime
from pathlib import Path

def create_spark_session():
    """Create Spark session with required configurations"""
    return SparkSession.builder \
        .appName("VideoRecommendationTraining") \
        .config("spark.mongodb.input.uri", "mongodb://admin:admin123@mongodb:27017/video_recommendation.interactions?authSource=admin") \
        .config("spark.mongodb.output.uri", "mongodb://admin:admin123@mongodb:27017/video_recommendation.models?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def load_microlens_data(spark, data_path="/data/MicroLens-100k"):
    """
    Load MicroLens-100k dataset
    Returns DataFrame with columns: userId, videoId, timestamp, implicit_rating
    """
    
    # Load interaction pairs
    pairs_df = spark.read.csv(
        f"{data_path}/MicroLens-100k_pairs.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"Loaded {pairs_df.count()} interactions")
    pairs_df.printSchema()
    pairs_df.show(5)
    
    # Create implicit ratings (1.0 for each interaction)
    # In future, we can weight by watch_time, likes, etc.
    interactions_df = pairs_df.select(
        col("userID").alias("userId"),
        col("videoID").alias("videoId"),
        col("timestamp").cast("long")
    ).withColumn("rating", col("timestamp").cast("double") * 0.0 + 1.0)
    
    # Count interactions per user-video pair for implicit feedback
    ratings_df = interactions_df.groupBy("userId", "videoId").agg(
        count("*").alias("interaction_count"),
        avg("timestamp").alias("avg_timestamp")
    ).withColumn("rating", col("interaction_count").cast("double"))
    
    print(f"\nGenerated {ratings_df.count()} unique user-video pairs")
    print("\nRating distribution:")
    ratings_df.select("rating").describe().show()
    
    return ratings_df

def train_als_model(spark, ratings_df, checkpoint_dir="/data/checkpoints"):
    """
    Train ALS (Alternating Least Squares) model
    """
    
    print("\n" + "="*80)
    print("TRAINING ALS MODEL")
    print("="*80)
    
    # Split data
    (training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"\nTraining set: {training.count()} interactions")
    print(f"Test set: {test.count()} interactions")
    
    # Set checkpoint directory for iterative algorithms
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    # Build ALS model
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="userId",
        itemCol="videoId",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=True,  # For implicit feedback
        nonnegative=True
    )
    
    # Hyperparameter tuning with CrossValidator
    paramGrid = ParamGridBuilder() \
        .addGrid(als.rank, [10, 20, 30]) \
        .addGrid(als.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(als.alpha, [1.0, 10.0, 40.0]) \
        .build()
    
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    # Use smaller number of folds for faster training
    cv = CrossValidator(
        estimator=als,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=4
    )
    
    print("\nStarting cross-validation training...")
    print(f"Testing {len(paramGrid)} parameter combinations")
    
    # Train model
    cv_model = cv.fit(training)
    best_model = cv_model.bestModel
    
    print("\nBest Model Parameters:")
    print(f"  Rank: {best_model.rank}")
    print(f"  RegParam: {best_model._java_obj.parent().getRegParam()}")
    print(f"  Alpha: {best_model._java_obj.parent().getAlpha()}")
    
    # Evaluate on test set
    predictions = best_model.transform(test)
    rmse = evaluator.evaluate(predictions)
    
    print(f"\nModel Performance:")
    print(f"  RMSE: {rmse:.4f}")
    
    # Show sample predictions
    print("\nSample Predictions:")
    predictions.select("userId", "videoId", "rating", "prediction").show(10)
    
    return best_model, training, test

def generate_recommendations(spark, model, ratings_df, num_recommendations=20):
    """
    Generate recommendations for all users
    """
    
    print("\n" + "="*80)
    print("GENERATING RECOMMENDATIONS")
    print("="*80)
    
    # Get unique users
    users = ratings_df.select("userId").distinct()
    
    # Generate top N recommendations for all users
    user_recs = model.recommendForAllUsers(num_recommendations)
    
    print(f"\nGenerated recommendations for {user_recs.count()} users")
    
    # Flatten recommendations
    user_recs_flat = user_recs.select(
        col("userId"),
        explode(col("recommendations")).alias("recommendation")
    ).select(
        col("userId"),
        col("recommendation.videoId").alias("videoId"),
        col("recommendation.rating").alias("score")
    )
    
    print("\nSample recommendations:")
    user_recs_flat.show(20, truncate=False)
    
    return user_recs, user_recs_flat

def evaluate_model(spark, model, test_df, k=10):
    """
    Evaluate model using ranking metrics
    """
    
    print("\n" + "="*80)
    print("EVALUATING MODEL")
    print("="*80)
    
    # Generate predictions for test users
    predictions = model.recommendForUserSubset(
        test_df.select("userId").distinct(),
        k
    )
    
    # Get actual items from test set
    actual = test_df.groupBy("userId").agg(
        collect_list("videoId").alias("actual_videos")
    )
    
    # Get predicted items
    predicted = predictions.select(
        col("userId"),
        col("recommendations.videoId").alias("predicted_videos")
    )
    
    # Join actual and predicted
    eval_df = actual.join(predicted, "userId", "inner")
    
    print(f"\nEvaluating {eval_df.count()} users")
    
    return eval_df

def save_model_to_mongodb(spark, model, user_recs_flat):
    """
    Save model and recommendations to MongoDB
    """
    
    print("\n" + "="*80)
    print("SAVING TO MONGODB")
    print("="*80)
    
    # Save recommendations
    user_recs_flat.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "video_recommendation") \
        .option("collection", "recommendations") \
        .save()
    
    print(f"Saved {user_recs_flat.count()} recommendations to MongoDB")
    
    # Save model metadata
    metadata = spark.createDataFrame([{
        "model_type": "ALS",
        "rank": model.rank,
        "trained_at": datetime.utcnow().isoformat(),
        "num_users": user_recs_flat.select("userId").distinct().count(),
        "num_videos": user_recs_flat.select("videoId").distinct().count(),
        "version": "1.0"
    }])
    
    metadata.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "video_recommendation") \
        .option("collection", "model_metadata") \
        .save()
    
    print("Saved model metadata to MongoDB")

def save_model_checkpoint(model, path="/data/checkpoints/als_model"):
    """Save model checkpoint"""
    try:
        model.write().overwrite().save(path)
        print(f"\nModel saved to {path}")
    except Exception as e:
        print(f"Warning: Could not save model checkpoint: {e}")

def main():
    """Main training pipeline"""
    
    print("\n" + "="*80)
    print("VIDEO RECOMMENDATION TRAINING WITH SPARK MLLIB")
    print("="*80)
    print(f"Start time: {datetime.utcnow().isoformat()}\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Import collect_list here after spark session is created
    from pyspark.sql.functions import collect_list
    
    try:
        # Load data
        data_path = "/opt/data/MicroLens-100k" if os.path.exists("/opt/data") else "data/MicroLens-100k"
        ratings_df = load_microlens_data(spark, data_path)
        
        # Train model
        model, training, test = train_als_model(spark, ratings_df)
        
        # Generate recommendations
        user_recs, user_recs_flat = generate_recommendations(spark, model, ratings_df)
        
        # Save to MongoDB
        save_model_to_mongodb(spark, model, user_recs_flat)
        
        # Save model checkpoint
        checkpoint_path = "/opt/data/checkpoints/als_model" if os.path.exists("/opt/data") else "data/checkpoints/als_model"
        save_model_checkpoint(model, checkpoint_path)
        
        print("\n" + "="*80)
        print("TRAINING COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"End time: {datetime.utcnow().isoformat()}")
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
