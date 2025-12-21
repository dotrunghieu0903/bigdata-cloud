"""
Train the recommendation model using collected data
This script should be run periodically (e.g., daily) to update the model
"""

import subprocess
import sys

def train_model():
    """Train recommendation model"""
    
    spark_submit_cmd = [
        'docker', 'exec', 'spark-master',
        'spark-submit',
        '--master', 'spark://spark-master:7077',
        '--driver-memory', '4g',
        '--executor-memory', '4g',
        '--executor-cores', '2',
        '/opt/spark-jobs/recommendation_engine.py'
    ]
    
    print("Training recommendation model...")
    print(f"Command: {' '.join(spark_submit_cmd)}")
    
    try:
        process = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Stream output
        for line in process.stdout:
            print(line, end='')
        
        process.wait()
        
        if process.returncode == 0:
            print("\n✅ Model training completed successfully!")
        else:
            print(f"\n❌ Model training failed with return code {process.returncode}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error training model: {e}")
        sys.exit(1)

if __name__ == "__main__":
    train_model()
