"""
Submit Spark Streaming job to process events in real-time
"""

import subprocess
import time
import sys

def submit_spark_job():
    """Submit Spark Streaming job"""
    
    spark_submit_cmd = [
        'docker', 'exec', 'spark-master',
        '/opt/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--packages', 
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0',
        '--driver-memory', '2g',
        '--executor-memory', '2g',
        '--executor-cores', '2',
        '/opt/spark-jobs/streaming_consumer.py'
    ]
    
    print("Submitting Spark Streaming job...")
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
            print("\n✅ Spark job submitted successfully!")
        else:
            print(f"\n❌ Spark job failed with return code {process.returncode}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error submitting Spark job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    submit_spark_job()
