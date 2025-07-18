# spark_streaming/spark_config.py

from pyspark.sql import SparkSession
import os

def get_spark_session():
    """
    Creates and returns a SparkSession with the necessary configurations
    for the Reddit streaming job. It pulls configurations from environment
    variables for better flexibility.
    """
    # Define the required JAR packages for Kafka and PostgreSQL
    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.5.0"
    ]

    spark = SparkSession.builder \
        .appName("RedditKafkaConsumer") \
        .config("spark.jars.packages", ",".join(jar_packages)) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # Set a log level to reduce console output verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark