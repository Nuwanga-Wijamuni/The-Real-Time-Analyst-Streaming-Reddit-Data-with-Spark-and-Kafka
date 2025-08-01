# spark_streaming/spark_streaming_job.py

import os
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType
from textblob import TextBlob

# Import helper functions from other modules in the project
from spark_config import get_spark_session
from write_to_postgres import write_to_postgres

def get_sentiment(text):
    """Calculates sentiment polarity using TextBlob. Returns 0.0 for None or empty text."""
    if text:
        blob = TextBlob(text)
        return blob.sentiment.polarity
    return 0.0

def main():
    """Main function to run the Spark Streaming job."""
    # 1️⃣ Initialize Spark Session using the config file
    spark = get_spark_session()

    # Get Kafka bootstrap servers from environment variable for flexibility
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

    # 2️⃣ Define the schema for the incoming JSON data from Kafka
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("created_utc", LongType(), True),
        StructField("subreddit", StringType(), True),
        StructField("url", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("num_comments", IntegerType(), True)
    ])

    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "reddit_stream") \
        .option("startingOffsets", "latest") \
        .load()

    
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("author").isNotNull())

    # 5 Apply sentiment analysis UDF
    sentiment_udf = udf(get_sentiment, DoubleType())
    df_with_sentiment = parsed_df.withColumn("sentiment_score", sentiment_udf(col("title")))

    
    final_df = df_with_sentiment.select(
        col("created_utc").cast(TimestampType()).alias("timestamp"),
        col("author"),
        col("title").alias("body"),
        col("sentiment_score"),
        col("subreddit")
    )

    
    query = final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "./checkpoints/reddit_stream") \
        .trigger(processingTime='30 seconds') \
        .start()

    # Wait for the streaming query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main()

