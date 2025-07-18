from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, LongType, IntegerType, DoubleType
from textblob import TextBlob

# 1️⃣ Spark Session
spark = SparkSession.builder \
    .appName("RedditKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.5") \
    .getOrCreate()

# 2️⃣ Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_stream") \
    .load()

# 3️⃣ JSON schema — matches your producer fields
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("author", StringType()) \
    .add("created_utc", LongType()) \
    .add("subreddit", StringType()) \
    .add("url", StringType()) \
    .add("score", IntegerType()) \
    .add("num_comments", IntegerType())

# 4️⃣ Parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5️⃣ Sentiment UDF
def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

sentiment_udf = udf(get_sentiment, DoubleType())
df_with_sentiment = df_parsed.withColumn("sentiment_score", sentiment_udf(col("title")))

# 6️⃣ Rename + select for DB
final_df = df_with_sentiment.selectExpr(
    "timestamp(current_timestamp()) as timestamp",
    "author",
    "title as body",
    "sentiment_score",
    "subreddit"
)

# 7️⃣ Write to PostgreSQL (batch-wise)
query = final_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "C:/Users/Nuwanga Wijamuni/realtime_reddit_pipeline/spark_streaming/checkpoints/reddit_stream") \
    .foreachBatch(lambda df, epochId: df.write
                  .format("jdbc")
                  .option("url", "jdbc:postgresql://localhost:5432/reddit_db")
                  .option("dbtable", "processed_comments")
                  .option("user", "postgres")
                  .option("password", "mypassword")
                  .option("driver", "org.postgresql.Driver")
                  .mode("append")
                  .save()) \
    .start()


query.awaitTermination()
