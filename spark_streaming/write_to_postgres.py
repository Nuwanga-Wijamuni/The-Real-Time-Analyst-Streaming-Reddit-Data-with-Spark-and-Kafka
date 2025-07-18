# spark_streaming/write_to_postgres.py
import os

def write_to_postgres(df, epoch_id):
    """
    Writes a micro-batch DataFrame to PostgreSQL using JDBC.
    This function is designed to be used with foreachBatch in Spark Structured Streaming.
    It securely fetches database credentials from environment variables.
    """
    # Fetch database connection details from environment variables
    # This avoids hardcoding secrets and makes the application more configurable.
    db_url = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/reddit_db")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "mypassword")
    db_table = os.getenv("POSTGRES_TABLE", "processed_comments")

    # Write the DataFrame to the specified PostgreSQL table
    df.write \
      .format("jdbc") \
      .option("url", db_url) \
      .option("dbtable", db_table) \
      .option("user", db_user) \
      .option("password", db_password) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()