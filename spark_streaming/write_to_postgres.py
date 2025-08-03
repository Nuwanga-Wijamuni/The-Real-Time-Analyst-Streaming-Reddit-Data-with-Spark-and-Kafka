def write_to_postgres(df, epoch_id):
    """
    Writes a micro-batch DataFrame to PostgreSQL using JDBC.
    """
    # Hardcoded database connection details to match docker-compose.yml
    db_url = "jdbc:postgresql://localhost:5432/reddit_db"
    db_user = "postgres"
    db_password = "postgres"
    db_table = "processed_comments"
    db_driver = "org.postgresql.Driver"

    # Write the DataFrame to the specified PostgreSQL table
    (df.write
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", db_table)
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", db_driver)
        .mode("append")
        .save()
    )
