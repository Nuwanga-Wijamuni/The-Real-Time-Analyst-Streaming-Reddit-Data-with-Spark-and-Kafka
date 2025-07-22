# producer/config.py

REDDIT_CLIENT_ID = "NKWBo6fApzVXbk-KkKYtpw"
REDDIT_CLIENT_SECRET = "CIlrAb4iDJF58EorEV6yseAcrb7klA"
REDDIT_USER_AGENT = "KafkaProducerScript/0.1 by No-Length-5195"

# --- Kafka Configuration ---
# This list should contain the address of your Kafka broker(s)
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# The Kafka topic you want to send messages to
KAFKA_TOPIC = 'reddit_stream'

# --- Subreddit Configuration ---
# The name of the subreddit you want to stream data from
SUBREDDIT_NAME = 'technology'