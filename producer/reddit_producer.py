import praw
from kafka import KafkaProducer
import json
import logging
import sys
import time

# It's good practice to have configuration in a separate file.
from config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    KAFKA_BOOTSTRAP_SERVERS, # e.g., ['localhost:9092']
    KAFKA_TOPIC,             # e.g., 'reddit_stream'
    SUBREDDIT_NAME           # e.g., 'technology'
)

# 1. Set up proper logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 2. Add callbacks for handling send results
def on_send_success(record_metadata):
    """Callback for successful message sending."""
    logging.info(f"Message sent successfully to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")

def on_send_error(excp):
    """Callback for failed message sending."""
    logging.error('Error sending message', exc_info=excp)

def main():
    """Main function to stream Reddit data to Kafka."""
    # Set up Reddit instance
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            read_only=True # Good practice for stream-only scripts
        )
        reddit.user.me() # Verify authentication
        logging.info("✅ Successfully connected to Reddit API.")
    except Exception as e:
        logging.error(f"❌ Could not connect to Reddit: {e}")
        sys.exit(1) # Exit if connection fails

    # Set up Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # Add retries for producer resilience
        retries=5,
        # Acknowledge from all replicas to ensure data isn't lost
        acks='all'
    )

    logging.info(f"🚀 Streaming from r/{SUBREDDIT_NAME} to Kafka topic '{KAFKA_TOPIC}'...")

    try:
        # 3. Stream submissions and handle potential PRAW errors
        # The `stream` method handles rate limits automatically. The explicit `time.sleep` is often not needed.
        for submission in reddit.subreddit(SUBREDDIT_NAME).stream.submissions(skip_existing=True):
            data = {
                'id': submission.id,
                'title': submission.title,
                'author': str(submission.author),
                'created_utc': submission.created_utc,
                'subreddit': str(submission.subreddit),
                'url': submission.url,
                'score': submission.score,
                'num_comments': submission.num_comments
            }

            # The send operation is asynchronous
            producer.send(KAFKA_TOPIC, value=data).add_callback(on_send_success).add_errback(on_send_error)
            logging.info(f"Queued submission: {data['title'][:50]}...")

    except KeyboardInterrupt:
        logging.info("🛑 Stream stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        # 4. Ensure all buffered messages are sent before exiting
        logging.info("Flushing remaining messages...")
        producer.flush()
        logging.info("Closing Kafka producer.")
        producer.close()

if __name__ == "__main__":
    main()