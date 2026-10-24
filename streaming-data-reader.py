"""Consume toll events from Kafka and persist them in MySQL.

This consumer expects JSON events produced by toll_traffic_generator.py.
Set TOLL_MYSQL_PASSWORD in the terminal environment before starting it.
"""

import json  # Decode JSON messages received from Kafka.
import os  # Read configuration and credentials from environment variables.
from datetime import datetime  # Convert timestamp text to a Python datetime.

import mysql.connector  # MySQL driver used to insert each event into the database.
from kafka import KafkaConsumer  # Kafka client used to subscribe to a topic.


# Connection settings can be changed through environment variables when reproducing the project.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "toll")  # Must match the producer's topic.
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "toll-mysql-consumer")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "tolldata")
MYSQL_USER = os.getenv("MYSQL_USER", "tolluser")
MYSQL_PASSWORD = os.getenv("TOLL_MYSQL_PASSWORD")  # Keep the password out of source code.
MYSQL_TABLE = "livetolldata"  # Table created in the lab's MySQL setup.

# Fail clearly before starting Kafka consumption if the required secret is missing.
if not MYSQL_PASSWORD:
    raise SystemExit("TOLL_MYSQL_PASSWORD is not set. Export it in this terminal before running the consumer.")


def decode_json_message(raw_message):
    """Decode Kafka message bytes and parse the JSON event into a Python dictionary."""
    return json.loads(raw_message.decode("utf-8"))  # Decode UTF-8 bytes, then parse JSON.


def main():
    """Read events, insert each one into MySQL, then commit its Kafka offset."""
    # Connect to the local MySQL database before beginning to consume messages.
    connection = mysql.connector.connect(
        host=MYSQL_HOST,  # MySQL server address.
        port=MYSQL_PORT,  # MySQL server port.
        database=MYSQL_DATABASE,  # Database that contains the target table.
        user=MYSQL_USER,  # Dedicated database account for this consumer.
        password=MYSQL_PASSWORD,  # Read the secret from the environment variable.
        connection_timeout=10,  # Avoid waiting indefinitely if MySQL is unavailable.
    )

    # Disable automatic offset commits so an event is acknowledged only after the DB insert succeeds.
    consumer = KafkaConsumer(
        TOPIC,  # Subscribe to the toll topic, or the topic supplied in KAFKA_TOPIC.
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),  # Support a list of Kafka brokers.
        group_id=CONSUMER_GROUP,  # Identify this application's consumer group.
        enable_auto_commit=False,  # Commit offsets only after the database transaction succeeds.
        auto_offset_reset="earliest",  # Read from the earliest available offset for a new group.
        value_deserializer=decode_json_message,  # Convert message bytes to a dictionary.
    )

    # Use a parameterized INSERT so values are handled safely by the database driver.
    insert_sql = (
        f"INSERT INTO `{MYSQL_TABLE}` "
        "(`timestamp`, `vehicle_id`, `vehicle_type`, `toll_plaza_id`) "
        "VALUES (%s, %s, %s, %s)"
    )

    print(f"Connected to MySQL database '{MYSQL_DATABASE}'. Reading Kafka topic '{TOPIC}'. Press Ctrl+C to stop.")

    try:
        with connection.cursor() as cursor:  # Create a cursor for executing the parameterized INSERT.
            for message in consumer:  # Iterate as Kafka messages arrive.
                event = message.value  # The deserializer already converted the JSON to a dictionary.

                # Check required keys before attempting the database insert.
                required_fields = {"timestamp", "vehicle_id", "vehicle_type", "toll_plaza_id"}
                if not isinstance(event, dict) or not required_fields.issubset(event):
                    raise ValueError(f"Message is missing required fields: {event}")

                # Convert the timestamp string into a datetime value MySQL can store.
                event_timestamp = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
                values = (
                    event_timestamp,  # Event time.
                    int(event["vehicle_id"]),  # Numeric vehicle identifier.
                    str(event["vehicle_type"]),  # Vehicle category.
                    int(event["toll_plaza_id"]),  # Numeric toll plaza identifier.
                )

                try:
                    cursor.execute(insert_sql, values)  # Insert this event into MySQL.
                    connection.commit()  # Persist the record before acknowledging the Kafka message.
                    consumer.commit()  # Commit the consumed Kafka offset after the DB commit.
                    print(f"Stored event in MySQL: {event}")  # Show successful processing in the terminal.
                except Exception:
                    connection.rollback()  # Undo any uncommitted database work after an insert failure.
                    raise  # Surface the error instead of silently losing the event.
    except KeyboardInterrupt:
        print("Consumer stopped by user.")  # Explain why consumption ended.
    finally:
        consumer.close()  # Release Kafka resources.
        connection.close()  # Release the MySQL connection.


if __name__ == "__main__":
    main()  # Start the consumer only when this file is run directly.
