"""Generate simulated toll events and publish them to a Kafka topic.

Run from the AirflowKafka project virtual environment. The topic and broker
can be changed with KAFKA_TOPIC and KAFKA_BOOTSTRAP_SERVERS environment vars.
"""

import json  # Serialize each toll event as JSON before sending it to Kafka.
import os  # Read optional configuration from environment variables.
import random  # Generate realistic-looking sample vehicle and plaza values.
import time  # Pause between messages to simulate a live traffic stream.
from datetime import datetime  # Create the event timestamp.

from kafka import KafkaProducer  # Kafka client used to publish events.


# Use localhost for Kafka on this same Ubuntu/WSL environment unless overridden.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Use the course topic by default; set KAFKA_TOPIC to reuse this producer elsewhere.
TOPIC = os.getenv("KAFKA_TOPIC", "toll")
# Wait one second between events by default; override with TOLL_EVENT_INTERVAL.
EVENT_INTERVAL_SECONDS = float(os.getenv("TOLL_EVENT_INTERVAL", "1"))

# Sample values for this learning project; replace these lists with real source data if available.
VEHICLE_TYPES = ["car", "bus", "truck", "van"]
TOLL_PLAZA_IDS = [4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009, 4010]


def build_toll_event():
    """Build one record whose keys match the MySQL table columns."""
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # MySQL-friendly DATETIME format.
        "vehicle_id": random.randint(1_000_000, 9_999_999),  # Generate a numeric anonymized vehicle ID.
        "vehicle_type": random.choice(VEHICLE_TYPES),  # Pick a sample vehicle type.
        "toll_plaza_id": random.choice(TOLL_PLAZA_IDS),  # Pick a sample toll plaza ID.
    }


def main():
    """Connect to Kafka and publish toll events until interrupted by the user."""
    # JSON serialization makes the event structure explicit and easy for the consumer to parse.
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),  # Support one or more comma-separated brokers.
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),  # Convert dictionaries to UTF-8 JSON bytes.
        acks="all",  # Wait for acknowledgement from the in-sync replicas available to the topic.
        retries=5,  # Retry transient send failures a few times.
    )

    print(f"Producing toll events to topic '{TOPIC}' at {BOOTSTRAP_SERVERS}. Press Ctrl+C to stop.")

    try:
        while True:  # Keep producing until stopped by Ctrl+C.
            event = build_toll_event()  # Create the next simulated toll event.
            metadata = producer.send(TOPIC, value=event).get(timeout=10)  # Send and wait for Kafka acknowledgement.
            print(f"Sent event to {metadata.topic}, partition {metadata.partition}, offset {metadata.offset}: {event}")
            time.sleep(EVENT_INTERVAL_SECONDS)  # Control the event rate.
    except KeyboardInterrupt:
        print("Producer stopped by user.")  # Explain why the loop ended.
    finally:
        producer.flush(timeout=10)  # Wait for any buffered messages to be delivered.
        producer.close(timeout=10)  # Release the producer's network resources.


if __name__ == "__main__":
    main()  # Run the producer only when this file is executed directly.
