# Apache Kafka Streaming ETL Pipeline: Toll Traffic to MySQL


This project demonstrates a real-time streaming data pipeline. A Python producer generates simulated toll traffic events and publishes them to Apache Kafka. A Python consumer reads the events, validates and converts their fields, then inserts them into a MySQL table.

The producer and consumer are designed to work together using the same JSON message structure. The project can be reused with another Kafka topic or local configuration through environment variables.

## Architecture

```text
┌───────────────────────────┐
│ Python toll event producer│
│ toll_traffic_generator.py │
└─────────────┬─────────────┘
              │ JSON event
              ▼
┌───────────────────────────┐
│ Apache Kafka              │
│ topic: toll               │
└─────────────┬─────────────┘
              │ Kafka message
              ▼
┌───────────────────────────┐
│ Python consumer           │
│ streaming-data-reader.py  │
└─────────────┬─────────────┘
              │ parameterized INSERT
              ▼
┌───────────────────────────┐
│ MySQL                     │
│ tolldata.livetolldata     │
└───────────────────────────┘
```

### How the components connect

1. **Producer:** creates one event at a time with a timestamp, vehicle ID, vehicle type, and toll plaza ID. It serializes the dictionary as JSON and sends it to Kafka.
2. **Kafka topic:** buffers and distributes the events, keeping the producer and consumer separate. The broker used by default is `localhost:9092`.
3. **Consumer:** subscribes to the same topic, decodes each JSON message, validates the required fields, and converts values to the expected Python types.
4. **MySQL:** the consumer inserts each event into `tolldata.livetolldata` using a parameterized SQL statement.
5. **Offset handling:** the consumer commits the Kafka offset after the corresponding database transaction commits. This reduces the risk of acknowledging a message before it has been stored, although a crash between the database commit and the Kafka offset commit can still result in a duplicate insert.

## Project files

```text
AirflowKafka/
├── README.md                       # Project overview and quick-start guide
├── KAFKA_STREAMING_ETL_GUIDE.md     # Detailed reproduction and troubleshooting guide
├── toll_traffic_generator.py        # Kafka producer
└── streaming-data-reader.py         # Kafka consumer and MySQL writer
```

The files are intended to be placed in your existing `~/IBMDEexercises/AirflowKafka` workspace. The Kafka installation itself is located in `kafka_2.12-3.7.0/` under that workspace and is not source code for this project.

## Tech stack

- **Python:** creates and processes streaming events.
- **Apache Kafka 3.7.0 with KRaft:** message broker and topic management.
- **`kafka-python`:** connects Python producer and consumer code to Kafka.
- **MySQL:** stores the processed toll events.
- **`mysql-connector-python`:** connects the consumer to MySQL.
- **Ubuntu on WSL:** local Linux runtime and shell environment.
- **VS Code:** code editing and project navigation.

## Event format and database schema

Each JSON message contains these keys:

```json
{
  "timestamp": "2025-10-23 12:30:00",
  "vehicle_id": 1234567,
  "vehicle_type": "car",
  "toll_plaza_id": 4001
}
```

The destination table is `tolldata.livetolldata` with these columns:

| Column | Purpose | Expected type |
|---|---|---|
| `timestamp` | Time of the toll event | `DATETIME` |
| `vehicle_id` | Anonymized numeric vehicle identifier | `INT` |
| `vehicle_type` | Category of vehicle | `CHAR(15)` |
| `toll_plaza_id` | Identifier of the toll plaza | `SMALLINT` |

## Setup and reproduction

### 1. Prepare the Python environment

```bash
cd ~/IBMDEexercises/AirflowKafka  # Use the existing workspace
source .venv/bin/activate  # Activate its Python virtual environment
uv pip install --python .venv/bin/python kafka-python mysql-connector-python  # Install Kafka and MySQL drivers
python -m py_compile toll_traffic_generator.py streaming-data-reader.py  # Check both scripts for syntax errors
```

### 2. Install and initialize Kafka (one-time setup)

Skip downloading or extracting Kafka if `kafka_2.12-3.7.0/` already exists.

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the existing workspace
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz  # Download Kafka if the archive is missing
tar -xzf kafka_2.12-3.7.0.tgz  # Extract the compressed archive into a Kafka directory
cd kafka_2.12-3.7.0  # Enter the extracted Kafka installation
```

Only initialize KRaft storage on the first setup. **Do not reformat storage after Kafka has already been initialized**, because doing so can make local Kafka data inaccessible.

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"  # Generate a unique cluster ID for a new local Kafka cluster
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties  # Initialize storage for KRaft
```

### 3. Start Kafka

In a dedicated terminal, start the Kafka broker and leave that terminal open:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-server-start.sh config/kraft/server.properties  # Start the Kafka broker
```
### 4. Create the Kafka topic

In a second terminal, create the `toll` topic once. If it already exists, skip the create command.

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092  # Create the topic for toll events
bin/kafka-topics.sh --list --bootstrap-server localhost:9092  # Verify that toll appears in the topic list
```

### 5. Start MySQL and create its database/table

```bash
sudo service mysql start  # Start the local MySQL server
sudo service mysql status  # Confirm that MySQL is running
sudo mysql  # Open the MySQL command-line client as local administrator
```

At the `mysql>` prompt, create the database and destination table if needed:

```sql
CREATE DATABASE IF NOT EXISTS tolldata;  -- Create the project database if it does not exist
USE tolldata;  -- Select the project database
CREATE TABLE IF NOT EXISTS livetolldata (  -- Create the table that stores events
    `timestamp` DATETIME,  -- Event timestamp
    vehicle_id INT,  -- Numeric vehicle identifier
    vehicle_type CHAR(15),  -- Vehicle type/category
    toll_plaza_id SMALLINT  -- Toll plaza identifier
);  -- Finish creating the table
EXIT;  -- Leave the MySQL client
```

Create a dedicated account for the consumer if it has not been created. Replace `CHOOSE_A_PASSWORD` with your own local password consistently; do not commit it to GitHub.

```sql
CREATE USER IF NOT EXISTS 'tolluser'@'localhost' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Create the local account
ALTER USER 'tolluser'@'localhost' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Set its password to your chosen value
GRANT SELECT, INSERT ON tolldata.livetolldata TO 'tolluser'@'localhost';  -- Allow reading/inserting events
CREATE USER IF NOT EXISTS 'tolluser'@'127.0.0.1' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Create the TCP loopback account
ALTER USER 'tolluser'@'127.0.0.1' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Set the same password for this account
GRANT SELECT, INSERT ON tolldata.livetolldata TO 'tolluser'@'127.0.0.1';  -- Allow reading/inserting events
FLUSH PRIVILEGES;  -- Apply the permission changes
```

Run the SQL user statements inside `sudo mysql`, not at the regular shell prompt.

### 6. Configure the MySQL password securely

In the terminal where you will launch the consumer, enter the password without displaying it:

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the project's Python environment
read -s -p "Enter MySQL password: " TOLL_MYSQL_PASSWORD  # Prompt for the password without echoing it
echo  # Move to a new line after password entry
export TOLL_MYSQL_PASSWORD  # Make the secret available to Python launched from this terminal
```

The consumer reads `TOLL_MYSQL_PASSWORD` from the environment. It is not hardcoded in the source file. Each new terminal needs its own environment variable setting.

The default configuration can be overridden with environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address(es), comma-separated if multiple |
| `KAFKA_TOPIC` | `toll` | Topic used by both producer and consumer |
| `TOLL_EVENT_INTERVAL` | `1` | Seconds between generated messages |
| `KAFKA_CONSUMER_GROUP` | `toll-mysql-consumer` | Consumer group identity |
| `MYSQL_HOST` | `127.0.0.1` | MySQL server address |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_DATABASE` | `tolldata` | Destination database |
| `MYSQL_USER` | `tolluser` | Database account |
| `TOLL_MYSQL_PASSWORD` | No default | Password supplied securely through the environment |

### 7. Start the consumer and producer

Keep Kafka running in its own terminal. Start the **consumer before the producer**, so it is ready to receive records.

**Consumer terminal:** set `TOLL_MYSQL_PASSWORD` as shown in step 6, then run:

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the virtual environment
python streaming-data-reader.py  # Read Kafka messages and insert them into MySQL
```

**Producer terminal:**

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the virtual environment
python toll_traffic_generator.py  # Generate and publish toll events continuously
```

The producer emits a JSON event every second by default. Press `Ctrl+C` in the producer and consumer terminals to stop them gracefully; then stop Kafka with `Ctrl+C` in the broker terminal if desired.

## Verify the pipeline

### Confirm the topic exists

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --list --bootstrap-server localhost:9092  # List Kafka topics
```

### Count stored events

```bash
sudo mysql -e 'SELECT COUNT(*) AS total_records FROM tolldata.livetolldata;'  # Count rows persisted in MySQL
```

Run the count more than once while the producer and consumer are running. An increasing count indicates new events are being stored.

### Inspect recent events

```bash
sudo mysql -e 'SELECT `timestamp`, vehicle_id, vehicle_type, toll_plaza_id FROM tolldata.livetolldata ORDER BY `timestamp` DESC LIMIT 10;'  # Display the latest ten events
```

## Use a different topic

Create another topic, such as `toll_demo`, once:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --create --topic toll_demo --bootstrap-server localhost:9092  # Create an alternate topic
```

Set the same topic in the **producer terminal and consumer terminal** before launching each script:

```bash
export KAFKA_TOPIC=toll_demo  # Tell this terminal's producer or consumer to use the alternate topic
```

Both scripts must use the same topic. The JSON message structure must also continue to match what the consumer expects and what the MySQL table can store.

## Troubleshooting

- **Producer cannot connect:** Confirm the Kafka broker is running and `KAFKA_BOOTSTRAP_SERVERS` points to the correct address.
- **Topic does not exist:** List topics, then create the exact topic configured in both scripts.
- **MySQL service is unavailable:** Run `sudo service mysql status`; start it with `sudo service mysql start` if needed.
- **MySQL access denied:** Confirm that `tolluser` has the password currently assigned to the account host used by `MYSQL_HOST`, and reset `TOLL_MYSQL_PASSWORD` in the consumer terminal if needed.
- **Consumer says the password is missing:** Set and export `TOLL_MYSQL_PASSWORD` in the same terminal where the consumer will run.
- **No records arrive:** Confirm the producer and consumer use the same topic and broker, and inspect both terminals for errors.
- **Python import fails:** Activate `.venv` and install `kafka-python` and `mysql-connector-python` in that environment.
- **Duplicate rows after a restart:** The sample does not implement event IDs or database deduplication. A crash after MySQL commits but before Kafka offset commit may lead to the same event being inserted again.

## Key concepts learned

- **Producer:** generates and publishes event messages.
- **Topic:** named Kafka stream that separates event production from consumption.
- **Consumer:** subscribes to a topic, decodes and validates each event, then writes it to a destination system.
- **Consumer group and offsets:** identify the consumer group and track how far it has progressed through a topic partition.
- **Parameterized SQL:** passes record values separately from SQL text when inserting rows.
- **Environment variables:** allow brokers, topics, database settings, and secrets to be configured outside source code.
- **Streaming pipeline:** the producer, Kafka broker, consumer, and database each have a distinct responsibility and can be monitored separately.
