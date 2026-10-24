# Kafka Streaming ETL: Toll Traffic to MySQL

This is a reusable, local learning project based on the IBM Skills Network *Build a Streaming ETL Pipeline using Kafka* lab. It runs in the existing `~/IBMDEexercises/AirflowKafka` workspace and does not create a separate top-level project directory.

## What the pipeline does

```text
Toll traffic producer  ->  Kafka topic  ->  Python consumer  ->  MySQL table
```

- **Producer (`toll_traffic_generator.py`)** creates simulated traffic events and publishes each event as JSON to Kafka.
- **Kafka topic (`toll`)** buffers/distributes the events between the producer and consumer.
- **Consumer (`streaming-data-reader.py`)** reads JSON events, inserts them into MySQL, then commits the Kafka offset.
- **MySQL (`tolldata.livetolldata`)** stores the events for querying.

The producer and consumer in this package are designed to work together because they agree on a JSON message format with four keys: `timestamp`, `vehicle_id`, `vehicle_type`, and `toll_plaza_id`. They are an improved, reusable version for this project, not a byte-for-byte copy of the course's sample scripts.

## Files

- `toll_traffic_generator.py`: event producer
- `streaming-data-reader.py`: Kafka consumer and MySQL writer

## 1. Workspace and Python dependencies

Run these shell commands in Ubuntu/WSL. Each comment explains the command.

```bash
cd ~/IBMDEexercises/AirflowKafka  # Use the existing AirflowKafka workspace
source .venv/bin/activate  # Activate the existing Python virtual environment
uv pip install --python .venv/bin/python kafka-python mysql-connector-python  # Install the Kafka and MySQL Python drivers
```

Copy these two `.py` files into `~/IBMDEexercises/AirflowKafka/`, replacing the earlier downloaded tutorial scripts if you want to use this JSON-based version. Do not run the older consumer with this producer because their message formats may differ.

Check Python syntax:

```bash
python -m py_compile toll_traffic_generator.py streaming-data-reader.py  # Compile both scripts to catch syntax errors
```

No output means the syntax check passed.

## 2. Install and start Kafka (one-time setup)

The lab uses Kafka 3.7.0 in KRaft mode. The commands below assume the archive has already been downloaded and extracted into the existing workspace.

```bash
cd ~/IBMDEexercises/AirflowKafka  # Move to the existing project directory
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz  # Download Kafka 3.7.0 if it is not already present
tar -xzf kafka_2.12-3.7.0.tgz  # Extract the compressed archive into kafka_2.12-3.7.0
cd kafka_2.12-3.7.0  # Enter the extracted Kafka installation
```

Only for the **first-time Kafka storage setup**, format its storage directory:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"  # Generate a new unique cluster ID
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties  # Initialize Kafka storage for KRaft
```

**Do not repeat the format command on an already initialized cluster.** Formatting initialized storage can prevent access to existing local Kafka data.

Start the Kafka broker in its own terminal and leave it running:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-server-start.sh config/kraft/server.properties  # Start the Kafka broker
```

Create the topic once, using another terminal:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092  # Create the default toll topic
bin/kafka-topics.sh --list --bootstrap-server localhost:9092  # Confirm that toll appears in the topic list
```

If the topic already exists, you do not need to create it again.

## 3. Start MySQL and create the table (one-time setup)

```bash
sudo service mysql start  # Start the existing MySQL server inside Ubuntu
sudo service mysql status  # Confirm that MySQL is active
```

Create the database and table if they do not already exist:

```bash
sudo mysql  # Open the MySQL command-line client as the local administrator
```

Then run these SQL statements at the `mysql>` prompt:

```sql
CREATE DATABASE IF NOT EXISTS tolldata;  -- Create the project database if needed
USE tolldata;  -- Select the project database
CREATE TABLE IF NOT EXISTS livetolldata (  -- Create the streaming destination table if needed
    `timestamp` DATETIME,  -- Event time
    vehicle_id INT,  -- Anonymized vehicle identifier
    vehicle_type CHAR(15),  -- Vehicle category
    toll_plaza_id SMALLINT  -- Toll plaza identifier
);  -- Finish creating the table
EXIT;  -- Leave MySQL; enter EXIT on its own, without a trailing SQL comment
```

The consumer uses a dedicated account named `tolluser`. If you need to create/reset it, replace `CHOOSE_A_PASSWORD` with your own strong local password in each matching statement. Do not commit this password to GitHub.

```sql
CREATE USER IF NOT EXISTS 'tolluser'@'localhost' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Create the local account
ALTER USER 'tolluser'@'localhost' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Make its password match your chosen value
GRANT SELECT, INSERT ON tolldata.livetolldata TO 'tolluser'@'localhost';  -- Allow reading/inserting table rows
CREATE USER IF NOT EXISTS 'tolluser'@'127.0.0.1' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Create the TCP-loopback account
ALTER USER 'tolluser'@'127.0.0.1' IDENTIFIED BY 'CHOOSE_A_PASSWORD';  -- Make its password match the same value
GRANT SELECT, INSERT ON tolldata.livetolldata TO 'tolluser'@'127.0.0.1';  -- Allow reading/inserting table rows
FLUSH PRIVILEGES;  -- Apply privilege changes
```

Run those user-management statements inside `sudo mysql`, not at the ordinary Linux prompt. To test the connection, use the Python command from the project virtual environment, after setting `TOLL_MYSQL_PASSWORD` as shown below.

## 4. Set configuration for the producer and consumer

The scripts use sensible defaults for this local setup:

| Setting | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `toll` | Topic shared by producer and consumer |
| `MYSQL_HOST` | `127.0.0.1` | MySQL server address |
| `MYSQL_PORT` | `3306` | MySQL server port |
| `MYSQL_DATABASE` | `tolldata` | Destination database |
| `MYSQL_USER` | `tolluser` | Database account |
| `TOLL_MYSQL_PASSWORD` | No default | Password supplied through the environment, not stored in source |
| `TOLL_EVENT_INTERVAL` | `1` | Seconds between produced events |

Before starting the consumer, open a terminal and set the database password without displaying it:

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the virtual environment
read -s -p "Enter MySQL password: " TOLL_MYSQL_PASSWORD  # Prompt without echoing the password
echo  # Move to a new line
export TOLL_MYSQL_PASSWORD  # Make the password available to Python processes launched from this terminal
```

Test the database connection:

```bash
python - <<'PY'
import os  # Read the password from the environment
import mysql.connector  # Load the MySQL driver

connection = mysql.connector.connect(  # Try connecting with the same settings as the consumer
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),  # Use the local MySQL address
    port=int(os.getenv("MYSQL_PORT", "3306")),  # Use the MySQL port
    database=os.getenv("MYSQL_DATABASE", "tolldata"),  # Select the project database
    user=os.getenv("MYSQL_USER", "tolluser"),  # Use the dedicated account
    password=os.environ["TOLL_MYSQL_PASSWORD"],  # Get the secret from the environment
)
print("MySQL connection successful!")  # Confirm that authentication worked
connection.close()  # Release the test connection
PY
```

## 5. Run the streaming pipeline

Keep Kafka running in its own terminal. Start the consumer **before** the producer so it is ready to receive messages.

**Terminal A: Kafka broker**

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka installation directory
bin/kafka-server-start.sh config/kraft/server.properties  # Start Kafka; leave this terminal open
```

**Terminal B: Consumer**

Set `TOLL_MYSQL_PASSWORD` in this terminal using the prompt shown in section 4, then run:

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the virtual environment
python streaming-data-reader.py  # Read Kafka events and insert them into MySQL
```

**Terminal C: Producer**

```bash
cd ~/IBMDEexercises/AirflowKafka  # Go to the project directory
source .venv/bin/activate  # Activate the virtual environment
python toll_traffic_generator.py  # Generate and publish sample traffic events
```

Keep all three processes running while testing. Press `Ctrl+C` in the producer and consumer terminals when you want to stop them gracefully.

## 6. Verify the pipeline

List topics:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --list --bootstrap-server localhost:9092  # List existing topics
```

Count rows in MySQL:

```bash
sudo mysql -e 'SELECT COUNT(*) AS total_records FROM tolldata.livetolldata;'  # Count stored events
```

Inspect the latest 10 records:

```bash
sudo mysql -e 'SELECT `timestamp`, vehicle_id, vehicle_type, toll_plaza_id FROM tolldata.livetolldata ORDER BY `timestamp` DESC LIMIT 10;'  # Display recent toll events
```

If the count increases over time while producer and consumer run, the full producer-to-Kafka-to-consumer-to-MySQL flow is working.

## 7. Reuse the pipeline with a different topic

Create a different topic once. For example, `toll_demo`:

```bash
cd ~/IBMDEexercises/AirflowKafka/kafka_2.12-3.7.0  # Enter the Kafka directory
bin/kafka-topics.sh --create --topic toll_demo --bootstrap-server localhost:9092  # Create an alternate topic
```

In **both** the producer terminal and consumer terminal, set the same topic before launching the script:

```bash
export KAFKA_TOPIC=toll_demo  # Point this terminal's producer or consumer at the new topic
```

Then run the same producer and consumer commands as above. The code's default topic is `toll` when `KAFKA_TOPIC` is not set. The MySQL table remains `livetolldata`; changing the topic alone does not change the destination schema.

You can also change the event rate in the producer terminal:

```bash
export TOLL_EVENT_INTERVAL=0.5  # Generate about two events per second instead of one
```

## 8. Key concepts

- **Producer:** Creates events and sends them to Kafka. It does not write directly to MySQL.
- **Topic:** A named stream of Kafka messages, such as `toll`.
- **Broker:** The Kafka server that accepts and serves messages; here it listens on `localhost:9092`.
- **Consumer:** Reads the messages from Kafka and processes them.
- **Database transaction:** The consumer commits the MySQL insert before committing the Kafka message offset. This basic approach reduces the chance of acknowledging an event before it is stored; it is not a full exactly-once guarantee and duplicates can still occur in some failure scenarios.
- **Environment variables:** Keep deployment settings and the database password outside the source files. An exported variable applies to child processes of that terminal, so set the password in the terminal where you launch the consumer.
- **`tar -xzf`:** `-x` extracts, `-z` handles gzip compression, and `-f` provides the archive filename.

## Troubleshooting checklist

- **Producer cannot connect:** Ensure the Kafka broker is running and that the configured broker address is correct.
- **Topic missing:** List topics and create the configured topic before starting the scripts.
- **Consumer cannot authenticate to MySQL:** Verify `tolluser` has the expected password for the host used in `MYSQL_HOST`, and reset the terminal's `TOLL_MYSQL_PASSWORD` value if necessary.
- **Python import error:** Activate `~/IBMDEexercises/AirflowKafka/.venv` and install `kafka-python` and `mysql-connector-python` into that environment.
- **Consumer receives no messages:** Confirm producer and consumer use the exact same `KAFKA_TOPIC` and `KAFKA_BOOTSTRAP_SERVERS` settings.
- **Repeated messages after restart:** This basic project does not implement unique event IDs or database-side deduplication; Kafka consumer offsets and database transactions do not by themselves guarantee exactly-once insertion.

## Security note

Use a password intended for this local learning project, keep it out of source code and screenshots, and never commit credentials to GitHub. The sample traffic is synthetic and intended for learning, not production use.
