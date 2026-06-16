# EC2 Kafka Producer Setup Guide

This document explains how to configure an AWS EC2 instance to run:

- Apache Kafka Server
- Kafka KRaft mode
- Python KYC data producer

Azure Databricks will connect to Kafka as the streaming consumer.

---

## 1. Target Architecture

```text
AWS EC2
 ├── Kafka Server
 ├── Kafka Topic: kyc.customer.profile.raw.v1
 └── Python Producer
        ↓
Kafka Topic
        ↓
Azure Databricks Structured Streaming
        ↓
Bronze Delta Table
        ↓
Silver Delta Table
        ↓
Gold KYC Risk Scoring Table
```

In this demo project:

- Kafka runs on AWS EC2.
- The Python producer also runs on the same EC2 instance.
- Azure Databricks connects to the EC2 Kafka broker through the EC2 public IP.
- Kafka uses KRaft mode, so ZooKeeper is not required.

---

## 2. EC2 Requirement

Recommended EC2 instance for this demo:

| Item                      | Recommendation               |
| ------------------------- | ---------------------------- |
| OS                        | Ubuntu 22.04 or Ubuntu 24.04 |
| Instance Type             | `t3.small` or higher         |
| Storage                   | 20 GB or higher              |
| Java                      | Java 17+                     |
| Python                    | Python 3.10+                 |
| Kafka Port for Databricks | `9094`                       |
| Kafka Internal Port       | `9092`                       |
| Kafka Controller Port     | `9093`                       |

---

## 3. AWS Security Group Configuration

The EC2 security group should allow only the required traffic.

### 3.1 Inbound Rules

| Type       | Port | Source                             | Purpose                        |
| ---------- | ---: | ---------------------------------- | ------------------------------ |
| SSH        |   22 | Your local IP only                 | Connect to EC2                 |
| Custom TCP | 9094 | Databricks outbound public IP only | Allow Databricks to read Kafka |

Do not open Kafka to the entire internet unless this is only a short temporary test.

Avoid this for long-term use:

```text
0.0.0.0/0
```

Recommended:

```text
<databricks_outbound_public_ip>/32
```

### 3.2 Ports Explanation

| Port | Usage                                        | Should Be Public?   |
| ---: | -------------------------------------------- | ------------------- |
| 9092 | Internal Kafka listener for producer on EC2  | No                  |
| 9093 | Kafka KRaft controller listener              | No                  |
| 9094 | External Kafka listener for Azure Databricks | Yes, but restricted |

---

## 4. Connect to EC2

From your local machine:

```bash
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
```

Example:

```bash
ssh -i kyc-project.pem ubuntu@13.58.100.10
```

---

## 5. Install Java and Basic Tools

For Ubuntu:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk wget curl vim net-tools python3 python3-pip python3-venv
```

Check Java version:

```bash
java -version
```

Expected result should show Java 17 or higher.

If you are using Amazon Linux 2023, use:

```bash
sudo dnf update -y
sudo dnf install -y java-17-amazon-corretto wget curl vim python3 python3-pip
```

---

## 6. Download Apache Kafka

Go to the home directory:

```bash
cd ~
```

Download Kafka:

```bash
wget https://downloads.apache.org/kafka/4.3.0/kafka_2.13-4.3.0.tgz
```

Extract the file:

```bash
tar -xzf kafka_2.13-4.3.0.tgz
```

Rename the folder:

```bash
mv kafka_2.13-4.3.0 kafka
```

Go into the Kafka folder:

```bash
cd ~/kafka
```

---

## 7. Configure Kafka Server

This project uses Kafka KRaft mode, not ZooKeeper.

Kafka needs two listeners:

- `INTERNAL`: used by the Python producer running on the same EC2
- `EXTERNAL`: used by Azure Databricks

Get the EC2 public IP:

```bash
PUBLIC_IP=$(curl -s http://checkip.amazonaws.com)
echo $PUBLIC_IP
```

Back up the original Kafka config file:

```bash
cp config/server.properties config/server.properties.bak
```

Edit the Kafka server config:

```bash
vim config/server.properties
```

Find and update the following properties.

If the properties already exist, replace them.
If they do not exist, add them at the bottom of the file.

```properties
process.roles=broker,controller
node.id=1

controller.quorum.voters=1@localhost:9093

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://localhost:9093
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://<EC2_PUBLIC_IP>:9094

listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER

log.dirs=/tmp/kraft-combined-logs
auto.create.topics.enable=false
```

Replace this value:

```text
<EC2_PUBLIC_IP>
```

with your real EC2 public IP.

Example:

```properties
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://13.58.100.10:9094
```

Important:

Do not use `localhost` for the external listener.
Azure Databricks cannot connect to `localhost` on your EC2 machine.

---

## 8. Format Kafka Storage

Kafka KRaft requires storage formatting before the first startup.

Run:

```bash
cd ~/kafka
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID
```

Format Kafka storage:

```bash
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

Only run this format command once when setting up Kafka for the first time.

If you run it again after data already exists, Kafka may fail or old metadata may be overwritten.

---

## 9. Start Kafka Server

Start Kafka in the foreground:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

If Kafka starts successfully, keep this terminal open.

For a background run, use:

```bash
cd ~/kafka
nohup bin/kafka-server-start.sh config/server.properties > kafka-server.log 2>&1 &
```

Check Kafka log:

```bash
tail -f kafka-server.log
```

Check if Kafka is listening on the expected ports:

```bash
netstat -tulnp | grep 909
```

Expected ports:

```text
9092
9093
9094
```

---

## 10. Create Kafka Topic

Create the KYC customer profile topic:

```bash
cd ~/kafka

bin/kafka-topics.sh \
  --create \
  --topic kyc.customer.profile.raw.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Because this demo uses only one Kafka broker, the replication factor should be `1`.

Describe the topic:

```bash
bin/kafka-topics.sh \
  --describe \
  --topic kyc.customer.profile.raw.v1 \
  --bootstrap-server localhost:9092
```

Expected result:

```text
Topic: kyc.customer.profile.raw.v1
PartitionCount: 3
ReplicationFactor: 1
```

---

## 11. Test Kafka Locally on EC2

### 11.1 Start a Console Consumer

Open a new EC2 terminal and run:

```bash
cd ~/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic kyc.customer.profile.raw.v1 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

### 11.2 Send a Test Message

Open another EC2 terminal and run:

```bash
cd ~/kafka

bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic kyc.customer.profile.raw.v1 \
  --property parse.key=true \
  --property key.separator=":"
```

Paste this test message:

```json
USR100001:{"event_id":"evt_test_001","event_type":"CUSTOMER_PROFILE_CREATED","event_time":"2026-06-15T14:30:00Z","source_system":"TD_BANK","schema_version":"1.0","payload":{"user_id":"USR100001","user_job":"Software Engineer","risk_flag":"N","pep_flag":"N","user_country":"CANADA"}}
```

The consumer should receive:

```text
USR100001 | {"event_id":"evt_test_001", ...}
```

---

## 12. Configure Python Producer on EC2

Create project folder:

```bash
mkdir -p ~/kyc-streaming-pipeline/producer
cd ~/kyc-streaming-pipeline
```

Create Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install Python packages:

```bash
pip install kafka-python faker
```

Create a producer file:

```bash
vim producer/customer_producer.py
```

Example producer code:

```python
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

TOPIC_NAME = "kyc.customer.profile.raw.v1"
BOOTSTRAP_SERVERS = "localhost:9092"

USER_JOBS = [
    "Software Engineer",
    "Data Analyst",
    "Business Owner",
    "Doctor",
    "Lawyer",
    "Real Estate Agent",
    "Accountant",
    "Student",
    "Consultant",
    "Cashier"
]

USER_COUNTRIES = [
    "CANADA",
    "UNITED_STATES",
    "CHINA",
    "INDIA",
    "UNITED_KINGDOM",
    "MEXICO",
    "BRAZIL",
    "IRAN",
    "RUSSIA",
    "UNITED_ARAB_EMIRATES"
]

SOURCE_SYSTEMS = [
    "TD_BANK",
    "RBC",
    "HSBC",
    "WEALTHSIMPLE",
    "AMERICAN_BANK"
]

ACCOUNT_CHANNELS = [
    "ONLINE",
    "BRANCH"
]

ACCOUNT_TYPES = [
    "CHECKING",
    "SAVINGS",
    "CREDIT_CARD",
    "INVESTMENT",
    "MORTGAGE",
    "LOAN",
    "BUSINESS_ACCOUNT"
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_customer_event():
    user_id = f"USR{random.randint(100000, 999999)}"

    account_types = random.sample(
        ACCOUNT_TYPES,
        random.randint(1, 3)
    )

    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "CUSTOMER_PROFILE_CREATED",
        "event_time": utc_now(),
        "source_system": random.choice(SOURCE_SYSTEMS),
        "schema_version": "1.0",
        "payload": {
            "user_id": user_id,
            "user_address": fake.address().replace("\n", ", "),
            "user_job": random.choice(USER_JOBS),
            "user_account_types": account_types,
            "user_jurisdiction": random.choice(["CA-ON", "CA-BC", "US-NY", "US-CA", "GB-LDN"]),
            "risk_flag": random.choice(["Y", "N", "N", "N"]),
            "pep_flag": random.choice(["Y", "N", "N", "N"]),
            "user_country": random.choice(USER_COUNTRIES),
            "user_account_channel": random.choice(ACCOUNT_CHANNELS),
            "user_last_review_time": fake.date_time_between(
                start_date="-3y",
                end_date="now",
                tzinfo=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_accounts": [
                {
                    "account_id": f"ACC{random.randint(100000, 999999)}",
                    "account_type": account_type,
                    "account_status": "ACTIVE",
                    "currency": random.choice(["CAD", "USD", "GBP"]),
                    "open_date": str(fake.date_between(start_date="-5y", end_date="today"))
                }
                for account_type in account_types
            ]
        }
    }

    return user_id, event


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3
    )

    print(f"Producing messages to topic: {TOPIC_NAME}")

    while True:
        key, event = generate_customer_event()

        producer.send(
            TOPIC_NAME,
            key=key,
            value=event
        )

        producer.flush()

        print(f"Sent event for user_id={key}")

        time.sleep(2)


if __name__ == "__main__":
    main()
```

Run the producer:

```bash
cd ~/kyc-streaming-pipeline
source .venv/bin/activate
python producer/customer_producer.py
```

Run producer in the background:

```bash
cd ~/kyc-streaming-pipeline
source .venv/bin/activate
nohup python producer/customer_producer.py > producer.log 2>&1 &
```

Check producer logs:

```bash
tail -f producer.log
```

---

## 13. Test Data in Kafka

Use Kafka console consumer to confirm data is being sent:

```bash
cd ~/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic kyc.customer.profile.raw.v1 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

You should see customer events like:

```text
USR123456 | {"event_id": "evt_xxx", "event_type": "CUSTOMER_PROFILE_CREATED", ...}
```

---

## 14. Find Azure Databricks Outbound Public IP

To allow Azure Databricks to connect to Kafka on EC2, you need the outbound public IP used by the Databricks cluster.

In an Azure Databricks notebook, run:

```python
import urllib.request

public_ip = urllib.request.urlopen("https://api.ipify.org").read().decode("utf-8")
print(public_ip)
```

Add this IP to the AWS EC2 security group inbound rule:

```text
Type: Custom TCP
Port: 9094
Source: <DATABRICKS_PUBLIC_IP>/32
```

If the Databricks cluster restarts and the IP changes, update the AWS security group again.

For a stable production-like setup, configure Azure Databricks with a stable outbound public IP using Azure NAT Gateway.

---

## 15. Test Network Connection from Databricks to EC2 Kafka

In Azure Databricks notebook, test the TCP connection:

```python
import socket

ec2_public_ip = "<EC2_PUBLIC_IP>"
port = 9094

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(10)

try:
    sock.connect((ec2_public_ip, port))
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    sock.close()
```

If the connection fails, check:

- EC2 security group inbound rule
- EC2 public IP
- Kafka external listener port `9094`
- Kafka server is running
- Databricks outbound public IP
- Network ACL rules

---

## 16. Read Kafka Data from Azure Databricks

Create a Databricks notebook.

Set Kafka connection variables:

```python
kafka_bootstrap_servers = "<EC2_PUBLIC_IP>:9094"
topic_name = "kyc.customer.profile.raw.v1"
```

Read from Kafka:

```python
raw_kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .load()
)
```

Convert Kafka key and value to strings:

```python
from pyspark.sql.functions import col, current_timestamp

bronze_df = (
    raw_kafka_df
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
            current_timestamp().alias("ingestion_time")
        )
)
```

Write to a Bronze Delta table:

```python
(
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/bronze_kyc_customer_profile_raw")
        .toTable("bronze_kyc_customer_profile_raw")
)
```

Check the Bronze table:

```sql
SELECT *
FROM bronze_kyc_customer_profile_raw
ORDER BY ingestion_time DESC;
```

---

## 17. Parse JSON in Databricks

After writing raw Kafka messages into Bronze, parse the JSON payload for the Silver layer.

Example schema:

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType
)

account_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("open_date", StringType(), True)
])

payload_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_address", StringType(), True),
    StructField("user_job", StringType(), True),
    StructField("user_account_types", ArrayType(StringType()), True),
    StructField("user_jurisdiction", StringType(), True),
    StructField("risk_flag", StringType(), True),
    StructField("pep_flag", StringType(), True),
    StructField("user_country", StringType(), True),
    StructField("user_account_channel", StringType(), True),
    StructField("user_last_review_time", StringType(), True),
    StructField("user_accounts", ArrayType(account_schema), True)
])

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("payload", payload_schema, True)
])
```

Parse Bronze data:

```python
from pyspark.sql.functions import from_json

bronze_stream_df = (
    spark.readStream
        .table("bronze_kyc_customer_profile_raw")
)

parsed_df = (
    bronze_stream_df
        .withColumn("json_data", from_json(col("raw_value"), event_schema))
        .select(
            col("kafka_key"),
            col("json_data.event_id"),
            col("json_data.event_type"),
            col("json_data.event_time"),
            col("json_data.source_system"),
            col("json_data.schema_version"),
            col("json_data.payload.user_id"),
            col("json_data.payload.user_address"),
            col("json_data.payload.user_job"),
            col("json_data.payload.user_account_types"),
            col("json_data.payload.user_jurisdiction"),
            col("json_data.payload.risk_flag"),
            col("json_data.payload.pep_flag"),
            col("json_data.payload.user_country"),
            col("json_data.payload.user_account_channel"),
            col("json_data.payload.user_last_review_time"),
            col("json_data.payload.user_accounts"),
            col("ingestion_time")
        )
)
```

Write Silver table:

```python
(
    parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/silver_kyc_customer_profile")
        .toTable("silver_kyc_customer_profile")
)
```

---

## 18. Common Issues

### Issue 1: Databricks Cannot Connect to Kafka

Possible reasons:

- EC2 security group does not allow Databricks public IP on port `9094`
- Kafka is not running
- Kafka external listener is not configured correctly
- `advertised.listeners` still uses `localhost`
- EC2 public IP changed after instance restart

Check Kafka config:

```bash
grep advertised.listeners ~/kafka/config/server.properties
```

Expected:

```properties
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://<EC2_PUBLIC_IP>:9094
```

---

### Issue 2: Producer Works Locally but Databricks Cannot Read

This usually means Kafka is working internally, but external access is not configured correctly.

Check:

```bash
netstat -tulnp | grep 9094
```

Expected:

```text
0.0.0.0:9094
```

---

### Issue 3: Kafka Topic Does Not Exist

List topics:

```bash
cd ~/kafka

bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

Create the topic again if needed:

```bash
bin/kafka-topics.sh \
  --create \
  --topic kyc.customer.profile.raw.v1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

### Issue 4: EC2 Public IP Changed

If you stop and start your EC2 instance, the public IP may change unless you use an Elastic IP.

Recommended for this project:

- Allocate an Elastic IP in AWS
- Associate it with the EC2 instance
- Use the Elastic IP in Kafka `advertised.listeners`
- Use the Elastic IP in Databricks `kafka.bootstrap.servers`

---

## 19. Stop Services

Stop Python producer:

```bash
ps aux | grep customer_producer.py
kill <PROCESS_ID>
```

Stop Kafka:

```bash
ps aux | grep kafka
kill <PROCESS_ID>
```

Or if Kafka is running in the foreground, press:

```text
Ctrl + C
```

---

## 20. Recommended Repository Structure

```text
kyc-streaming-pipeline/
├── README.md
├── docs/
│   └── EC2_KAFKA_PRODUCER_SETUP.md
├── producer/
│   └── customer_producer.py
├── databricks/
│   ├── bronze_kafka_ingestion.py
│   ├── silver_customer_profile.py
│   └── gold_risk_scoring.py
├── config/
│   └── kafka_config_example.json
└── sample_data/
    └── sample_customer_event.json
```

---

## 21. Security Notes

This setup is for demo and learning purposes only.

For production, do not use plaintext Kafka over the public internet.

Production improvements should include:

- SASL or SSL authentication
- Private networking between cloud environments
- VPN or private link
- Kafka ACLs
- Secret management
- Stable outbound IP from Databricks
- Restricted EC2 security group rules
- Monitoring and alerting
- Kafka broker logs and retention policies
