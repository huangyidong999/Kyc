import socket
import urllib.request

from pyspark.sql.functions import col, current_timestamp

catalog_name = "kyc_catalog"
bronze_schema_name = "bronze"
bronze_table_name = "bronze_kyc_customer_profile_raw"
bronze_table = f"{catalog_name}.{bronze_schema_name}.{bronze_table_name}"
checkpoint_path = "/Volumes/kyc_catalog/bronze/checkpoints/bronze_kyc_customer_profile_raw_v3"
kafka_bootstrap_servers = "3.96.154.193:9094"
topic_name = "kyc.customer.profile.raw.v1"

try:
    public_ip = urllib.request.urlopen("https://api.ipify.org").read().decode("utf-8")
    print(f"Public IP: {public_ip}")
except Exception as e:
    print(f"Unable to resolve public IP: {e}")

ec2_public_ip = "3.96.154.193"
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

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{bronze_schema_name}.checkpoints")

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .load()
)

bronze_df = (
    raw_kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        current_timestamp().alias("ingestion_time"),
    )
)

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()

display(spark.table(bronze_table))