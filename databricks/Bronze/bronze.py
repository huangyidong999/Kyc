import urllib.request

public_ip = urllib.request.urlopen("https://api.ipify.org").read().decode("utf-8")
print(public_ip)

''' test the connection to the public IP address '''

import socket

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



'''create the catalog , schema and volume in databricks'''
spark.sql("CREATE CATALOG IF NOT EXISTS kyc_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS kyc_catalog.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS kyc_catalog.bronze.checkpoints")


'''Load the data from kafka and show it in the console'''
from pyspark.sql.functions import col, current_timestamp

kafka_bootstrap_servers = "3.96.154.193:9094"
topic_name = "kyc.customer.profile.raw.v1"

checkpoint_path = "/Volumes/kyc_catalog/bronze/checkpoints/bronze_kyc_customer_profile_raw_v2"

raw_kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
)

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

query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("kyc_catalog.bronze.bronze_kyc_customer_profile_raw")
)

query.awaitTermination()


display(spark.table("kyc_catalog.bronze.bronze_kyc_customer_profile_raw"))