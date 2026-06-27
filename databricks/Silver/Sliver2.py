from pyspark.sql.functions import col, from_json, when, trim, upper, row_number
from pyspark.sql.window import Window
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

bronze_df = spark.table("kyc_catalog.bronze.bronze_kyc_customer_profile_raw")

parsed_df = (
    bronze_df
        .withColumn("json_data", from_json(col("raw_value"), event_schema))
        .select(
            col("kafka_key"),
            col("json_data.event_id").alias("event_id"),
            col("json_data.event_type").alias("event_type"),
            col("json_data.event_time").alias("event_time"),
            col("json_data.source_system").alias("source_system"),
            col("json_data.schema_version").alias("schema_version"),
            col("json_data.payload.user_id").alias("user_id"),
            col("json_data.payload.user_address").alias("user_address"),
            col("json_data.payload.user_job").alias("user_job"),
            col("json_data.payload.user_account_types").alias("user_account_types"),
            col("json_data.payload.user_jurisdiction").alias("user_jurisdiction"),
            col("json_data.payload.risk_flag").alias("risk_flag"),
            col("json_data.payload.pep_flag").alias("pep_flag"),
            col("json_data.payload.user_country").alias("user_country"),
            col("json_data.payload.user_account_channel").alias("user_account_channel"),
            col("json_data.payload.user_last_review_time").alias("user_last_review_time"),
            col("json_data.payload.user_accounts").alias("user_accounts"),
            col("ingestion_time")
        )
)

silver_df = parsed_df.withColumn(
    "pep_role",
    when(trim(upper(col("pep_flag"))) == "Y", "officer")
    .when(trim(upper(col("pep_flag"))) == "N", "customer")
    .otherwise(col("pep_flag"))
)

user_window = Window.partitionBy("user_id").orderBy(col("event_time").desc(), col("ingestion_time").desc())

silver_deduped = (
    silver_df
        .withColumn("row_num", row_number().over(user_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
)

silver_deduped.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("kyc_catalog.bronze.silver_kyc_customer_profile")

# 查看覆盖后的 silver 表结果
display(spark.table("kyc_catalog.bronze.silver_kyc_customer_profile"))
