import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, to_date
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import min, max

fraud_input_topic = "tpc_fraud_decisions"
checkpoint_location = os.getenv(
    "SPARK_CHECKPOINT_DIR", "/streaming/.checkpoint/fraud_decisions"
)
fraud_output_topic = "tpc_alerts_aggregated"
key_space = "mykeyspace"
cass_table_name = "fraud"
parquet_output_path = "/streaming/data/parquet/fraud_decisions"

def log_row(row):
    import logging
    logging.warning(f"Row: {row}")

def debug_stream(df, name):
    return df.withColumn("type", lit(name)).writeStream \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", f"/tmp/debug_checkpoint_{name}") \
        .queryName(f"debug_{name}") \
        .start()

def debug_batch(df, epoch_id):
    import logging
    logger = logging.getLogger("streaming")
    logger.warning(f"Batch {epoch_id}")
    df.show(20, truncate=False)

def write_parquet_for_analytics(df, epoch_id, logger):
    if df.isEmpty():
        return

    analytics_df = (
        df.drop("partition", "offset")
        .withColumn("batch_id", lit(epoch_id))
        .withColumn("event_date", to_date(col("timestamp")))
    )

    analytics_df.write.mode("append").partitionBy("event_date").parquet(
        parquet_output_path
    )
    logger.warning(f"[Batch {epoch_id}] Appended batch to {parquet_output_path}")

def process_batch(df, epoch_id):
    import logging

    logger = logging.getLogger("streaming")

    # log offset range (unchanged)
    offsets = df.groupBy("partition") \
        .agg(
        min("offset").alias("min_offset"),
        max("offset").alias("max_offset")
    ) \
        .collect()

    for row in offsets:
        logger.warning(
            f"[Batch {epoch_id}] Partition {row['partition']} "
            f"offsets {row['min_offset']} -> {row['max_offset']}"
        )

    debug_batch(df, epoch_id)

    write_parquet_for_analytics(df, epoch_id, logger)

    # --- FILTER BLOCK ---
    df_block = df.filter(col("decision") == "BLOCK")

    # --- AGGREGATION ---
    fraud_count = df_block.count()

    logger.warning(f"[Batch {epoch_id}] BLOCK count = {fraud_count}")

    # --- ALERT THRESHOLD ---
    THRESHOLD = 3

    if fraud_count >= THRESHOLD:
        logger.warning(f"🚨 FRAUD SPIKE DETECTED: {fraud_count}")

        # create a single-row DataFrame for alert
        spark = df.sparkSession

        alert_data = [{
            "alert_type": "fraud_spike",
            "fraud_count": str(fraud_count),
            "batch_id": str(epoch_id)
        }]

        alert_df = spark.createDataFrame(alert_data)

        # convert to Kafka format
        alert_kafka_df = alert_df.selectExpr(
            "CAST(alert_type AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )

        # --- WRITE ALERT TO KAFKA ---
        alert_kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", fraud_output_topic) \
            .save()

    # --- KEEP ORIGINAL CASSANDRA WRITE ---
    df_block \
        .drop("partition", "offset", "min_offset", "max_offset") \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", key_space) \
        .option("table", cass_table_name) \
        .mode("append") \
        .save()

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToCassandraPipeline") \
    .config("spark.ui.host", "0.0.0.0") \
    .config("spark.driver.host", "spark-driver") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema of incoming JSON
schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("fraud_probability", StringType()) \
    .add("decision", StringType()) \
    .add("timestamp", StringType())

# Read stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", fraud_input_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100) \
    .load()
#q1 = debug_stream(kafka_df, "kafka_df")

# Convert Kafka value (binary) → JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value",
                              "topic",
                              "partition",
                              "offset",
                              "timestamp as kafka_timestamp")

#q2 = debug_stream(json_df, "json_df")

parsed_df = (json_df.select(
    from_json(col("json_value"), schema).alias("data"),
    "topic",
    "partition",
    "offset",
    "kafka_timestamp").select("data.*", "partition", "offset"))

# to convert timestamp to proper type
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

query = parsed_df.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

query.awaitTermination()

