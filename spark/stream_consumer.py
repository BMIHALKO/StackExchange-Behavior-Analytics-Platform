from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, coalesce, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)

load_dotenv()

# kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# kafka_topic = os.getenv("KAFKA_TOPIC", "stackexchange-events")
# raw_parquet_dir = os.getenv("RAW_PARQUET_DIR", "data/raw")
# checkpoint_dir = os.getenv("CHECKPOINT_DIR", "data/checkpoints/stream_consumer")
# trigger_once = os.getenv("TRIGGER_ONCE", "true").lower() in ("1", "true", "yes")
# trigger_processing_time = os.getenv("TRIGGER_PROCESSING_TIME", "10 seconds")
# stream_run_seconds = int(os.getenv("STREAM_RUN_SECONDS", "300"))

kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "stackexchange-events"
raw_parquet_dir = "opt/airflow/data/raw"
checkpoint_dir = "data/checkpoints/stream_consumer"
trigger_once = os.getenv("TRIGGER_ONCE", "true").lower() in ("1", "true", "yes")
trigger_processing_time = os.getenv("TRIGGER_PROCESSING_TIME", "10 seconds")
stream_run_seconds = int(os.getenv("STREAM_RUN_SECONDS", "300"))


event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("source", StringType(), True),
    StructField("payload_version", IntegerType(), True),
    StructField("payload", StructType([
        StructField("question_id", LongType(), True),
        StructField("creation_date", LongType(), True),
        StructField("title", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("answer_count", IntegerType(), True),
        StructField("is_answered", BooleanType(), True),
        StructField("link", StringType(), True),
    ]), True),
])


def ensure_dir(path_str: str) -> None:
    Path(path_str).mkdir(parents = True, exist_ok = True)


def build_spark(app_name: str = "StreamConsumer") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform(df: DataFrame) -> DataFrame:
    out = df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")
    )

    out = out.withColumn("event_date", to_date(col("event_time")))

    out = out.withColumn(
        "event_date",
        coalesce(col("event_date"), lit("1970-01-01").cast("date"))
    )

    return out


def main() -> None:
    spark = build_spark()
    try:
        ensure_dir(raw_parquet_dir)
        ensure_dir(checkpoint_dir)

        

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed_df = (
            kafka_df
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), event_schema).alias("data"))
            .select("data.*")
        )

        out_df = transform(parsed_df)

        writer = (
            out_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", raw_parquet_dir)
            .option("checkpointLocation", checkpoint_dir)
            .partitionBy("event_date")
        )

        if trigger_once:
            print("Starting consumer in trigger-once mode...")
            query = writer.trigger(once=True).start()
            query.awaitTermination()
        else:
            print(
                f"Starting consumer in processing-time mode every "
                f"{trigger_processing_time} for up to {stream_run_seconds} seconds..."
            )
            query = writer.trigger(processingTime = trigger_processing_time).start()
            query.awaitTermination(stream_run_seconds)

            if query.isActive:
                print("Stopping streaming query after timeout...")
                query.stop()
    except Exception as e:
        print(f"    [ERROR] {e}")

    spark.stop()


if __name__ == "__main__":
    main()