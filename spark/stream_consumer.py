from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, to_date, coalesce, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, BooleanType
)

# The Config
load_dotenv()

raw_input_dir = os.getenv("raw_input_dir", "data/raw")
raw_output_dir = os.getenv("raw_output_dir", "data/processed/raw_parquet")
checkpoint_dir = os.getenv("checkpoint_dir", "data/checkpoints/stream_consumer")

max_files_per_trigger = int(os.getenv("max_files_per_trigger", "1"))

trigger_processing_time = os.getenv("trigger_processing_time", "10 seconds")
trigger_once = os.getenv("trigger_once", "false").lower() in ("1", "true", "yes")

def ensure_dir(path_str: str) -> None:
    Path(path_str).mkdir(parents = True, exist_ok = True)

# The Schema
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), False),

    StructField("user_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("source", StringType(), True),
    StructField("payload_version", IntegerType(), True),

    StructField("payload", StructField([
        StructField("question_id", LongType(), True),
        StructField("creation_date", LongType(), True),
        StructField("title", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("answer_count", IntegerType(), True),
        StructField("is_answered", BooleanType(), True),
        StructField("link", StringType(), True)
    ]), True)
])

def build_spark(app_name: str = "StreamConsumer") -> SparkSession:
    
    spark =  SparkSession.builder \
        .appName("StackExchange Behavior Analytics Platform") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    return spark

def transform(df: DataFrame) -> DataFrame:
    event_time = to_timestamp(col("timstamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")

    out = df.withColumn("event_time", event_time)
    out = out.withColumn("event_date", to_date(col("event_time")))

    # If timestamp parsing fails, we avoid the null partitions
    out = out.withColumn(
        "event_date",
        coalesce(col("event_date"), lit("1970-01-01"). cast("date"))
    )

    return out

def main() -> None:
    ensure_dir(raw_input_dir)
    ensure_dir(raw_output_dir)
    ensure_dir(checkpoint_dir)

    spark = build_spark()

    # Read NDJSON from partitioned folders
    input_path = os.path.join(raw_input_dir, "date=*")

    stream_df = (
        spark.readStream
        .schema(event_schema)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("pathGlobFilter", "*.ndjson")
        .json(input_path)
    )

    out_df = transform(stream_df)

    writer = (
        out_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", raw_output_dir)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("event_date")
    )

    if trigger_once:
        query = writer.trigger(once = True).start()
    else:
        query = writer.trigger(processingTime = trigger_processing_time).start()

    query.awaitTermination()

if __name__ == "__main__":
    main()