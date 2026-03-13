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

landing_dir = os.getenv("LANDING_DIR", "data/landing")
raw_parquet_dir = os.getenv("RAW_PARQUET_DIR", "data/raw")
checkpoint_dir = os.getenv("CHECKPOINT_DIR", "data/checkpoints/stream_consumer")

max_files_per_trigger = int(os.getenv("MAX_FILES_PER_TRIGGER", "1"))
trigger_once = os.getenv("TRIGGER_ONCE", "true").lower() in ("1", "true", "yes")
trigger_processing_time = os.getenv("TRIGGER_PROCESSING_TIME", "10 seconds")

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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark

def transforms(df: DataFrame) -> DataFrame:
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
    ensure_dir(landing_dir)
    ensure_dir(raw_parquet_dir)
    ensure_dir(checkpoint_dir)

    spark = build_spark()

    input_path = os.path.join(landing_dir, "date=*")

    stream_df = (
        spark.readStream
        .schema(event_schema)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("pathGlobFilter", "*.ndjson")
        .json(input_path)
    )

    out_df = transforms(stream_df)

    writer = (
        out_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", raw_parquet_dir)
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