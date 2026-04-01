from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    desc,
    row_number,
    to_date
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data" / "raw"
OUTPUT_PATH = PROJECT_ROOT / "data" / "transformed"


def build_spark():
    spark = (
        SparkSession.builder
        .appName("BatchDFETL")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():

    spark = build_spark()

    print("Reading raw parquet...")

    df = spark.read.parquet(str(RAW_PATH))

    df = df.withColumn("event_date", to_date(col("event_time")))

    df.cache()

    print("Record count:", df.count())

    # --------------------------------
    # 1. events per event_type per day
    # --------------------------------

    events_by_type = (
        df.groupBy("event_date", "event_type")
        .agg(count("*").alias("event_count"))
    )

    events_by_type.write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(OUTPUT_PATH / "events_by_type_daily"))

    # --------------------------------
    # 2. top questions by score
    # --------------------------------

    window_spec = Window.partitionBy("event_date").orderBy(desc("payload.score"))

    top_questions = (
        df.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
        .select(
            "event_date",
            col("payload.question_id").alias("question_id"),
            col("payload.title").alias("title"),
            col("payload.score").alias("score"),
            col("payload.answer_count").alias("answer_count"),
        )
    )

    top_questions.write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(OUTPUT_PATH / "top_questions_daily"))

    # --------------------------------
    # 3. answered vs unanswered
    # --------------------------------

    answered_rate = (
        df.groupBy("event_date")
        .agg(
            count("*").alias("total_questions"),
            sum(when(col("payload.is_answered") == True, 1).otherwise(0)).alias("answered"),
            sum(when(col("payload.is_answered") == False, 1).otherwise(0)).alias("unanswered")
        )
    )

    answered_rate.write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(OUTPUT_PATH / "answered_rate_daily"))

    # --------------------------------
    # 4. region distribution
    # --------------------------------

    region_dist = (
        df.groupBy("event_date", "region")
        .agg(count("*").alias("event_count"))
    )

    region_dist.write.mode("overwrite") \
        .option("header", "true") \
        .csv(str(OUTPUT_PATH / "region_distribution"))

    print("Batch ETL finished successfully")

    spark.stop()


if __name__ == "__main__":
    main()