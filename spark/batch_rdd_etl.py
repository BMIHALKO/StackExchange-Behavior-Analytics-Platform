# spark/batch_rdd_etl.py
import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession


RAW_PATH = os.getenv("RAW_PARQUET_DIR", "data/raw")
TRANSFORMED_DIR = os.getenv("TRANSFORMED_DIR", "data/transformed")
OUTPUT_PATH = os.path.join(TRANSFORMED_DIR, "question_score_totals_rdd")


def build_spark():
    spark = (
        SparkSession.builder
        .appName("BatchRDDETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = build_spark()
    sc = spark.sparkContext

    print("Reading raw parquet...")
    df = spark.read.parquet(RAW_PATH)

    malformed_rows = sc.accumulator(0)

    def to_pair(row):
        payload = row["payload"]
        if payload is None:
            malformed_rows.add(1)
            return None

        question_id = payload["question_id"]
        score = payload["score"]

        if question_id is None:
            malformed_rows.add(1)
            return None

        if score is None:
            score = 0

        return (question_id, score)

    rdd = df.rdd.map(to_pair).filter(lambda x: x is not None)

    question_score_totals = (
        rdd.reduceByKey(lambda a, b: a + b)
           .sortBy(lambda x: x[1], ascending=False)
    )

    print("Top 20 question score totals:")
    for row in question_score_totals.take(20):
        print(row)
    
    output_path_obj = Path(OUTPUT_PATH)
    if output_path_obj.exists():
        shutil.rmtree(output_path_obj)

    question_score_totals.saveAsTextFile(OUTPUT_PATH)

    print(f"Malformed/null rows skipped: {malformed_rows.value}")
    print(f"Saved RDD output to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()