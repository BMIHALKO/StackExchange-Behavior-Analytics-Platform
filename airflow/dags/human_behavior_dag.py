from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.docker.operators.docker import DockerOperator

load_dotenv()

project_root = os.getenv("PROJECT_ROOT")
raw_parquet_dir = os.getenv("RAW_PARQUET_DIR")
transformed_dir = os.getenv("TRANSFORMED_DIR")

if not project_root:
    raise ValueError("PROJECT_ROOT not set in .env")
if not raw_parquet_dir:
    raise ValueError("RAW_PARQUET_DIR not set in .env")
if not transformed_dir:
    raise ValueError("TRANSFORMED_DIR not set in .env")

PROJECT_ROOT = Path(project_root).resolve()
RAW_DATA_DIR = Path(raw_parquet_dir).resolve()
TRANSFORMED_DIR = Path(transformed_dir).resolve()
SPARK_SUBMIT_BIN = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "stackexchange-events")

JAVA_HOME = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
PYSPARK_PYTHON = os.getenv("PYSPARK_PYTHON", "python3")

BASE_TASK_ENV = {
    "JAVA_HOME": JAVA_HOME,
    "SPARK_HOME": SPARK_HOME,
    "PYSPARK_PYTHON": PYSPARK_PYTHON,
    "PROJECT_ROOT": str(PROJECT_ROOT),
    "RAW_PARQUET_DIR": str(RAW_DATA_DIR),
    "TRANSFORMED_DIR": str(TRANSFORMED_DIR),
    "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
    "KAFKA_TOPIC": kafka_topic,
    "PATH": f"{SPARK_HOME}/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
}


def check_kafka_topic() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers = kafka_bootstrap_servers,
        consumer_timeout_ms = 5000,
        auto_offset_reset = "earliest",
        enable_auto_commit = False,
    )

    try:
        partitions = consumer.partitions_for_topic(kafka_topic)

        if not partitions:
            raise ValueError(
                f"Kafka topic '{kafka_topic}' does not exist or has not partitions."
            )
        print(
            f"Kafka topic '{kafka_topic}' is readable with partitions: "
            f"{sorted(partitions)}"
        )
    finally:
        consumer.close()

def check_raw_data_exists(**context):
    partitions = sorted(RAW_DATA_DIR.glob("event_date=*"))

    if not partitions:
        raise FileNotFoundError(f"No raw event_date partitions found in: {RAW_DATA_DIR}")

    print("Raw data partitions found:")
    for p in partitions[-5:]:
        print(f" - {p}")


def validate_output():
    required_paths = [
        TRANSFORMED_DIR / "question_score_totals_rdd",
        TRANSFORMED_DIR / "events_by_type_daily",
        TRANSFORMED_DIR / "top_questions_daily",
        TRANSFORMED_DIR / "answered_rate_daily",
        TRANSFORMED_DIR / "region_distribution",
    ]

    missing = [str(path) for path in required_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Expected transformed output paths were not created: " + ", ".join(missing)
        )

    print("Output validation passed.")

def send_records_to_snowflake(**context):
    # Instantiate the hook with the connection ID defined in the Airflow UI
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    bronze_check="CREATE SCHEMA IF NOT EXISTS BRONZE;"
    staging_query = f"PUT 'file://{RAW_DATA_DIR}/*/*.parquet' @BRONZE.RAW_EVENT_STAGE AUTO_COMPRESS=TRUE;"
    
    # print(f"Uploading files from {RAW_DATA_DIR} to {"RAW_EVENT_STAGE"}...")
    hook.run([bronze_check, staging_query], autocommit=True)

def send_to_table():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    query = """
        COPY INTO BRONZE.RAW_EVENT_TABLE
        FROM @BRONZE.RAW_EVENT_STAGE
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE;
        """
    hook.run(query, autocommit=True)

    dedupe_query = """
        INSERT OVERWRITE INTO BRONZE.RAW_EVENT_TABLE
        SELECT DISTINCT *
        FROM BRONZE.RAW_EVENT_TABLE;
        """
    hook.run(dedupe_query, autocommit=True)

    print("Success")

#This function might be used for sending data from the bronze layer to the silver layer
def move_to_silver():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    silver_check="CREATE SCHEMA IF NOT EXISTS SILVER;"

    query = """
    CREATE OR REPLACE TABLE SILVER.RAW_EVENT_TABLE AS
    SELECT
        EVENT_ID,
        EVENT_TYPE,
        TIMESTAMP AS TIME_POSTED,
        USER_ID,
        SOURCE AS API_USED,
        PAYLOAD:question_id::VARCHAR AS QUESTION_ID,
        PAYLOAD:creation_date::VARCHAR AS CREATION_DATE,
        PAYLOAD:title::VARCHAR AS TITLE,
        PAYLOAD:score::INT AS SCORE,
        PAYLOAD:answer_count::INT AS ANSWER_COUNT,
        PAYLOAD:is_answered::BOOLEAN AS IS_ANSWERED,
        PAYLOAD:link::VARCHAR AS LINK
    FROM BRONZE.RAW_EVENT_TABLE;
    """
    



    hook.run([silver_check, query], autocommit=True)
    print("Success")

def data_cleansing():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    clean_table_creation = """CREATE OR REPLACE TABLE SILVER.CLEANED_SILVER_TABLE AS
                                WITH unique_questions AS (
                                    SELECT 
                                        *,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY QUESTION_ID
                                            ORDER BY DATE(TIME_POSTED) DESC
                                        ) as row_num
                                    FROM SILVER.RAW_EVENT_TABLE
                                )
                                SELECT * EXCLUDE row_num
                                FROM unique_questions
                                WHERE row_num = 1"""
    hook.run(clean_table_creation, autocommit=True)


def move_to_gold():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    gold_check="CREATE SCHEMA IF NOT EXISTS GOLD;"

    answered_vs_unanswered_query = """CREATE OR REPLACE TABLE GOLD.ANSWER_RATE_DAILY AS
                                        SELECT 
                                            DATE(TIME_POSTED) AS POST_DATE, 
                                            COUNT(QUESTION_ID) AS TOTAL_QUESTIONS,
                                            COUNT(CASE WHEN IS_ANSWERED = TRUE THEN 1 END) AS QUESTIONS_ANSWERED,
                                            COUNT(CASE WHEN IS_ANSWERED = FALSE THEN 1 END) AS QUESTIONS_NOT_ANSWERED
                                        FROM SILVER.CLEANED_SILVER_TABLE
                                        GROUP BY DATE(TIME_POSTED);"""
    
    returning_user_vs_onetime_user = """CREATE OR REPLACE TABLE GOLD.RETURNING_VS_NEW_USER AS
                                        WITH user_counts AS (
                                            SELECT 
                                                USER_ID,
                                                -- Count how many times each specific user appears
                                                CASE 
                                                    WHEN COUNT(*) > 1 THEN 'Returning_user'
                                                    ELSE 'One_time_user'
                                                END AS IS_REPEAT_USER
                                            FROM SILVER.CLEANED_SILVER_TABLE
                                            GROUP BY USER_ID  -- This is the missing piece
                                        )
                                        SELECT 
                                            IS_REPEAT_USER, 
                                            COUNT(USER_ID) AS USER_COUNT
                                        FROM user_counts
                                        GROUP BY IS_REPEAT_USER;"""

    hook.run([gold_check, answered_vs_unanswered_query, returning_user_vs_onetime_user], autocommit=True)



default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id = "human_behavior_pipeline",
    description = "Airflow DAG for StackExchange behavior analytics pipeline",
    default_args = default_args,
    start_date = datetime(2025, 1, 1),
    schedule = "@daily",
    catchup = False,
    tags=["airflow", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_kafka_task = PythonOperator(
        task_id = "check_kafka_topic",
        python_callable = check_kafka_topic
    )

    run_streaming_job = BashOperator(
        task_id="run_streaming_job",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT_BIN} "
            f"--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            f"{PROJECT_ROOT / 'spark' / 'stream_consumer.py'}"
        ),
        env={
            **BASE_TASK_ENV,
            "CHECKPOINT_DIR": str(PROJECT_ROOT / "data" / "checkpoints" / "stream_consumer"),
            "TRIGGER_ONCE": "false",
            "TRIGGER_PROCESSING_TIME": "10 seconds",
            "STREAM_RUN_SECONDS": "30",
            "MAX_FILES_PER_TRIGGER": "1",
        },
    )

    wait_for_raw_data = PythonOperator(
        task_id = "wait_for_raw_data",
        python_callable = check_raw_data_exists,
    )

    # run_rdd_etl = BashOperator(
    #     task_id="run_rdd_etl",
    #     bash_command=(
    #         f"cd {PROJECT_ROOT} && "
    #         f"{SPARK_SUBMIT_BIN} {PROJECT_ROOT / 'spark' / 'batch_rdd_etl.py'}"
    #     ),
    #     env={
    #         **BASE_TASK_ENV,
    #     },
    # )

    # run_df_etl = BashOperator(
    #     task_id="run_df_etl",
    #     bash_command=(
    #         f"cd {PROJECT_ROOT} && "
    #         f"{SPARK_SUBMIT_BIN} {PROJECT_ROOT / 'spark' / 'batch_df_etl.py'}"
    #     ),
    #     env={
    #         **BASE_TASK_ENV,
    #     },
    # )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    send_to_snowflake = PythonOperator(
        task_id="send_data_to_snowflake",
        python_callable=send_records_to_snowflake
    )

    stage_to_table = PythonOperator(
        task_id="stage_to_table",
        python_callable=send_to_table
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=move_to_silver
    )

    silver_cleansing = PythonOperator(
        task_id="silver_cleansing",
        python_callable=data_cleansing
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=move_to_gold
    )


    end = EmptyOperator(task_id="end")

    # start >> check_kafka_task >> run_streaming_job >> wait_for_raw_data >> run_rdd_etl >> run_df_etl >> validate_output_task >> end

    start >> check_kafka_task >> run_streaming_job >> wait_for_raw_data >> send_to_snowflake >> stage_to_table >> bronze_to_silver >> silver_cleansing >> validate_output_task >> silver_to_gold >> end

    # start >> bronze_to_silver >> end

