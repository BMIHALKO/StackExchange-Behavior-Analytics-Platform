from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        "/Users/shloksingh/Desktop/TRNG2366_Project1/StackExchange-Behavior-Analytics-Platform",
    )
).resolve()

RAW_DATA_DIR = Path(os.getenv("RAW_PARQUET_DIR", PROJECT_ROOT / "data" / "raw")).resolve()
TRANSFORMED_DIR = Path(
    os.getenv("TRANSFORMED_DIR", PROJECT_ROOT / "data" / "transformed")
).resolve()

SPARK_SUBMIT_BIN = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")


def check_raw_data_exists(**context):
    run_date = context["ds"]
    raw_partition = RAW_DATA_DIR / f"event_date={run_date}"

    if not raw_partition.exists():
        raise FileNotFoundError(f"Raw data folder not found: {raw_partition}")

    print(f"Raw data found: {raw_partition}")


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


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="human_behavior_pipeline",
    description="Airflow DAG for StackExchange behavior analytics pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["airflow", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_streaming_job = BashOperator(
        task_id="run_streaming_job",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT_BIN} {PROJECT_ROOT / 'stream_consumer.py'}"
        ),
        env={
            **os.environ,
            "RAW_PARQUET_DIR": str(RAW_DATA_DIR),
            "TRIGGER_ONCE": "true",
        },
    )

    wait_for_raw_data = PythonOperator(
        task_id="wait_for_raw_data",
        python_callable=check_raw_data_exists,
    )

    run_rdd_etl = BashOperator(
        task_id="run_rdd_etl",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT_BIN} {PROJECT_ROOT / 'batch_rdd_etl.py'}"
        ),
        env={
            **os.environ,
            "RAW_PARQUET_DIR": str(RAW_DATA_DIR),
            "TRANSFORMED_DIR": str(TRANSFORMED_DIR),
        },
    )

    run_df_etl = BashOperator(
        task_id="run_df_etl",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{SPARK_SUBMIT_BIN} {PROJECT_ROOT / 'batch_df_etl.py'}"
        ),
        env={
            **os.environ,
            "RAW_PARQUET_DIR": str(RAW_DATA_DIR),
            "TRANSFORMED_DIR": str(TRANSFORMED_DIR),
        },
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    end = EmptyOperator(task_id="end")

    start >> run_streaming_job >> wait_for_raw_data >> run_rdd_etl >> run_df_etl >> validate_output_task >> end