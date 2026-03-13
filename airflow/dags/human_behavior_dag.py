from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="human_behavior_pipeline",
    default_args=default_args,
    description="End-to-end behavioral analytics pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:


    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="""
        cd ~/Desktop/TRNG2366_Project1/StackExchange-Behavior-Analytics-Platform &&
        source .venv/bin/activate &&
        python producer.py
        """,
    )


    run_stream_consumer = BashOperator(
        task_id="run_stream_consumer",
        bash_command="""
        cd ~/Desktop/TRNG2366_Project1/StackExchange-Behavior-Analytics-Platform &&
        source .venv/bin/activate &&
        python spark/stream_consumer.py
        """,
    )


    run_batch_etl = BashOperator(
        task_id="run_batch_etl",
        bash_command="""
        cd ~/Desktop/TRNG2366_Project1/StackExchange-Behavior-Analytics-Platform &&
        source .venv/bin/activate &&
        python spark/batch_df_etl.py
        """,
    )


    run_producer >> run_stream_consumer >> run_batch_etl