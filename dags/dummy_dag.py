from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer

TRANSFORMED_DIR = "opt/airflow/data/transformed"
RAW_DATA_DIR = "opt/airflow/data/raw"
kafka_topic = "stackexchange-events"

def check_kafka_topic() -> None:

    try:
        consumer = KafkaConsumer(
            bootstrap_servers = "kafka:9092",
            consumer_timeout_ms = 5000,
            auto_offset_reset = "earliest",
            enable_auto_commit = False,
        )
        partitions = consumer.partitions_for_topic(kafka_topic)

        # if not partitions:
        #     raise ValueError(
        #         f"Kafka topic '{kafka_topic}' does not exist or has not partitions."
        #     )
        # print(
        #     f"Kafka topic '{kafka_topic}' is readable with partitions: "
        #     f"{sorted(partitions)}"
        # )
    except Exception as e:
        print(f"    [ERROR] {e}")
        raise ValueError(
            f"Kafka topic '{kafka_topic}' does not exist or has not partitions."
        )
    finally:
        consumer.close()



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

def check_raw_data_exists(**context):
    run_date = context["ds"]
    raw_partition = RAW_DATA_DIR / f"event_date={run_date}"

    if not raw_partition.exists():
        raise FileNotFoundError(f"Raw data folder not found: {raw_partition}")

    print(f"Raw data found: {raw_partition}")




# Default arguments for DAG setup
default_args = {
        "owner" : "human_behavior_team",
        "start_date" : datetime(2026,3,12),
        "retries" : 3,
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
        bash_command="""spark-submit \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                            /opt/airflow/spark/stream_consumer.py \
                            --bootstrap-servers kafka:9092"""
        
    )

    # run_consumer_task = PythonOperator(
    #     task_id = "run_consumer",
    #     python_callable=call_stream
    # )

    wait_for_raw_data = PythonOperator(
        task_id = "wait_for_raw_data",
        python_callable = check_raw_data_exists,
    )

    run_rdd_etl = BashOperator(
        task_id = "run_rdd_etl",
        bash_command = """spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/airflow/spark/batch_rdd_etl.py"""
    )

    run_df_etl = BashOperator(
        task_id = "run_df_etl",
        bash_command = """spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/airflow/spark/batch_df_etl.py"""
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    end = EmptyOperator(task_id="end")

    start >> check_kafka_task >> run_streaming_job >> wait_for_raw_data >> run_rdd_etl >> run_df_etl >> validate_output_task >> end