# StackExchange Behavior Analytics Platform

## Overview

This project is a real-time and batch data engineering pipeline built around StackExchange question activity data.

The system uses Docker Compose to provision Kafka and Zookeeper, ingests question events from the StackExchange API through a Kafka producer, processes streaming events with Spark Structured Streaming, stores partitioned raw Parquet data, and executes batch analytics using both Spark RDD and Spark DataFrame APIs. Apache Airflow orchestrates the full workflow from ingestion validation through transformed output generation.

This project demonstrates:
- API-based ingestion
- Kafka event streaming
- Spark Structured Streaming
- Batch ETL with both RDD and DataFrame APIs
- Airflow DAG orchestration
- Partitioned Parquet storage

---

## Architecture

```text
StackExchange API
      ↓
Kafka Producer
      ↓
Docker Kafka Broker
      ↓
Spark Structured Streaming
      ↓
Raw Parquet Storage
      ↓
Airflow DAG
      ↓
RDD + DataFrame ETL
      ↓
Analytics Outputs
```

---

## Project Structure

```text
StackExchange-Behavior-Analytics-Platform/
├── README.md
├── airflow/
│   └── dags/
│       └── human_behavior_dag.py
├── data/
│   ├── checkpoints/
│   ├── landing/
│   ├── raw/
│   └── transformed/
├── kafka/
│   └── producer.py
├── spark/
│   ├── batch_df_etl.py
│   ├── batch_rdd_etl.py
│   └── stream_consumer.py
├── docker-compose.yaml
├── requirements.txt
└── .env
```

---

## Technologies Used
- Python
- Apache Kafka
- Apache Spark / PySpark
- Apache Airflow
- Parquet
- Docker Compose
- StackExchange API

---

## Environment Variables

Create a `.env` file in the project root with the following values:

```text
STACKEXCHANGE_API_KEY=your_api_key_here
STACKEXCHANGE_SITE=stackoverflow
STACKEXCHANGE_PAGESIZE=100
STACKEXCHANGE_MAX_PAGES=20
MAX_CYCLES=15
POLL_INTERVAL=3
RUN_FOREVER=false

LANDING_DIR=/opt/airflow/data/landing
CHECKPOINT_DIR=/opt/airflow/data/checkpoints/stream_consumer
MAX_FILES_PER_TRIGGER=1
STREAM_RUN_SECONDS=30
TRIGGER_ONCE=false

KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_TOPIC=stackexchange-events

PROJECT_ROOT=/opt/airflow
RAW_PARQUET_DIR=/opt/airflow/data/raw
TRANSFORMED_DIR=/opt/airflow/data/transformed

SPARK_HOME=/opt/spark
PYSPARK_PYTHON=python3
SPARK_SUBMIT_BIN=spark-submit

FERNET_KEY=your_fernet_key_here
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Start Kafka Infrastructure

Kafka and Zookeeper are started with Docker Compose:

```bash
docker compose up -d
```

This starts:
- Zookeeper
- Kafka broker

---

## Run the Producer

From the project root, with the virtual environment activated

```bash
python3 kafka/producer.py
```

The producer:
- pulls question data from the StackExchange API
- builds structured behavioral events
- publishes those events ot the kafka topic

---

## Run the Streaming Consumer

From the project root, again with the virtual environment activated

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/stream_consumer.py
```

The streaming consumer:
- reads events from Kafka
- parses JSON into structured column
- writes raw Parquet files partitioned by `event_date`

---

## Run Batch ETL Jobs

From the project root, also with the virtual environment activated

**RDD ETL**

```bash
spark-submit spark/batch_rdd_etl.py
```

**DataFrame ETL**

```bash
spark-submit spark/batch_df_etl.py
```

Outputs:
- `events_by_type_daily`
- `top_questions_daily`
- `answered_rate_daily`
- `region_distribution`

---

## Run the Airflow Pipeline

From the project root, with the virtual environment activated, start Airflow in your local environment and trigger the DAG:

    ```bash
    airflow dags trigger human_behavior_pipeline
    ```

The DAG performs the following steps:
1. Validate the Kafka topic
2. Run the Spark streaming consumer
3. Check that raw partitioned data exists
4. Run the RDD batch ETL job
5. Run the DataFrame batch ETL job
6. Validate transformed output folders

---

## Output Datasets

A successful pipeline run produces the following transformed outputs:

```text
data/transformed/
├── answered_rate_daily
├── events_by_type_daily
├── question_score_totals_rdd
├── region_distribution
└── top_questions_daily
```

---

## Current Status

The pipeline is currently validated in a local environment with:
- Kafka and Zookeeper running through Docker Compose
- Spark running locally
- Airflow running locally
- end-to-end DAG execution succeeding

---

## Docker Support

This project currently uses Docker Compose for Kafka infrastructure

Spark and Airflow are currently run in the local environment rather than fully containerized

---

## Future Improvements
- Full containerization of Spark and Airflow
- cloud storage integration
- dashboard / reporting layer
- expanded analytics on behavioral trends