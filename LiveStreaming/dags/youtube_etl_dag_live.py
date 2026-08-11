import os
import sys
from datetime import datetime, timedelta

# 1. Dynamically append scripts directory to Python path for Airflow parser
PROJECT_ROOT = "/home/kritix/bigdata/test-jupyter/Live_Streaming"
DAGS_DIR = os.path.join(PROJECT_ROOT, "dags")
if DAGS_DIR not in sys.path:
    sys.path.append(DAGS_DIR)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

# 2. Imports from scripts/
from scripts.gsheets_sync import sync_hdfs_to_gsheet
from scripts.producer_ingest import execute_ingestion

# Default DAG Arguments
default_args = {
    "owner": "kritix",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Define DAG
with DAG(
    dag_id="youtube_trend_analysis_etl_streaming_ds",
    default_args=default_args,
    description="End-to-End YouTube Trend Analysis using Kafka, PySpark, HDFS, and Google Sheets",
    schedule_interval=None,
    catchup=False,
    params={
        "primary_keyword": "FIFA",
        "gen_kw1": "Football",
        "gen_kw2": "Champions League",
        "gen_kw3": "World Cup",
    },
) as dag:

    # Configurations
    HDFS_EXPORTS_DIR = f"{PROJECT_ROOT}/tableau_exports"
    TEMP_CSV_FILE = f"{PROJECT_ROOT}/dags/scripts/temp_bigdata.csv"

    # Python Callable for Ingestion Step
    def run_ingestion_callable(**kwargs):
        params = kwargs["params"]
        primary_kw = params.get("primary_keyword", "").strip()
        
        # Build keywords list while filtering out empty/blank entries
        raw_keywords = [
            primary_kw,
            params.get("gen_kw1", "").strip(),
            params.get("gen_kw2", "").strip(),
            params.get("gen_kw3", "").strip(),
        ]
        keywords = [kw for kw in raw_keywords if kw]

        # Generate unique run topic name
        topic_name = (
            f"yt_topic_{kwargs['ds_nodash']}_{kwargs['ts_nodash'][-6:]}"
        )

        print(
            f"🚀 Ingesting YouTube records for topic: '{topic_name}' | Keywords: {keywords}"
        )

        execute_ingestion(
            keywords=keywords,
            primary_kw=primary_kw,
            kafka_topic=topic_name,
            csv_file=TEMP_CSV_FILE,
            max_videos_per_kw=250,  # 🎯 Harvests ~1,000 unique records across keywords
        )

        # Push generated Kafka topic name to XCom for PySpark task
        kwargs["ti"].xcom_push(key="kafka_topic", value=topic_name)

    # -------------------------------------------------------------------------
    # DAG TASKS DEFINITION
    # -------------------------------------------------------------------------

    # Task 1: Clean up old HDFS output files before running
    cleanup_hdfs_task = BashOperator(
        task_id="cleanup_hdfs_exports",
        bash_command=f"hdfs dfs -rm -r -f {HDFS_EXPORTS_DIR}/* || true",
    )

    # Task 2: Ingest YouTube Data -> Kafka
    ingest_youtube_task = PythonOperator(
        task_id="ingest_youtube_to_kafka",
        python_callable=run_ingestion_callable,
        provide_context=True,
    )

    # Task 3: PySpark ETL (Kafka -> Feature Engineering -> HDFS Output)
    pyspark_etl_task = SparkSubmitOperator(
        task_id="pyspark_trend_analysis",
        application=f"{PROJECT_ROOT}/dags/scripts/pyspark_etl.py",
        conn_id="spark_default",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",  # 👈 Updated for Scala 2.13 & Spark 4.0.0
        application_args=[
            "{{ ti.xcom_pull(task_ids='ingest_youtube_to_kafka', key='kafka_topic') }}",
            "{{ params.primary_keyword }}",
            HDFS_EXPORTS_DIR,
        ],
    )

    # Task 4: Stream refined dataset from HDFS directly into Google Sheets
    sync_to_gsheets_task = PythonOperator(
        task_id="sync_hdfs_to_gsheets",
        python_callable=sync_hdfs_to_gsheet,
    )

    # -------------------------------------------------------------------------
    # TASK DEPENDENCIES
    # -------------------------------------------------------------------------
    (
        cleanup_hdfs_task
        >> ingest_youtube_task
        >> pyspark_etl_task
        >> sync_to_gsheets_task
    )