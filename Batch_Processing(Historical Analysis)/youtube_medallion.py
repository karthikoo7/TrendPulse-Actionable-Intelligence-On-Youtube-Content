"""
youtube_medallion_dag.py
─────────────────────────────────────────────────────────────────
SINGLE DAG, all layers as tasks in one linear chain:

    load_data  >>  bronze  >>  silver  >>  gold  >> upload_gsheet >> notify_success

Each task runs its own standalone script (etl_bronze.py / etl_silver.py
/ etl_gold.py) via spark-submit, so each still gets its own row in the
Airflow UI's Graph view, its own logs, and its own per-task retries --
you just don't get separate DAG runs/schedules per layer like a
3-DAG/Dataset split would. One DAG, chained tasks: load_data >> bronze
>> silver >> gold >> upload_gsheet >> notify_success.

Paths below are wired to your actual environment:
    AIRFLOW_HOME   = /home/jatin/bigdata/airflow
    dags_folder    = /home/jatin/bigdata/airflow/dags
    scripts/data   = /home/jatin/bigdata/test-jupyter
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── PATHS / ENVIRONMENT — adjust these for your environment ─────────────
SPARK_SUBMIT_BIN   = "/home/jatin/bigdata/spark/bin/spark-submit"
SCRIPTS_DIR        = "/home/jatin/bigdata/test-jupyter"

BRONZE_SCRIPT_PATH = f"{SCRIPTS_DIR}/etl_bronze.py"
SILVER_SCRIPT_PATH = f"{SCRIPTS_DIR}/etl_silver.py"
GOLD_SCRIPT_PATH   = f"{SCRIPTS_DIR}/etl_gold.py"
GSHEET_SCRIPT_PATH = f"{SCRIPTS_DIR}/upload_to_gsheet.py"   # new

GSHEET_ID          = "1cM5hplglo1bJ0w4NCVz9LyH7bcnNQZsQ1v84amJZDes"                  # new
GSHEET_WORKSHEET   = "gold_data"                             # new
GSHEET_CREDS_PATH  = "/home/jatin/bigdata/test-jupyter/trendpulse-etl-2bb5d9bda817.json"  # new

INPUT_CSV_PATH     = f"{SCRIPTS_DIR}/youtube_social_data_enriched.csv"
OUTPUT_CSV_PATH    = f"{SCRIPTS_DIR}/youtube_analysis_final.csv"
DELTA_PACKAGE      = "io.delta:delta-spark_2.13:4.0.0"

SPARK_SUBMIT_COMMON = (
    f'{SPARK_SUBMIT_BIN} --master "local[*]" --driver-memory 4g '
    f'--packages {DELTA_PACKAGE} '
    f'--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
    f'--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
)

MIN_VIEWS = 100
# No KEYWORD_FILTER -- gold intentionally processes every keyword (see etl_gold.py).


# ── ALERTING ──────────────────────────────────────────────────────────────
def notify_failure(context):
    """Central failure callback wired into default_args so EVERY task in
    this DAG reports failures the same way. Swap the body for a real
    Slack/PagerDuty/email call."""
    ti = context["task_instance"]
    print(
        f"[ALERT] Task failed: dag={ti.dag_id} task={ti.task_id} "
        f"execution_date={context['execution_date']} "
        f"try={ti.try_number}/{ti.max_tries + 1} log_url={ti.log_url}"
    )
    # from airflow.providers.slack.notifications.slack import send_slack_notification
    # send_slack_notification(
    #     slack_conn_id="slack_default",
    #     text=f":red_circle: *{ti.dag_id}* failed on *{ti.task_id}*",
    # )(context)


def notify_success(**context):
    ti = context["ti"]
    print(f"[INFO] DAG run succeeded: {ti.dag_id} run_id={context['run_id']}")


# ── DEFAULT ARGS ──────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["jatin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id="youtube_medallion_etl",
    description="load_data -> bronze -> silver -> gold -> notify_success (Spark + Delta Lake)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,          # writes to shared Delta tables -- concurrent runs would race
    tags=["etl", "youtube", "spark", "delta-lake", "medallion"],
    doc_md=__doc__,
) as dag:

    # ── LOAD_DATA: wait for the source file to actually exist ───────────
    # Plain bash wait-loop instead of FileSensor -- FileSensor requires an
    # Airflow Connection named fs_conn_id (default "fs_default"), which a
    # fresh/reset Airflow metadata DB doesn't always seed automatically
    # (this is exactly what caused "AirflowNotFoundException: The conn_id
    # `fs_default` isn't defined"). This version needs zero Connections.
    load_data = BashOperator(
        task_id="load_data",
        bash_command=(
            f'timeout=1800; elapsed=0; '
            f'while [ ! -f "{INPUT_CSV_PATH}" ]; do '
            f'  if [ $elapsed -ge $timeout ]; then '
            f'    echo "Timed out after ${{timeout}}s waiting for {INPUT_CSV_PATH}" >&2; '
            f'    exit 1; '
            f'  fi; '
            f'  echo "[load_data] Waiting for {INPUT_CSV_PATH} (${{elapsed}}s elapsed)..."; '
            f'  sleep 60; '
            f'  elapsed=$((elapsed+60)); '
            f'done; '
            f'echo "[load_data] Found {INPUT_CSV_PATH}."'
        ),
        doc_md="Waits (bash poll, 60s interval, 30 min timeout) for the raw "
               "CSV to land before starting the pipeline. Uses no Airflow "
               "Connection -- avoids the fs_default conn_id error.",
    )

    # ── BRONZE ────────────────────────────────────────────────────────
    bronze = BashOperator(
        task_id="bronze",
        bash_command=(
            f'{SPARK_SUBMIT_COMMON} '
            f'{BRONZE_SCRIPT_PATH} '
            f'--input {INPUT_CSV_PATH}'
        ),
        doc_md="Bronze layer: raw CSV -> youtube_videos_bronze (append-only, no cleaning).",
    )

    # ── SILVER ────────────────────────────────────────────────────────
    silver = BashOperator(
        task_id="silver",
        bash_command=(
            f'{SPARK_SUBMIT_COMMON} '
            f'{SILVER_SCRIPT_PATH} --drop-legacy-constraints'
        ),
        doc_md="Silver layer: clean/conform bronze -> youtube_videos_clean. "
               "`--drop-legacy-constraints` clears the hardcoded keyword CHECK "
               "constraint that causes DELTA_VIOLATE_CONSTRAINT_WITH_VALUES.",
    )

    # ── GOLD ──────────────────────────────────────────────────────────
    gold = BashOperator(
        task_id="gold",
        bash_command=(
            f'{SPARK_SUBMIT_COMMON} '
            f'{GOLD_SCRIPT_PATH} --drop-legacy-constraints '
            f'--output {OUTPUT_CSV_PATH} '
            f'--min-views {MIN_VIEWS}'
        ),
        doc_md="Gold layer: aggregate youtube_videos_clean -> youtube_keyword_summary "
               "(every keyword, no --keyword filter) + append history + export final CSV.",
    )

    # ── UPLOAD TO GOOGLE SHEETS ───────────────────────────────────────
    upload_gsheet = BashOperator(
        task_id="upload_gsheet",
        bash_command=(
            f'/home/jatin/bigdata/airflow_env/bin/python3 {GSHEET_SCRIPT_PATH} '
            f'--csv-dir {OUTPUT_CSV_PATH} '
            f'--sheet-id {GSHEET_ID} '
            f'--worksheet {GSHEET_WORKSHEET} '
            f'--creds {GSHEET_CREDS_PATH}'
        ),
        doc_md="Pushes the Gold-layer CSV export into a Google Sheet so "
               "Tableau Public can auto-refresh from it via the Google "
               "Drive connector (~24h refresh cycle).",
    )

    # ── NOTIFY_SUCCESS ────────────────────────────────────────────────
    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    # ── DEPENDENCIES ────────────────────────────────────────────────────
    load_data >> bronze >> silver >> gold >> upload_gsheet >> notify_success_task