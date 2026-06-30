from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from chinook_spark_common import SPARK_CONF, SPARK_PACKAGES
from chinook_callbacks import get_default_args, on_dag_success, on_dag_failure


with DAG(
    dag_id="chinook_album_to_delta_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["chinook", "album", "sqlite", "csv", "delta", "bronze", "etl", "dq"],
    default_args=get_default_args(),
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    start = EmptyOperator(task_id="start")


    simulate_retry_once_for_rca = BashOperator(
        task_id="simulate_retry_once_for_rca",
        bash_command=(
            "set -e; "
            "MARKER=/tmp/rca_retry_once_{{ dag.dag_id }}_{{ run_id | replace(':', '_') }}_{{ task.task_id }}; "
            "echo '[RCA_TEST] retry-once task marker='${MARKER}; "
            "if [ ! -f ${MARKER} ]; then "
            "  echo '[RCA_TEST_ERROR] intentional first-attempt failure for RCA retry collection'; "
            "  echo 'Traceback (most recent call last): simulated retry'; "
            "  touch ${MARKER}; "
            "  exit 1; "
            "fi; "
            "echo '[RCA_TEST_OK] retry-once task succeeded after retry'"
        ),
        retries=1,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=2),
    )

    export_sqlite_to_csv = BashOperator(
        task_id="export_sqlite_to_csv",
        bash_command=(
            "python /opt/airflow/dags/scripts/export_chinook_table.py "
            "--table Album "
            "--sqlite-path /data/chinook.db "
            "--csv-dir /data/chinook_csv "
            "--metadata-dir /data/chinook_metadata"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    validate_csv = BashOperator(
        task_id="validate_csv",
        bash_command="test -s /data/chinook_csv/Album.csv && echo '[VALIDATE_CSV_OK] /data/chinook_csv/Album.csv'",
        execution_timeout=timedelta(minutes=2),
    )

    data_quality_cleansing = SparkSubmitOperator(
        task_id="data_quality_cleansing",
        application="/opt/airflow/dags/scripts/cleanse_chinook_csv.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        application_args=[
            "--table", "Album",
            "--csv-dir", "/data/chinook_csv",
            "--cleansed-dir", "/data/chinook_cleansed",
            "--metadata-dir", "/data/chinook_metadata",
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )

    validate_cleansed_data = BashOperator(
        task_id="validate_cleansed_data",
        bash_command=(
            "test -d /data/chinook_cleansed/Album && "
            "test -s /data/chinook_metadata/Album_dq_report.json && "
            "echo '[VALIDATE_CLEANSED_OK] /data/chinook_cleansed/Album'"
        ),
        execution_timeout=timedelta(minutes=2),
    )

    load_cleansed_to_delta = SparkSubmitOperator(
        task_id="load_cleansed_to_delta",
        application="/opt/airflow/dags/scripts/load_chinook_cleansed_to_delta.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        application_args=[
            "--table", "Album",
            "--cleansed-dir", "/data/chinook_cleansed",
            "--target-base", "s3a://lakehouse/bronze/chinook",
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=20),
    )

    validate_delta = SparkSubmitOperator(
        task_id="validate_delta",
        application="/opt/airflow/dags/scripts/validate_chinook_delta.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        application_args=[
            "--table", "Album",
            "--target-base", "s3a://lakehouse/bronze/chinook",
            "--min-rows", "0",
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )

    finish = EmptyOperator(task_id="finish")

    start >> simulate_retry_once_for_rca >> export_sqlite_to_csv
    export_sqlite_to_csv >> validate_csv >> data_quality_cleansing >> validate_cleansed_data >> load_cleansed_to_delta >> validate_delta >> finish
