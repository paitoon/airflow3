from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from chinook_spark_common import SPARK_CONF, SPARK_PACKAGES
from chinook_callbacks import get_default_args, on_dag_success, on_dag_failure


with DAG(
    dag_id="chinook_track_to_delta_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["chinook", "track", "sqlite", "csv", "delta", "bronze", "etl", "dq"],
    default_args=get_default_args(),
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_album = ExternalTaskSensor(
        task_id="wait_for_album",
        external_dag_id="chinook_album_to_delta_dag",
        external_task_id="finish",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=10,
        timeout=1800,
        execution_timeout=timedelta(minutes=35),
    )

    wait_for_genre = ExternalTaskSensor(
        task_id="wait_for_genre",
        external_dag_id="chinook_genre_to_delta_dag",
        external_task_id="finish",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=10,
        timeout=1800,
        execution_timeout=timedelta(minutes=35),
    )

    wait_for_mediatype = ExternalTaskSensor(
        task_id="wait_for_mediatype",
        external_dag_id="chinook_mediatype_to_delta_dag",
        external_task_id="finish",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=10,
        timeout=1800,
        execution_timeout=timedelta(minutes=35),
    )

    export_sqlite_to_csv = BashOperator(
        task_id="export_sqlite_to_csv",
        bash_command=(
            "python /opt/airflow/dags/scripts/export_chinook_table.py "
            "--table Track "
            "--sqlite-path /data/chinook.db "
            "--csv-dir /data/chinook_csv "
            "--metadata-dir /data/chinook_metadata"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    validate_csv = BashOperator(
        task_id="validate_csv",
        bash_command="test -s /data/chinook_csv/Track.csv && echo '[VALIDATE_CSV_OK] /data/chinook_csv/Track.csv'",
        execution_timeout=timedelta(minutes=2),
    )

    data_quality_cleansing = SparkSubmitOperator(
        task_id="data_quality_cleansing",
        application="/opt/airflow/dags/scripts/cleanse_chinook_csv.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        application_args=[
            "--table", "Track",
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
            "test -d /data/chinook_cleansed/Track && "
            "test -s /data/chinook_metadata/Track_dq_report.json && "
            "echo '[VALIDATE_CLEANSED_OK] /data/chinook_cleansed/Track'"
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
            "--table", "Track",
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
            "--table", "Track",
            "--target-base", "s3a://lakehouse/bronze/chinook",
            "--min-rows", "0",
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=15),
    )

    finish = EmptyOperator(task_id="finish")

    start >> [wait_for_album, wait_for_genre, wait_for_mediatype] >> export_sqlite_to_csv
    export_sqlite_to_csv >> validate_csv >> data_quality_cleansing >> validate_cleansed_data >> load_cleansed_to_delta >> validate_delta >> finish
