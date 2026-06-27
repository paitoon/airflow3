from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from chinook_callbacks import get_default_args, on_dag_success, on_dag_failure


TABLES = ['Artist', 'Album', 'Genre', 'MediaType', 'Track', 'Playlist', 'PlaylistTrack', 'Employee', 'Customer', 'Invoice', 'InvoiceLine']


with DAG(
    dag_id="chinook_master_trigger_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["chinook", "master", "trigger", "etl", "dq"],
    default_args=get_default_args(),
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    start = EmptyOperator(task_id="start")

    trigger_tasks = [
        TriggerDagRunOperator(
            task_id=f"trigger_{table.lower()}",
            trigger_dag_id=f"chinook_{table.lower()}_to_delta_dag",
            reset_dag_run=True,
            wait_for_completion=False,
            logical_date="{{ data_interval_start }}",
        )
        for table in TABLES
    ]

    finish = EmptyOperator(task_id="finish")

    start >> trigger_tasks >> finish
