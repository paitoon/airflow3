from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from chinook_callbacks import get_default_args, on_dag_success, on_dag_failure


TRIGGER_POKE_INTERVAL = 10


def trigger_table(table: str) -> TriggerDagRunOperator:
    table_key = table.lower()
    return TriggerDagRunOperator(
        task_id=f"trigger_{table_key}",
        trigger_dag_id=f"chinook_{table_key}_to_delta_dag",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=TRIGGER_POKE_INTERVAL,
        allowed_states=["success"],
        failed_states=["failed"],
        logical_date="{{ data_interval_start }}",
    )


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

    trigger_artist = trigger_table("Artist")
    trigger_album = trigger_table("Album")
    trigger_genre = trigger_table("Genre")
    trigger_mediatype = trigger_table("MediaType")
    trigger_track = trigger_table("Track")
    trigger_playlist = trigger_table("Playlist")
    trigger_playlisttrack = trigger_table("PlaylistTrack")
    trigger_employee = trigger_table("Employee")
    trigger_customer = trigger_table("Customer")
    trigger_invoice = trigger_table("Invoice")
    trigger_invoiceline = trigger_table("InvoiceLine")

    finish = EmptyOperator(task_id="finish")

    # Dimension/base tables can start immediately.
    start >> [
        trigger_artist,
        trigger_genre,
        trigger_mediatype,
        trigger_playlist,
        trigger_employee,
    ]

    # Chinook dependency graph.
    trigger_artist >> trigger_album
    [trigger_album, trigger_genre, trigger_mediatype] >> trigger_track

    trigger_employee >> trigger_customer >> trigger_invoice
    [trigger_invoice, trigger_track] >> trigger_invoiceline

    [trigger_playlist, trigger_track] >> trigger_playlisttrack

    [trigger_invoiceline, trigger_playlisttrack] >> finish
