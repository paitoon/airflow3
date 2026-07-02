"""DAG ทดสอบ RCA + DE email escalation — task `always_fail` ล้มเหลวเสมอ

Flow ที่คาดหวังเมื่อ trigger:
  1. try 1 fail → on_task_retry → status=retry (ต่ำกว่า threshold ไม่แจ้ง)
  2. try 2 fail (หมด retry) → on_task_failure (scope=task, status=failed)
     → pipeline_runs.failed_task_id = 'always_fail'
     → เปิด RCA case + ส่งเมล์ escalation หา DE คนแรก
  3. on_dag_failure (scope=dag) ตามมา — เป็น duplicate ของเคสเดิม (idempotent)

ลบไฟล์นี้ทิ้งได้เลยเมื่อทดสอบเสร็จ
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from chinook_callbacks import get_default_args, on_dag_failure, on_dag_success

with DAG(
    dag_id="chinook_fail_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["chinook", "test", "rca", "escalation"],
    # retries=1, delay 1 นาที → ได้ทั้ง event retry และ failed ภายใน ~2 นาที
    default_args=get_default_args(retries=1, retry_delay_minutes=1),
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    start = EmptyOperator(task_id="start")

    always_fail = BashOperator(
        task_id="always_fail",
        bash_command=(
            "echo '[FAIL_TEST] simulating pipeline failure for RCA escalation test'; "
            "exit 1"
        ),
        execution_timeout=timedelta(minutes=2),
    )

    finish = EmptyOperator(task_id="finish")

    start >> always_fail >> finish
