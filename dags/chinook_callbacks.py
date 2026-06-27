import logging
import os
import httpx
from datetime import datetime, timezone, timedelta
from typing import Any

logger = logging.getLogger(__name__)

FASTAPI_URL = os.getenv("PIPELINE_API_URL", "http://idp_manager:8000")
NOTIFY_TIMEOUT = float(os.getenv("PIPELINE_NOTIFY_TIMEOUT", "5"))


def _iso(dt: Any) -> str | None:
    if not dt:
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _get_context_info(context: dict[str, Any]) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    ti = context.get("task_instance")
    exception = context.get("exception")

    return {
        "event_type": "AIRFLOW_CALLBACK",

        "dag_id": dag_run.dag_id if dag_run else getattr(dag, "dag_id", None),
        "dag_run_id": dag_run.run_id if dag_run else None,
        "task_id": ti.task_id if ti else None,

        "dag_state": str(dag_run.state) if dag_run and dag_run.state else None,
        "task_state": str(ti.state) if ti and ti.state else None,

        "dag_start": _iso(dag_run.start_date) if dag_run else None,
        "dag_end": _iso(dag_run.end_date) if dag_run else None,
        "task_start": _iso(ti.start_date) if ti else None,
        "task_end": _iso(ti.end_date) if ti else None,

        "logical_date": _iso(getattr(dag_run, "logical_date", None)) if dag_run else None,

        "exception": str(exception) if exception else None,
        "try_number": ti.try_number if ti else None,
        "max_tries": ti.max_tries if ti else None,

        "callback_ts": datetime.now(timezone.utc).isoformat(),
    }


def _notify(payload: dict[str, Any]) -> None:
    """ส่ง payload ไป FastAPI ถ้า fail ต้องไม่กระทบ DAG"""

    dag_id = payload.get("dag_id")
    dag_run_id = payload.get("dag_run_id")
    status = payload.get("status")

    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": f"{dag_id}:{dag_run_id}:{payload.get('task_id')}:{status}:{payload.get('try_number')}",
    }

    try:
        with httpx.Client(timeout=NOTIFY_TIMEOUT) as client:
            resp = client.post(
                f"{FASTAPI_URL.rstrip('/')}/pipeline/notify",
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()

        logger.info(
            "[NOTIFY_OK] dag=%s run=%s task=%s status=%s http_status=%s",
            dag_id,
            dag_run_id,
            payload.get("task_id"),
            status,
            resp.status_code,
        )

    except Exception as e:
        logger.warning(
            "[NOTIFY_FAIL] dag=%s run=%s task=%s status=%s error=%s",
            dag_id,
            dag_run_id,
            payload.get("task_id"),
            status,
            e,
        )


def on_dag_success(context: dict[str, Any]) -> None:
    info = _get_context_info(context)
    payload = {**info, "status": "success", "scope": "dag"}

    logger.info(
        "[DAG_SUCCESS] dag=%s run=%s",
        payload.get("dag_id"),
        payload.get("dag_run_id"),
    )

    _notify(payload)


def on_dag_failure(context: dict[str, Any]) -> None:
    info = _get_context_info(context)
    payload = {**info, "status": "failed", "scope": "dag"}

    logger.error(
        "[DAG_FAILURE] dag=%s run=%s task=%s exception=%s",
        payload.get("dag_id"),
        payload.get("dag_run_id"),
        payload.get("task_id"),
        payload.get("exception"),
    )

    _notify(payload)


def on_task_retry(context: dict[str, Any]) -> None:
    info = _get_context_info(context)
    payload = {**info, "status": "retry", "scope": "task"}

    logger.warning(
        "[TASK_RETRY] dag=%s run=%s task=%s try=%s",
        payload.get("dag_id"),
        payload.get("dag_run_id"),
        payload.get("task_id"),
        payload.get("try_number"),
    )

    _notify(payload)


def get_default_args(retries: int = 2, retry_delay_minutes: int = 3) -> dict[str, Any]:
    return {
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "on_retry_callback": on_task_retry,
    }