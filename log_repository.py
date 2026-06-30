from __future__ import annotations

import hashlib
import json
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_text(_json_dumps(value))


def get_pipeline_run(pool: ConnectionPool, pipeline_run_id: int) -> dict[str, Any] | None:
    sql = """
    select *
    from pipeline_runs
    where id = %s
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (pipeline_run_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def get_pipeline_run_by_dag_run(
    pool: ConnectionPool,
    dag_id: str,
    dag_run_id: str,
) -> dict[str, Any] | None:
    sql = """
    select *
    from pipeline_runs
    where dag_id = %s
      and dag_run_id = %s
    """
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (dag_id, dag_run_id))
            row = cur.fetchone()
    return dict(row) if row else None


def save_pipeline_resource(
    pool: ConnectionPool,
    pipeline_run_id: int,
    resource_type: str,
    resource_id: str,
    resource_name: str | None = None,
    discovered_from: str = "AIRFLOW_LOG",
    confidence: str = "high",
    raw_payload: dict[str, Any] | None = None,
    pipeline_task_run_id: int | None = None,
    parent_resource_type: str | None = None,
    parent_resource_id: str | None = None,
    namespace: str | None = None,
    cluster_name: str | None = None,
    application_id: str | None = None,
    started_at: Any | None = None,
    finished_at: Any | None = None,
    duration_sec: float | None = None,
) -> dict[str, Any] | None:
    sql = """
    insert into pipeline_resources (
        pipeline_run_id,
        pipeline_task_run_id,
        resource_type,
        resource_id,
        resource_name,
        parent_resource_type,
        parent_resource_id,
        discovered_from,
        confidence,
        namespace,
        cluster_name,
        application_id,
        started_at,
        finished_at,
        duration_sec,
        raw_payload
    )
    values (
        %(pipeline_run_id)s,
        %(pipeline_task_run_id)s,
        %(resource_type)s::pipeline_resource_type,
        %(resource_id)s,
        %(resource_name)s,
        %(parent_resource_type)s::pipeline_resource_type,
        %(parent_resource_id)s,
        %(discovered_from)s,
        %(confidence)s::resource_confidence,
        %(namespace)s,
        %(cluster_name)s,
        %(application_id)s,
        %(started_at)s,
        %(finished_at)s,
        %(duration_sec)s,
        %(raw_payload)s::jsonb
    )
    on conflict (
        pipeline_run_id,
        (coalesce(pipeline_task_run_id, 0)),
        resource_type,
        resource_id
    )
    do update set
        pipeline_task_run_id = coalesce(excluded.pipeline_task_run_id, pipeline_resources.pipeline_task_run_id),
        resource_name = coalesce(excluded.resource_name, pipeline_resources.resource_name),
        parent_resource_type = coalesce(excluded.parent_resource_type, pipeline_resources.parent_resource_type),
        parent_resource_id = coalesce(excluded.parent_resource_id, pipeline_resources.parent_resource_id),
        discovered_from = excluded.discovered_from,
        confidence = excluded.confidence,
        namespace = coalesce(excluded.namespace, pipeline_resources.namespace),
        cluster_name = coalesce(excluded.cluster_name, pipeline_resources.cluster_name),
        application_id = coalesce(excluded.application_id, pipeline_resources.application_id),
        started_at = coalesce(excluded.started_at, pipeline_resources.started_at),
        finished_at = coalesce(excluded.finished_at, pipeline_resources.finished_at),
        duration_sec = coalesce(excluded.duration_sec, pipeline_resources.duration_sec),
        raw_payload = coalesce(excluded.raw_payload, pipeline_resources.raw_payload),
        updated_at = now()
    returning *
    """
    params = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_task_run_id": pipeline_task_run_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "parent_resource_type": parent_resource_type,
        "parent_resource_id": parent_resource_id,
        "discovered_from": discovered_from,
        "confidence": confidence,
        "namespace": namespace,
        "cluster_name": cluster_name,
        "application_id": application_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": duration_sec,
        "raw_payload": _json_dumps(raw_payload or {}),
    }
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None


def save_log_source_text(
    pool: ConnectionPool,
    pipeline_run_id: int,
    source_type: str,
    raw_text: str | None = None,
    raw_content: str | None = None,
    source_uri: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    try_number: int | None = None,
    map_index: int | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    pipeline_task_run_id: int | None = None,
) -> int:
    content = raw_text if raw_text is not None else raw_content
    content = content or ""

    sql = """
    insert into log_sources (
        pipeline_run_id,
        pipeline_task_run_id,
        source_type,
        content_type,
        source_uri,
        dag_id,
        dag_run_id,
        task_id,
        try_number,
        map_index,
        resource_type,
        resource_id,
        raw_content,
        raw_json,
        content_hash,
        content_size_bytes,
        line_count
    )
    values (
        %(pipeline_run_id)s,
        %(pipeline_task_run_id)s,
        %(source_type)s::log_source_type,
        'TEXT'::log_content_type,
        %(source_uri)s,
        %(dag_id)s,
        %(dag_run_id)s,
        %(task_id)s,
        %(try_number)s,
        %(map_index)s,
        %(resource_type)s::pipeline_resource_type,
        %(resource_id)s,
        %(raw_content)s,
        null,
        %(content_hash)s,
        %(content_size_bytes)s,
        %(line_count)s
    )
    on conflict (
        pipeline_run_id,
        (coalesce(pipeline_task_run_id, 0)),
        source_type,
        (coalesce(task_id, '')),
        (coalesce(try_number, 0)),
        (coalesce(map_index, -1)),
        (coalesce(resource_id, ''))
    )
    do update set
        pipeline_task_run_id = coalesce(excluded.pipeline_task_run_id, log_sources.pipeline_task_run_id),
        content_type = excluded.content_type,
        source_uri = excluded.source_uri,
        dag_id = excluded.dag_id,
        dag_run_id = excluded.dag_run_id,
        map_index = excluded.map_index,
        resource_type = excluded.resource_type,
        raw_content = excluded.raw_content,
        raw_json = null,
        content_hash = excluded.content_hash,
        content_size_bytes = excluded.content_size_bytes,
        line_count = excluded.line_count,
        collected_at = now()
    returning id
    """
    params = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_task_run_id": pipeline_task_run_id,
        "source_type": source_type,
        "source_uri": source_uri,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "raw_content": content,
        "content_hash": _sha256_text(content),
        "content_size_bytes": len(content.encode("utf-8", errors="replace")),
        "line_count": len(content.splitlines()),
    }
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise RuntimeError("save_log_source_text failed: no id returned")
    return int(row[0])


def save_log_source_json(
    pool: ConnectionPool,
    pipeline_run_id: int,
    source_type: str,
    raw_json: Any,
    source_uri: str | None = None,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    task_id: str | None = None,
    try_number: int | None = None,
    map_index: int | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    pipeline_task_run_id: int | None = None,
) -> int:
    json_text = _json_dumps(raw_json if raw_json is not None else {})
    sql = """
    insert into log_sources (
        pipeline_run_id,
        pipeline_task_run_id,
        source_type,
        content_type,
        source_uri,
        dag_id,
        dag_run_id,
        task_id,
        try_number,
        map_index,
        resource_type,
        resource_id,
        raw_content,
        raw_json,
        content_hash,
        content_size_bytes,
        line_count
    )
    values (
        %(pipeline_run_id)s,
        %(pipeline_task_run_id)s,
        %(source_type)s::log_source_type,
        'JSON'::log_content_type,
        %(source_uri)s,
        %(dag_id)s,
        %(dag_run_id)s,
        %(task_id)s,
        %(try_number)s,
        %(map_index)s,
        %(resource_type)s::pipeline_resource_type,
        %(resource_id)s,
        null,
        %(raw_json)s::jsonb,
        %(content_hash)s,
        %(content_size_bytes)s,
        null
    )
    on conflict (
        pipeline_run_id,
        (coalesce(pipeline_task_run_id, 0)),
        source_type,
        (coalesce(task_id, '')),
        (coalesce(try_number, 0)),
        (coalesce(map_index, -1)),
        (coalesce(resource_id, ''))
    )
    do update set
        pipeline_task_run_id = coalesce(excluded.pipeline_task_run_id, log_sources.pipeline_task_run_id),
        content_type = excluded.content_type,
        source_uri = excluded.source_uri,
        dag_id = excluded.dag_id,
        dag_run_id = excluded.dag_run_id,
        map_index = excluded.map_index,
        resource_type = excluded.resource_type,
        raw_content = null,
        raw_json = excluded.raw_json,
        content_hash = excluded.content_hash,
        content_size_bytes = excluded.content_size_bytes,
        line_count = null,
        collected_at = now()
    returning id
    """
    params = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_task_run_id": pipeline_task_run_id,
        "source_type": source_type,
        "source_uri": source_uri,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "try_number": try_number,
        "map_index": map_index,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "raw_json": json_text,
        "content_hash": _sha256_text(json_text),
        "content_size_bytes": len(json_text.encode("utf-8", errors="replace")),
    }
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise RuntimeError("save_log_source_json failed: no id returned")
    return int(row[0])


def delete_log_chunks_for_source(pool: ConnectionPool, log_source_id: int) -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from log_chunks where log_source_id = %s", (log_source_id,))
        conn.commit()


def _guess_level(message: str) -> str | None:
    upper = message.upper()
    if "ERROR" in upper:
        return "ERROR"
    if "WARN" in upper or "WARNING" in upper:
        return "WARN"
    if "INFO" in upper:
        return "INFO"
    if "DEBUG" in upper:
        return "DEBUG"
    return None


def _is_error(message: str) -> bool:
    upper = message.upper()
    return any(
        token in upper
        for token in [
            "ERROR",
            "EXCEPTION",
            "TRACEBACK",
            "FAILED",
            "FAILURE",
            "OUTOFMEMORY",
            "OUT OF MEMORY",
            "EXECUTORLOSTFAILURE",
            "PERMISSION DENIED",
            "TIMEOUT",
            "PY4JJAVAERROR",
            "ANALYSIS_EXCEPTION",
        ]
    )


def _build_text_chunks(
    raw_text: str,
    max_lines: int = 80,
    max_chars: int = 8000,
) -> list[dict[str, Any]]:
    lines = raw_text.splitlines()
    chunks: list[dict[str, Any]] = []
    current: list[str] = []
    line_start: int | None = None
    char_start = 0
    running_char = 0

    def flush(line_end: int, char_end: int) -> None:
        nonlocal current, line_start, char_start
        message = "\n".join(current).strip()
        if message:
            chunks.append(
                {
                    "chunk_index": len(chunks),
                    "line_start": line_start,
                    "line_end": line_end,
                    "char_start": char_start,
                    "char_end": char_end,
                    "message": message,
                }
            )
        current = []
        line_start = None
        char_start = char_end

    for idx, line in enumerate(lines, start=1):
        if line_start is None:
            line_start = idx
            char_start = running_char

        candidate_len = len("\n".join(current + [line]))
        if current and (len(current) >= max_lines or candidate_len >= max_chars):
            flush(idx - 1, running_char)
            line_start = idx
            char_start = running_char

        current.append(line)
        running_char += len(line) + 1

    if current:
        flush(len(lines), running_char)

    return chunks


def save_log_chunks_from_text(
    pool: ConnectionPool,
    pipeline_run_id: int,
    log_source_id: int,
    source_type: str,
    raw_text: str,
    pipeline_task_run_id: int | None = None,
    max_lines: int = 80,
    max_chars: int = 8000,
) -> int:
    if not raw_text:
        return 0

    chunks = _build_text_chunks(raw_text, max_lines=max_lines, max_chars=max_chars)
    if not chunks:
        return 0

    delete_log_chunks_for_source(pool, log_source_id)

    sql = """
    insert into log_chunks (
        log_source_id,
        pipeline_run_id,
        pipeline_task_run_id,
        source_type,
        chunk_index,
        line_start,
        line_end,
        char_start,
        char_end,
        message,
        message_hash,
        level,
        is_error,
        error_hint
    )
    values (
        %s,
        %s,
        %s,
        %s::log_source_type,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
    on conflict (log_source_id, chunk_index)
    do update set
        pipeline_run_id = excluded.pipeline_run_id,
        pipeline_task_run_id = excluded.pipeline_task_run_id,
        source_type = excluded.source_type,
        line_start = excluded.line_start,
        line_end = excluded.line_end,
        char_start = excluded.char_start,
        char_end = excluded.char_end,
        message = excluded.message,
        message_hash = excluded.message_hash,
        level = excluded.level,
        is_error = excluded.is_error,
        error_hint = excluded.error_hint
    """

    rows = []
    for chunk in chunks:
        message = chunk["message"]
        is_error = _is_error(message)
        rows.append(
            (
                log_source_id,
                pipeline_run_id,
                pipeline_task_run_id,
                source_type,
                chunk["chunk_index"],
                chunk["line_start"],
                chunk["line_end"],
                chunk["char_start"],
                chunk["char_end"],
                message,
                _sha256_text(message),
                _guess_level(message),
                is_error,
                "possible_error_chunk" if is_error else None,
            )
        )

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            affected = cur.rowcount
        conn.commit()

    return affected


# Backward-compatible aliases used by earlier POC code.
def save_log_source(*args, **kwargs) -> int:
    raw_content = kwargs.pop("raw_content", None)
    raw_json = kwargs.pop("raw_json", None)
    if raw_content is not None:
        return save_log_source_text(*args, raw_content=raw_content, **kwargs)
    return save_log_source_json(*args, raw_json=raw_json if raw_json is not None else {}, **kwargs)


def save_log_chunks(*args, **kwargs) -> int:
    raw_text = kwargs.pop("raw_text", "")  # [FIX] default "" ป้องกัน KeyError
    return save_log_chunks_from_text(*args, raw_text=raw_text, **kwargs)
