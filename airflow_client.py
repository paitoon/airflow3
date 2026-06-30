from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

import httpx


class AirflowClientError(RuntimeError):
    """Raised when Airflow API calls fail after retries."""


class AirflowClient:
    """Airflow API v2 client for IDP/RCA collectors."""

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
        retries: int = 2,
        retry_backoff_sec: float = 1.0,
        log_max_wait_sec: float = 60.0,
        log_poll_sec: float = 5.0,
    ):
        if not base_url:
            raise ValueError("Airflow base_url is required")

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = max(0, retries)
        self.retry_backoff_sec = max(0.0, retry_backoff_sec)
        self.log_max_wait_sec = max(0.0, log_max_wait_sec)
        self.log_poll_sec = max(1.0, log_poll_sec)

        self.headers: dict[str, str] = {
            "Accept": "application/json, text/plain, */*",
        }
        self.auth: httpx.Auth | None = None

        token = (token or "").strip()
        username = (username or "").strip()

        if token:
            self.headers["Authorization"] = f"Bearer {token}"
            self.auth_mode = "bearer"
        elif username:
            self.auth = httpx.BasicAuth(username, password or "")
            self.auth_mode = "basic"
        else:
            self.auth_mode = "none"

    @staticmethod
    def _encode_path_segment(value: str) -> str:
        """Percent-encode a single URL path segment (e.g. dag_run_id with +)."""
        return quote(value, safe="")

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None

        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        # Reuse a single client across retries to avoid repeated connection overhead.
        with httpx.Client(
            timeout=self.timeout,
            headers=request_headers,
            auth=self.auth,
            follow_redirects=True,
        ) as client:
            for attempt in range(self.retries + 1):
                try:
                    resp = client.request(method, url, params=params)

                    if raise_for_status:
                        if resp.status_code == 401:
                            raise AirflowClientError(
                                "Airflow API returned 401 Unauthorized. "
                                f"base_url={self.base_url!r}, auth_mode={self.auth_mode!r}. "
                                "Check [airflow] token or username/password in config.toml."
                            )

                        if resp.status_code == 403:
                            raise AirflowClientError(
                                "Airflow API returned 403 Forbidden. "
                                f"base_url={self.base_url!r}, auth_mode={self.auth_mode!r}. "
                                "The credential is valid but does not have enough permission."
                            )

                        resp.raise_for_status()

                    return resp

                except (
                    httpx.ConnectError,
                    httpx.ReadTimeout,
                    httpx.ConnectTimeout,
                    httpx.RemoteProtocolError,
                ) as exc:
                    last_error = exc
                    if attempt >= self.retries:
                        break
                    time.sleep(self.retry_backoff_sec * (attempt + 1))

                except httpx.HTTPStatusError as exc:
                    last_error = exc
                    status_code = exc.response.status_code if exc.response is not None else None
                    if status_code is None or status_code < 500 or attempt >= self.retries:
                        break
                    time.sleep(self.retry_backoff_sec * (attempt + 1))

                except Exception as exc:
                    last_error = exc
                    break

        raise AirflowClientError(
            f"Airflow API request failed: method={method} url={url} "
            f"auth_mode={self.auth_mode} params={params} error={last_error}"
        ) from last_error

    def _decode_response(self, resp: httpx.Response) -> Any:
        content_type = resp.headers.get("content-type", "")
        if "application/json" in content_type:
            return resp.json()
        return resp.text

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        resp = self._request("GET", path, params=params, headers=headers)
        return self._decode_response(resp)

    def get_health(self) -> Any:
        return self._get("/monitor/health")

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        path = (
            f"/dags/{self._encode_path_segment(dag_id)}"
            f"/dagRuns/{self._encode_path_segment(dag_run_id)}"
        )
        payload = self._get(path)
        if isinstance(payload, dict):
            return payload
        return {"raw": payload}

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> list[dict[str, Any]]:
        path = (
            f"/dags/{self._encode_path_segment(dag_id)}"
            f"/dagRuns/{self._encode_path_segment(dag_run_id)}"
            f"/taskInstances"
        )
        payload = self._get(path)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("task_instances", "taskInstances", "tasks", "items", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def _extract_log_text(self, payload: Any) -> str:
        if payload is None:
            return ""

        if isinstance(payload, str):
            return payload

        if isinstance(payload, list):
            return "\n".join(
                item for item in (self._extract_log_text(x) for x in payload) if item
            )

        if isinstance(payload, dict):
            for key in ("content", "message", "log", "logs", "data", "text", "body"):
                value = payload.get(key)
                if value is not None:
                    text = self._extract_log_text(value)
                    if text:
                        return text

            parts: list[str] = []
            for value in payload.values():
                if isinstance(value, (str, list, dict)):
                    text = self._extract_log_text(value)
                    if text:
                        parts.append(text)
            return "\n".join(parts) if parts else str(payload)

        return str(payload)

    def _get_task_log_direct(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int = -1,
    ) -> str:
        # Percent-encode path segments so characters like + in run_id are safe.
        path = (
            f"/dags/{self._encode_path_segment(dag_id)}"
            f"/dagRuns/{self._encode_path_segment(dag_run_id)}"
            f"/taskInstances/{self._encode_path_segment(task_id)}"
            f"/logs/{try_number}"
        )
        # Airflow 3 returns 404 when map_index=-1 is sent for non-mapped tasks.
        # Only include map_index for actual mapped tasks (map_index >= 0).
        params: dict[str, Any] = {"full_content": "true"}
        if map_index >= 0:
            params["map_index"] = map_index

        accepts = [
            "application/json",
            "application/x-ndjson",
            "text/plain",
            "*/*",
        ]

        last_error: Exception | None = None
        got_empty_response = False

        for accept in accepts:
            try:
                payload = self._get(
                    path,
                    params=params,
                    headers={"Accept": accept},
                )
                text = self._extract_log_text(payload)
                if text:
                    return text
                got_empty_response = True
            except Exception as exc:
                last_error = exc

        if last_error is None:
            last_error = ValueError(
                "All Accept types returned empty content"
                if got_empty_response
                else "No Accept types attempted"
            )

        raise AirflowClientError(
            f"Could not fetch task log from direct endpoint. "
            f"dag_id={dag_id} dag_run_id={dag_run_id} task_id={task_id} "
            f"try_number={try_number} map_index={map_index} error={last_error}"
        ) from last_error

    def _get_external_log_url(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int = -1,
    ) -> str | None:
        path = (
            f"/dags/{self._encode_path_segment(dag_id)}"
            f"/dagRuns/{self._encode_path_segment(dag_run_id)}"
            f"/taskInstances/{self._encode_path_segment(task_id)}"
            f"/externalLogUrl/{try_number}"
        )
        ext_params: dict[str, Any] = {}
        if map_index >= 0:
            ext_params["map_index"] = map_index
        try:
            payload = self._get(
                path,
                params=ext_params or None,
                headers={"Accept": "application/json"},
            )
        except Exception:
            return None

        if isinstance(payload, dict):
            for key in ("url", "external_log_url", "externalLogUrl", "log_url", "logUrl"):
                value = payload.get(key)
                if isinstance(value, str) and value:
                    return value

        if isinstance(payload, str) and payload.startswith("http"):
            return payload

        return None

    def _fetch_url(self, url: str) -> str:
        with httpx.Client(
            timeout=self.timeout,
            headers=self.headers,
            auth=self.auth,
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.text

    def _get_task_log_once(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int,
    ) -> str:
        """Single attempt to fetch task log (direct then external URL).

        Raises AirflowClientError on any failure.
        """
        direct_error: Exception | None = None

        try:
            return self._get_task_log_direct(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                try_number=try_number,
                map_index=map_index,
            )
        except Exception as exc:
            direct_error = exc

        external_url = self._get_external_log_url(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
        )

        if external_url:
            try:
                return self._fetch_url(external_url)
            except Exception as exc:
                raise AirflowClientError(
                    f"Could not fetch task log from direct endpoint or external URL. "
                    f"dag_id={dag_id} dag_run_id={dag_run_id} task_id={task_id} "
                    f"try_number={try_number} map_index={map_index} "
                    f"direct_error={direct_error} external_url={external_url} "
                    f"external_error={exc}"
                ) from exc

        raise AirflowClientError(
            f"Could not fetch task log. "
            f"dag_id={dag_id} dag_run_id={dag_run_id} task_id={task_id} "
            f"try_number={try_number} map_index={map_index} "
            f"direct_error={direct_error}"
        ) from direct_error

    @staticmethod
    def _is_404_error(exc: Exception) -> bool:
        """Return True if the exception indicates a 404 Not Found."""
        msg = str(exc)
        return "404" in msg

    def get_task_log(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        map_index: int = -1,
    ) -> str:
        """Return Airflow task log text using Airflow 3 API.

        Retries on 404 up to ``log_max_wait_sec`` seconds — Celery workers
        sometimes haven't flushed the log file by the time the monitoring
        system requests it.

        Tries per attempt:
        1. /logs/{try_number}?full_content=true&map_index=...
        2. /externalLogUrl/{try_number}?map_index=...
        """
        try_number = max(1, int(try_number))
        map_index = int(map_index)

        deadline = time.monotonic() + self.log_max_wait_sec
        last_error: Exception | None = None

        while True:
            try:
                return self._get_task_log_once(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_id,
                    try_number=try_number,
                    map_index=map_index,
                )
            except AirflowClientError as exc:
                last_error = exc
                if not self._is_404_error(exc):
                    # Non-404 errors (e.g. 401, 403, 500) → fail immediately.
                    raise

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            wait = min(self.log_poll_sec, remaining)
            time.sleep(wait)

        raise AirflowClientError(
            f"Log file not available after {self.log_max_wait_sec:.0f}s "
            f"(Celery worker may not have flushed yet). "
            f"dag_id={dag_id} dag_run_id={dag_run_id} task_id={task_id} "
            f"try_number={try_number} error={last_error}"
        ) from last_error
