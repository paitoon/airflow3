# Chinook RCA test DAG set

This test set intentionally injects:
- `chinook_album_to_delta_dag.simulate_retry_once_for_rca`: fails on first try, then succeeds on retry.
- `chinook_invoice_to_delta_dag.simulate_failed_task_for_rca`: fails on all tries, causing the DAG and master DAG to fail.

Expected RCA behavior:
- retry event should call `/pipeline/notify` with status `retry`
- failed event should call `/pipeline/notify` with status `failed`
- `idp_manager` should queue log collection for retry/failed events
- Airflow task logs should be collected into `log_sources`
- log chunks should be created in `log_chunks`

Install:
1. Unzip/copy these files to your Airflow DAGs folder.
2. Restart or wait for dag-processor/scheduler reload.
3. Trigger only `chinook_master_trigger_dag`.

Suggested checks:
```sql
select id, dag_id, dag_run_id, status, latest_task_id, failed_task_id
from pipeline_runs
order by id desc
limit 20;

select id, pipeline_run_id, pipeline_task_run_id, source_type, content_type, task_id, try_number
from log_sources
order by id desc
limit 20;

select id, pipeline_run_id, pipeline_task_run_id, log_source_id, is_error, level
from log_chunks
order by id desc
limit 20;
```
