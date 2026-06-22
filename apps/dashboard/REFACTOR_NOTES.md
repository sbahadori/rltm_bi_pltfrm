# Dashboard `app.py` Refactor Notes

This package splits the previous monolithic `apps/dashboard/app.py` into small, single-responsibility modules.

## Files

```text
apps/dashboard/
  app.py
  db.py
  config_loader.py
  airflow_client.py
  stream_runtime.py
  runtime_models.py
  runtime_resolver.py
  runtime_api.py
  control_api.py
  action_api.py
  log_api.py
  auth_api.py
  websocket_api.py
  __init__.py
```

## Runtime source precedence

Batch:

1. Control DB runtime rows
2. Control DB metadata/no-runtime-row explanation
3. Catalog-defined/unknown

Stream:

1. Control DB stream runtime tables
2. Stream supervisor status file as labelled local/debug fallback
3. Catalog-defined/unknown

## Important guardrail

Zero counters are valid values. They must not be treated as missing values.

Examples:

```python
records_inserted = 0  # valid
records_updated = 0   # valid
records_deleted = 0   # valid
```

## Migration sequence

1. Copy these files into `apps/dashboard/`.
2. Keep the current `auth.py` and `catalog_editor.py` unchanged.
3. Replace the old monolithic `app.py` with the new composition-root `app.py`.
4. Restart dashboard API.
5. Smoke test:
   - `GET /health`
   - `GET /api/config`
   - `GET /api/runtime/jobs`
   - `GET /api/runtime/runs/{job_id}`
   - `GET /api/runtime/streams/status`
   - `GET /api/runtime/mounts`
6. Only after runtime behavior is stable, delete/archive unused legacy functions from the old file history.

## Design choices

- `app.py` is only a composition root.
- `db.py` is the only Control DB gateway.
- `airflow_client.py` is the only Airflow HTTP client.
- `stream_runtime.py` owns stream heartbeat/status/control.
- `runtime_resolver.py` owns source precedence and normalized runtime schema.
- API route files are thin routers and delegate to service modules.
