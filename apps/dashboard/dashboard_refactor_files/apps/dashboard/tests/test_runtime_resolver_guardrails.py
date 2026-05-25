from __future__ import annotations


def test_zero_counters_are_preserved():
    from runtime_resolver import collapse_run_events

    rows = [
        {
            "run_id": "r1",
            "state": "running",
            "records_inserted": None,
            "records_updated": None,
            "records_deleted": None,
        },
        {
            "run_id": "r1",
            "state": "success",
            "records_inserted": 0,
            "records_updated": 14,
            "records_deleted": 0,
        },
    ]

    out = collapse_run_events(rows)

    assert len(out) == 1
    assert out[0]["records_inserted"] == 0
    assert out[0]["records_updated"] == 14
    assert out[0]["records_deleted"] == 0
    assert out[0]["state"] == "success"


def test_control_db_precedence_over_registry_and_airflow(monkeypatch):
    import runtime_resolver

    job = {"id": "p__j", "name": "j", "pipeline": "p", "type": "batch"}

    monkeypatch.setattr(
        runtime_resolver,
        "control_run_rows_for_job",
        lambda *_args, **_kwargs: [
            {"run_id": "control-1", "state": "success", "records_inserted": 0}
        ],
    )
    monkeypatch.setattr(
        runtime_resolver,
        "registry_run_rows_for_job",
        lambda *_args, **_kwargs: [
            {"run_id": "registry-1", "state": "failed", "records_inserted": 99}
        ],
    )
    monkeypatch.setattr(
        runtime_resolver,
        "latest_airflow_task",
        lambda *_args, **_kwargs: {
            "current_status": "running",
            "latest_run_id": "airflow-1",
        },
    )
    monkeypatch.setattr(runtime_resolver, "load_stream_status", lambda: {"units": {}})

    result = runtime_resolver.enrich_jobs([job])["jobs"][0]

    assert result["runtime_source"] == "control_db"
    assert result["latest_run_id"] == "control-1"
    assert result["current_status"] == "success"
    assert result["records_inserted"] == 0


def test_latest_dag_run_id_mapped_from_airflow_dag_run_id():
    from runtime_resolver import normalize_runtime_run_row

    row = {
        "run_id": "internal-1",
        "airflow_dag_run_id": "manual__2026-05-25T10:00:00+00:00",
        "status": "success",
    }

    out = normalize_runtime_run_row(row)

    assert out["run_id"] == "internal-1"
    assert out["dag_run_id"] == "manual__2026-05-25T10:00:00+00:00"


def test_silver_merge_inserted_zero_updated_nonzero():
    from runtime_resolver import normalize_runtime_run_row

    row = {
        "run_id": "silver-r1",
        "status": "success",
        "records_inserted": 0,
        "records_updated": 21,
        "records_deleted": 0,
    }

    out = normalize_runtime_run_row(row)

    assert out["records_inserted"] == 0
    assert out["records_updated"] == 21
    assert out["records_deleted"] == 0
