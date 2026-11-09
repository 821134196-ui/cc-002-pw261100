"""Quarantine reprocessing and date recompute."""
from busflow import pipeline
from helpers import make_v1, stats_map, write_jsonl


def _conflicting_pair(tmp_path):
    fa = write_jsonl(tmp_path / "a.jsonl", [
        make_v1("E1", station="Central", board="2026-11-01 08:15:00"),
    ])
    fb = write_jsonl(tmp_path / "b.jsonl", [
        make_v1("E1", station="Riverside", board="2026-11-01 09:45:00"),
    ])
    return fa, fb


def test_reprocess_keep_existing_leaves_conflicts_pending(store, tmp_path):
    fa, fb = _conflicting_pair(tmp_path)
    pipeline.ingest_file(store, fa)
    pipeline.ingest_file(store, fb)

    result = pipeline.reprocess_quarantine(store, "keep-existing")

    assert (result.examined, result.still_pending, result.ingested) == (1, 1, 0)
    assert store.get_event("E1")["station"] == "Central"
    assert len(store.quarantine_rows(status="pending")) == 1


def test_reprocess_use_quarantined_replaces_event(store, tmp_path):
    fa, fb = _conflicting_pair(tmp_path)
    pipeline.ingest_file(store, fa)
    pipeline.ingest_file(store, fb)

    result = pipeline.reprocess_quarantine(store, "use-quarantined")

    assert (result.ingested, result.still_pending) == (1, 0)
    assert result.affected_dates == ["2026-11-01"]
    event = store.get_event("E1")
    assert event["station"] == "Riverside"
    assert event["board_time"] == "2026-11-01T09:45:00"
    # stats moved from hour 8 to hour 9 and the anomaly is gone
    stats = stats_map(store)
    assert ("2026-11-01", "42", 8) not in stats
    assert stats[("2026-11-01", "42", 9)] == (1, 1, 0)
    assert store.quarantine_rows(status="pending") == []
    # partition on disk reflects the replacement
    rows = store.read_partition("2026-11-01")
    assert len(rows) == 1 and rows[0]["station"] == "Riverside"


def test_reprocess_drop_discards_conflicting_record(store, tmp_path):
    fa, fb = _conflicting_pair(tmp_path)
    pipeline.ingest_file(store, fa)
    pipeline.ingest_file(store, fb)

    result = pipeline.reprocess_quarantine(store, "drop")

    assert result.dropped == 1
    assert store.get_event("E1")["station"] == "Central"
    assert store.quarantine_rows(status="pending") == []
    # anomaly count cleared now that the record is resolved
    assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 0)


def test_reprocess_leaves_unfixable_records_pending(store, tmp_path):
    f = write_jsonl(tmp_path / "bad.jsonl", ["{broken json", make_v1("E1")])
    pipeline.ingest_file(store, f)

    result = pipeline.reprocess_quarantine(store, "use-quarantined")

    assert result.still_pending == 1
    assert len(store.quarantine_rows(status="pending")) == 1
    # the anomaly is still counted while pending
    assert sum(v[2] for v in stats_map(store).values()) == 1


def test_recompute_date_restores_deleted_partition(store, tmp_path):
    f = write_jsonl(tmp_path / "day1.jsonl", [
        make_v1("E1"), make_v1("E2", board="2026-11-01 09:00:00"),
    ])
    pipeline.ingest_file(store, f)
    before = store.read_partition("2026-11-01")

    (store.events_dir / "date=2026-11-01" / "data.parquet").unlink()
    dates = pipeline.recompute(store, ["2026-11-01"])

    assert dates == ["2026-11-01"]
    assert store.read_partition("2026-11-01") == before


def test_recompute_all_reproduces_identical_stats(store, tmp_path):
    f = write_jsonl(tmp_path / "two_days.jsonl", [
        make_v1("E1", board="2026-11-01 08:15:00"),
        make_v1("E2", board="2026-11-02 10:30:00", card="C2"),
        "{broken",
    ])
    pipeline.ingest_file(store, f)
    stats_before = store.read_stats()

    dates = pipeline.recompute(store)  # all dates

    assert dates == ["2026-11-01", "2026-11-02"]
    assert store.read_stats() == stats_before
