"""Crash recovery: no half-written partitions, no double counting."""
import os

import pytest

from busflow import pipeline
from busflow.store import Store
from helpers import event_tuples, make_v1, stats_map, write_jsonl


def test_crash_after_events_committed_recovers(data_dir, tmp_path, monkeypatch):
    """Crash between the ledger commit and the partition rebuild."""
    f = write_jsonl(tmp_path / "day1.jsonl", [
        make_v1("E1"), make_v1("E2", board="2026-11-01 09:00:00"),
    ])
    store = Store(data_dir)
    original = Store.rebuild_partitions
    monkeypatch.setattr(Store, "rebuild_partitions",
                        lambda self, dates: (_ for _ in ()).throw(
                            RuntimeError("simulated crash")))
    with pytest.raises(RuntimeError, match="simulated crash"):
        pipeline.ingest_file(store, f)
    monkeypatch.setattr(Store, "rebuild_partitions", original)

    # interrupted state: events journalled, no partition, import not committed
    assert store.imports()[0]["status"] == "events_committed"
    assert not (store.events_dir / "date=2026-11-01" / "data.parquet").exists()
    store.close()

    # the next run (a fresh process) finishes the interrupted import
    store = Store(data_dir)
    try:
        store.recover()
        assert store.imports()[0]["status"] == "committed"
        assert (store.events_dir / "date=2026-11-01" / "data.parquet").exists()
        assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 0)
        assert stats_map(store)[("2026-11-01", "42", 9)] == (1, 1, 0)

        # re-importing the same file afterwards is a no-op, not a second count
        result = pipeline.ingest_file(store, f)
        assert result.status == "skipped_duplicate"
        assert store.events_count() == 2
    finally:
        store.close()

    # final state equals a clean one-shot run
    reference = Store(tmp_path / "ref")
    try:
        pipeline.ingest_file(reference, f)
        store = Store(data_dir)
        try:
            assert event_tuples(store) == event_tuples(reference)
            assert store.read_stats() == reference.read_stats()
        finally:
            store.close()
    finally:
        reference.close()


def test_crash_during_partition_write_leaves_no_partial_files(
        data_dir, tmp_path, monkeypatch):
    """Crash in the middle of the atomic partition swap."""
    f = write_jsonl(tmp_path / "day1.jsonl", [make_v1("E1")])
    store = Store(data_dir)
    real_replace = os.replace
    monkeypatch.setattr(os, "replace",
                        lambda src, dst: (_ for _ in ()).throw(
                            RuntimeError("simulated crash during rename")))
    with pytest.raises(RuntimeError, match="simulated crash"):
        pipeline.ingest_file(store, f)
    monkeypatch.setattr(os, "replace", real_replace)

    part_dir = store.events_dir / "date=2026-11-01"
    # no visible partition, only a leftover temp file
    assert not (part_dir / "data.parquet").exists()
    assert list(part_dir.glob(".tmp-*.parquet"))
    store.close()

    store = Store(data_dir)
    try:
        store.recover()
        assert not list(part_dir.glob(".tmp-*.parquet"))  # temp cleaned up
        assert (part_dir / "data.parquet").exists()
        assert store.events_count() == 1
        assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 0)
    finally:
        store.close()


def test_crash_before_events_applied_rolls_back(data_dir, tmp_path, monkeypatch):
    """Crash before the ledger transaction: nothing is applied or counted."""
    f = write_jsonl(tmp_path / "day1.jsonl", [make_v1("E1")])
    store = Store(data_dir)
    original = Store.apply_import
    monkeypatch.setattr(Store, "apply_import",
                        lambda self, *a, **k: (_ for _ in ()).throw(
                            RuntimeError("simulated crash")))
    with pytest.raises(RuntimeError, match="simulated crash"):
        pipeline.ingest_file(store, f)
    monkeypatch.setattr(Store, "apply_import", original)

    assert store.events_count() == 0
    assert store.imports()[0]["status"] == "started"
    store.close()

    store = Store(data_dir)
    try:
        store.recover()
        assert store.imports() == []  # stale journal entry rolled back
        result = pipeline.ingest_file(store, f)
        assert result.status == "committed"
        assert result.accepted == 1
        assert store.events_count() == 1
        assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 0)
    finally:
        store.close()


def test_stale_stats_are_refreshed_on_recovery(data_dir, tmp_path):
    """Crash after the partition swap but before the stats build."""
    f = write_jsonl(tmp_path / "day1.jsonl", [make_v1("E1")])
    store = Store(data_dir)
    pipeline.ingest_file(store, f)

    # simulate the crash: stats file gone, dirty flag stuck on
    (store.stats_dir / "line_hourly.parquet").unlink()
    store._set_meta("stats_dirty", "1")
    store.close()

    store = Store(data_dir)
    try:
        store.recover()
        assert store._get_meta("stats_dirty") == "0"
        assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 0)
    finally:
        store.close()
