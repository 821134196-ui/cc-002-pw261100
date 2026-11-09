"""Ingest tests: mixed versions, idempotency, dedupe, conflicts, bad data."""
from busflow import pipeline
from helpers import make_v1, make_v2, stats_map, write_jsonl


def test_basic_ingest_mixed_versions(store, tmp_path):
    f = write_jsonl(tmp_path / "day1.jsonl", [
        make_v1("E1", line="42", station="Central",
                board="2026-11-01 08:15:00", card="C1"),
        make_v1("E2", line="42", station="Central",
                board="2026-11-01 08:20:00", card="C2"),
        make_v1("E3", line="42", station="Riverside",
                board="2026-11-01 08:45:00", card="C1"),  # same card as E1
        make_v2("E4", line="7", station="Museum",
                board="2026-11-01T09:05:00Z", card="C3"),
    ])
    result = pipeline.ingest_file(store, f)

    assert result.status == "committed"
    assert (result.total_lines, result.accepted, result.duplicates,
            result.conflicts, result.invalids) == (4, 4, 0, 0, 0)
    assert result.affected_dates == ["2026-11-01"]

    partition = store.events_dir / "date=2026-11-01" / "data.parquet"
    assert partition.exists()
    assert len(store.read_partition("2026-11-01")) == 4

    stats = stats_map(store)
    # 3 swipes on line 42 at hour 8, but only 2 distinct cards (C1, C2)
    assert stats[("2026-11-01", "42", 8)] == (3, 2, 0)
    assert stats[("2026-11-01", "7", 9)] == (1, 1, 0)


def test_same_file_twice_is_not_double_counted(store, tmp_path):
    f = write_jsonl(tmp_path / "day1.jsonl", [
        make_v1("E1"), make_v1("E2", board="2026-11-01 09:00:00"),
    ])
    first = pipeline.ingest_file(store, f)
    second = pipeline.ingest_file(store, f)

    assert first.status == "committed"
    assert second.status == "skipped_duplicate"
    assert store.events_count() == 2
    stats = stats_map(store)
    assert stats[("2026-11-01", "42", 8)] == (1, 1, 0)
    assert stats[("2026-11-01", "42", 9)] == (1, 1, 0)
    # the skipped import is not journalled twice
    assert len([i for i in store.imports() if i["status"] == "committed"]) == 1


def test_same_content_under_a_different_path_is_skipped(store, tmp_path):
    f1 = write_jsonl(tmp_path / "a.jsonl", [make_v1("E1")])
    f2 = tmp_path / "b.jsonl"
    f2.write_bytes(f1.read_bytes())

    pipeline.ingest_file(store, f1)
    result = pipeline.ingest_file(store, f2)

    assert result.status == "skipped_duplicate"
    assert store.events_count() == 1


def test_overlapping_files_dedupe_by_event_id(store, tmp_path):
    fa = write_jsonl(tmp_path / "a.jsonl", [
        make_v1("E1"), make_v1("E2", board="2026-11-01 09:00:00"),
    ])
    fb = write_jsonl(tmp_path / "b.jsonl", [
        make_v1("E2", board="2026-11-01 09:00:00"),      # exact duplicate
        make_v1("E3", board="2026-11-01 10:00:00"),      # new
    ])
    pipeline.ingest_file(store, fa)
    result = pipeline.ingest_file(store, fb)

    assert (result.accepted, result.duplicates) == (1, 1)
    assert store.events_count() == 3
    assert stats_map(store)[("2026-11-01", "42", 10)] == (1, 1, 0)


def test_cross_version_duplicate_is_detected(store, tmp_path):
    fa = write_jsonl(tmp_path / "a.jsonl", [
        make_v1("E1", board="2026-11-01 08:15:00", recv="2026-11-01 08:16:00"),
    ])
    fb = write_jsonl(tmp_path / "b.jsonl", [
        make_v2("E1", board="2026-11-01T08:15:00Z", recv="2026-11-01T08:16:00Z"),
    ])
    pipeline.ingest_file(store, fa)
    result = pipeline.ingest_file(store, fb)

    assert (result.accepted, result.duplicates, result.conflicts) == (0, 1, 0)
    assert store.events_count() == 1


def test_conflicting_event_id_is_quarantined(store, tmp_path):
    fa = write_jsonl(tmp_path / "a.jsonl", [make_v1("E1", station="Central")])
    fb = write_jsonl(tmp_path / "b.jsonl", [make_v1("E1", station="Riverside")])
    pipeline.ingest_file(store, fa)
    result = pipeline.ingest_file(store, fb)

    assert (result.accepted, result.conflicts) == (0, 1)
    rows = store.quarantine_rows(status="pending")
    assert len(rows) == 1
    row = rows[0]
    assert row["source_file"] == str(fb)
    assert row["line_number"] == 1
    assert "conflict" in row["reason"]
    assert row["event_id"] == "E1"
    # the stored event keeps the original content
    assert store.get_event("E1")["station"] == "Central"
    # the conflict is counted as an anomaly at its own date/line/hour
    assert stats_map(store)[("2026-11-01", "42", 8)] == (1, 1, 1)


def test_bad_records_are_quarantined_with_source_line_and_reason(store, tmp_path):
    f = write_jsonl(tmp_path / "bad.jsonl", [
        make_v1("E1"),                                            # line 1: ok
        "{not valid json",                                        # line 2
        {"line": "42", "station": "X",                            # line 3: no event_id
         "board_time": "2026-11-01 08:00:00",
         "receive_time": "2026-11-01 08:01:00"},
        make_v1("E4", board="not-a-timestamp"),                   # line 4: bad ts
        {"event_id": "E5", "station": "X",                        # line 5: no line
         "board_time": "2026-11-01 08:00:00",
         "receive_time": "2026-11-01 08:01:00"},
    ])
    result = pipeline.ingest_file(store, f)

    assert (result.accepted, result.invalids) == (1, 4)
    by_line = {r["line_number"]: r for r in store.quarantine_rows(status="pending")}
    assert set(by_line) == {2, 3, 4, 5}
    assert "invalid JSON" in by_line[2]["reason"]
    assert "version" in by_line[3]["reason"] or "event_id" in by_line[3]["reason"]
    assert "board_time" in by_line[4]["reason"]
    assert "'line'" in by_line[5]["reason"]
    assert all(r["source_file"] == str(f) for r in by_line.values())
    # raw content is preserved for reprocessing
    assert by_line[2]["raw"] == "{not valid json"
    # the one valid record was still ingested
    assert store.events_count() == 1
    # every quarantined record shows up in the anomaly counts
    stats = stats_map(store)
    assert stats[("2026-11-01", "42", 8)] == (1, 1, 1)  # line 3 attributable
    assert sum(v[2] for v in stats.values()) == 4       # 4 anomalies in total


def test_blank_lines_are_ignored(store, tmp_path):
    f = tmp_path / "blanks.jsonl"
    f.write_text('\n'.join([
        '{"event_id": "E1", "line": "42", "station": "S",'
        ' "board_time": "2026-11-01 08:00:00", "receive_time": "2026-11-01 08:01:00"}',
        "", "   ", ""]))

    result = pipeline.ingest_file(store, f)
    assert (result.total_lines, result.accepted, result.invalids) == (1, 1, 0)
