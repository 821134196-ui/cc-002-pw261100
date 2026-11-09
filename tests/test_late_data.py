"""Late-arriving events: incremental result must equal one-shot processing."""
from busflow import pipeline
from busflow.store import Store
from helpers import event_tuples, make_v1, make_v2, write_jsonl

DAY1 = [
    make_v1("E1", line="42", board="2026-11-01 08:15:00", card="C1"),
    make_v1("E2", line="42", board="2026-11-01 09:00:00", card="C2"),
]
LATE = [
    # late for a date that already has a partition
    make_v1("E3", line="42", board="2026-11-01 08:45:00",
            recv="2026-11-02 07:00:00", card="C1"),
    # late for a date that has no partition yet
    make_v1("E4", line="42", board="2026-10-31 23:50:00",
            recv="2026-11-02 07:00:00", card="C3"),
    make_v2("E5", line="7", board="2026-10-31T22:10:00Z",
            recv="2026-11-02T07:00:00Z", card="C4"),
]


def test_late_events_rebuild_affected_dates(tmp_path):
    incremental = Store(tmp_path / "inc")
    try:
        fa = write_jsonl(tmp_path / "day1.jsonl", DAY1)
        fb = write_jsonl(tmp_path / "late.jsonl", LATE)
        pipeline.ingest_file(incremental, fa)
        result = pipeline.ingest_file(incremental, fb)

        # both the existing-partition date and the brand-new date were rebuilt
        assert result.affected_dates == ["2026-10-31", "2026-11-01"]
        assert (incremental.events_dir / "date=2026-10-31"
                / "data.parquet").exists()

        # one-shot reference: everything imported in a single file
        reference = Store(tmp_path / "ref")
        try:
            pipeline.ingest_file(
                reference, write_jsonl(tmp_path / "all.jsonl", DAY1 + LATE))

            assert event_tuples(incremental) == event_tuples(reference)
            for date in ("2026-10-31", "2026-11-01"):
                assert (incremental.read_partition(date)
                        == reference.read_partition(date))
            assert incremental.read_stats() == reference.read_stats()
        finally:
            reference.close()
    finally:
        incremental.close()


def test_stats_after_late_arrival(tmp_path):
    store = Store(tmp_path / "data")
    try:
        pipeline.ingest_file(store, write_jsonl(tmp_path / "day1.jsonl", DAY1))
        pipeline.ingest_file(store, write_jsonl(tmp_path / "late.jsonl", LATE))
        stats = {
            (r["event_date"], r["line"], r["hour"]): r
            for r in store.read_stats()
        }
        # hour 8 on 11-01: E1 + E3, both card C1 -> 2 swipes, 1 passenger
        assert stats[("2026-11-01", "42", 8)]["swipes"] == 2
        assert stats[("2026-11-01", "42", 8)]["unique_passengers"] == 1
        assert stats[("2026-10-31", "42", 23)]["swipes"] == 1
        assert stats[("2026-10-31", "7", 22)]["swipes"] == 1
    finally:
        store.close()
