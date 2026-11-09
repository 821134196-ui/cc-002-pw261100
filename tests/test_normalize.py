"""Version conversion and validation tests."""
from datetime import datetime

import pytest

from busflow.schema import (
    ValidationError,
    best_effort_fields,
    detect_version,
    normalize_record,
)
from helpers import make_v1, make_v2


def test_v1_normalizes_to_unified_schema():
    ev = normalize_record(make_v1(
        event_id="E1", line="42", station="Central",
        board="2026-11-01 08:15:00", recv="2026-11-01 08:16:10", card="C100",
    ))
    assert ev.event_id == "E1"
    assert ev.line == "42"
    assert ev.station == "Central"
    assert ev.board_time == datetime(2026, 11, 1, 8, 15, 0)
    assert ev.receive_time == datetime(2026, 11, 1, 8, 16, 10)
    assert ev.card_id == "C100"
    assert ev.event_date == "2026-11-01"
    assert ev.hour == 8


def test_v2_normalizes_to_unified_schema():
    ev = normalize_record(make_v2(
        event_id="E2", line="7", station="Museum",
        board="2026-11-01T09:05:00Z", recv="2026-11-01T09:06:10Z", card="C9",
    ))
    assert ev.event_id == "E2"
    assert ev.line == "7"
    assert ev.station == "Museum"
    assert ev.board_time == datetime(2026, 11, 1, 9, 5, 0)
    assert ev.card_id == "C9"


def test_v2_offsets_are_converted_to_naive_utc():
    ev = normalize_record(make_v2(board="2026-11-01T10:15:00+02:00",
                                  recv="2026-11-01T10:16:00+02:00"))
    assert ev.board_time == datetime(2026, 11, 1, 8, 15, 0)
    assert ev.receive_time == datetime(2026, 11, 1, 8, 16, 0)


def test_equivalent_v1_and_v2_share_content_hash():
    """The same event delivered in both versions dedupes as identical."""
    v1 = normalize_record(make_v1(event_id="E1", line="42", station="Central",
                                  board="2026-11-01 08:15:00",
                                  recv="2026-11-01 08:16:00", card="C1"))
    v2 = normalize_record(make_v2(event_id="E1", line="42", station="Central",
                                  board="2026-11-01T08:15:00Z",
                                  recv="2026-11-01T08:16:00Z", card="C1"))
    assert v1.content_hash == v2.content_hash


def test_version_detection():
    assert detect_version(make_v1()) == 1
    assert detect_version(make_v2()) == 2
    # v2 without explicit schema_version is still recognized by its shape
    rec = make_v2()
    del rec["schema_version"]
    assert detect_version(rec) == 2


@pytest.mark.parametrize("record,needle", [
    ({"line": "42"}, "version"),                       # no recognizable shape
    (make_v1(event_id=""), "event_id"),                # empty required field
    (make_v1(line=None), "'line'"),                    # missing line
    (make_v1(station=123), "station"),                 # wrong type
    (make_v1(board="not-a-time"), "board_time"),       # bad timestamp
    (make_v1(recv=None), "receive_time"),              # missing receive_time
    (make_v2(event_id="E1", board=None), "boarded_at"),
    ({"schema_version": 2, "id": "E1", "route": "42"}, "route"),
    (["not", "an", "object"], "not a JSON object"),
])
def test_invalid_records_raise_validation_error(record, needle):
    with pytest.raises(ValidationError) as exc_info:
        normalize_record(record)
    assert needle in str(exc_info.value)


def test_best_effort_fields_for_quarantine_attribution():
    hint = best_effort_fields({"event_id": "E9", "line": "7",
                               "board_time": "2026-11-01 08:30:00"})
    assert hint == {"event_id": "E9", "line": "7",
                    "date": "2026-11-01", "hour": 8}
    # v2 shape is understood too
    hint = best_effort_fields(make_v2(event_id="E9", line="7",
                                      board="2026-11-01T08:30:00Z"))
    assert hint["event_id"] == "E9" and hint["line"] == "7" and hint["hour"] == 8
    # unparseable input degrades to nulls instead of raising
    assert best_effort_fields("garbage") == {
        "event_id": None, "line": None, "date": None, "hour": None}
