"""Unified event schema, validation and input-version adapters.

Supported input versions
------------------------
v1 (flat, legacy)::

    {"event_id": "E1", "line": "42", "station": "Central",
     "board_time": "2026-11-01 08:15:00", "receive_time": "2026-11-01 08:16:10",
     "card_id": "C100"}

v2 (nested, ISO-8601)::

    {"schema_version": 2, "id": "E2",
     "route": {"line": "42", "station": "Central"},
     "timestamps": {"boarded_at": "2026-11-01T08:15:00Z",
                    "received_at": "2026-11-01T08:16:10Z"},
     "card": "C100"}

Both normalize to :class:`Event`. Timestamps carrying a UTC offset are
converted to naive UTC; naive timestamps are stored as-is.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


class ValidationError(Exception):
    """A record cannot be normalized; the message says why."""


@dataclass(frozen=True)
class Event:
    """The unified record format every input version is normalized into."""

    event_id: str
    line: str
    station: str
    board_time: datetime    # naive
    receive_time: datetime  # naive
    card_id: Optional[str] = None

    @property
    def event_date(self) -> str:
        """Partition date (YYYY-MM-DD), derived from the boarding time."""
        return self.board_time.date().isoformat()

    @property
    def hour(self) -> int:
        return self.board_time.hour

    @property
    def content_hash(self) -> str:
        """Fingerprint of the normalized content.

        Identical re-deliveries share the hash (dedupe); a different hash for
        the same event_id means a conflict.
        """
        payload = "|".join((
            self.event_id,
            self.line,
            self.station,
            self.board_time.isoformat(),
            self.receive_time.isoformat(),
            self.card_id or "",
        ))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_timestamp(value: Any) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM:SS' or ISO-8601 (with optional offset/'Z').

    Offset-aware values are converted to naive UTC. Raises ValueError.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("not a non-empty string")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _required_str(obj: dict, key: str, where: str = "") -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"missing or invalid required field '{where}{key}'")
    return value.strip()


def _optional_str(obj: dict, key: str, where: str = "") -> Optional[str]:
    value = obj.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(f"field '{where}{key}' must be a string")
    return value.strip() or None


def _required_ts(obj: dict, key: str, where: str = "") -> datetime:
    try:
        return parse_timestamp(obj.get(key))
    except ValueError:
        raise ValidationError(
            f"field '{where}{key}' is missing or not a valid timestamp"
        ) from None


def detect_version(obj: dict) -> int:
    if obj.get("schema_version") == 2 or ("route" in obj and "timestamps" in obj):
        return 2
    if "event_id" in obj:
        return 1
    raise ValidationError(
        "unrecognized record version (expected v1 'event_id' or "
        "v2 'schema_version': 2 / 'route'+'timestamps')"
    )


def normalize_record(obj: Any) -> Event:
    """Normalize a decoded JSON record of either version into an Event.

    Raises ValidationError on malformed content or missing required fields.
    """
    if not isinstance(obj, dict):
        raise ValidationError("record is not a JSON object")
    if detect_version(obj) == 2:
        return _from_v2(obj)
    return _from_v1(obj)


def _from_v1(obj: dict) -> Event:
    return Event(
        event_id=_required_str(obj, "event_id"),
        line=_required_str(obj, "line"),
        station=_required_str(obj, "station"),
        board_time=_required_ts(obj, "board_time"),
        receive_time=_required_ts(obj, "receive_time"),
        card_id=_optional_str(obj, "card_id"),
    )


def _from_v2(obj: dict) -> Event:
    route = obj.get("route")
    if not isinstance(route, dict):
        raise ValidationError("field 'route' must be an object")
    timestamps = obj.get("timestamps")
    if not isinstance(timestamps, dict):
        raise ValidationError("field 'timestamps' must be an object")
    return Event(
        event_id=_required_str(obj, "id"),
        line=_required_str(route, "line", "route."),
        station=_required_str(route, "station", "route."),
        board_time=_required_ts(timestamps, "boarded_at", "timestamps."),
        receive_time=_required_ts(timestamps, "received_at", "timestamps."),
        card_id=_optional_str(obj, "card"),
    )


def best_effort_fields(obj: Any) -> dict:
    """Extract whatever attribution fields we can from a bad record.

    Used to file quarantine entries under the right date/line/hour when the
    record itself cannot be fully validated.
    """
    hint = {"event_id": None, "line": None, "date": None, "hour": None}
    if not isinstance(obj, dict):
        return hint
    for key in ("event_id", "id"):
        if isinstance(obj.get(key), str) and obj[key].strip():
            hint["event_id"] = obj[key].strip()
            break
    line = obj.get("line")
    if line is None and isinstance(obj.get("route"), dict):
        line = obj["route"].get("line")
    if isinstance(line, str) and line.strip():
        hint["line"] = line.strip()
    board = obj.get("board_time")
    if board is None and isinstance(obj.get("timestamps"), dict):
        board = obj["timestamps"].get("boarded_at")
    try:
        dt = parse_timestamp(board)
    except ValueError:
        return hint
    hint["date"] = dt.date().isoformat()
    hint["hour"] = dt.hour
    return hint
