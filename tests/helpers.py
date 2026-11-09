"""Shared helpers for busflow tests."""
import json
from pathlib import Path


def make_v1(event_id="E1", line="42", station="Central",
            board="2026-11-01 08:15:00", recv="2026-11-01 08:16:00", card="C1"):
    return {
        "event_id": event_id,
        "line": line,
        "station": station,
        "board_time": board,
        "receive_time": recv,
        "card_id": card,
    }


def make_v2(event_id="E2", line="42", station="Central",
            board="2026-11-01T08:15:00Z", recv="2026-11-01T08:16:00Z", card="C1"):
    return {
        "schema_version": 2,
        "id": event_id,
        "route": {"line": line, "station": station},
        "timestamps": {"boarded_at": board, "received_at": recv},
        "card": card,
    }


def write_jsonl(path, records):
    """Write records (dicts or raw strings) as JSONL; returns the Path."""
    lines = [r if isinstance(r, str) else json.dumps(r, ensure_ascii=False)
             for r in records]
    path = Path(path)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def stats_map(store):
    """Stats as {(date, line, hour): (swipes, unique_passengers, anomalies)}."""
    return {
        (r["event_date"], r["line"], r["hour"]):
            (r["swipes"], r["unique_passengers"], r["anomalies"])
        for r in store.read_stats()
    }


def event_tuples(store):
    """Canonical content of the event ledger (ignores bookkeeping columns)."""
    return sorted(
        (e["event_id"], e["line"], e["station"], e["board_time"],
         e["receive_time"], e["card_id"], e["event_date"])
        for e in store.ledger_events()
    )
