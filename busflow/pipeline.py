"""Ingest / reprocess / recompute orchestration."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

from .schema import ValidationError, best_effort_fields, normalize_record
from .store import Store


@dataclass
class ParsedLine:
    line_number: int
    raw: str
    event: object  # schema.Event


@dataclass
class BadLine:
    line_number: int
    raw: str
    reason: str
    hint: dict  # best-effort attribution fields for quarantine


@dataclass
class IngestResult:
    file: str
    file_hash: str
    status: str  # "committed" | "skipped_duplicate"
    total_lines: int = 0
    accepted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    invalids: int = 0
    affected_dates: List[str] = field(default_factory=list)


@dataclass
class ReprocessResult:
    examined: int = 0
    ingested: int = 0
    duplicates: int = 0
    dropped: int = 0
    still_pending: int = 0
    affected_dates: List[str] = field(default_factory=list)


def parse_jsonl(text: str):
    """Parse JSONL text into (total_records, valid ParsedLines, BadLines)."""
    valid: List[ParsedLine] = []
    bad: List[BadLine] = []
    total = 0
    for line_number, raw in enumerate(text.splitlines(), start=1):
        if not raw.strip():
            continue  # blank lines are ignored, not quarantined
        total += 1
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            bad.append(BadLine(line_number, raw, f"invalid JSON: {exc.msg}",
                               best_effort_fields(None)))
            continue
        try:
            event = normalize_record(obj)
        except ValidationError as exc:
            bad.append(BadLine(line_number, raw, str(exc), best_effort_fields(obj)))
            continue
        valid.append(ParsedLine(line_number, raw, event))
    return total, valid, bad


def ingest_file(store: Store, path: Union[str, Path]) -> IngestResult:
    """Import one JSONL file. Idempotent per file content and per event_id."""
    path = Path(path)
    store.recover()
    raw_bytes = path.read_bytes()
    file_hash = hashlib.sha256(raw_bytes).hexdigest()

    previous = store.committed_import_by_hash(file_hash)
    if previous is not None:
        return IngestResult(
            file=str(path), file_hash=file_hash, status="skipped_duplicate",
            total_lines=previous["total_lines"] or 0,
            accepted=previous["accepted"] or 0,
            duplicates=previous["duplicates"] or 0,
            conflicts=previous["conflicts"] or 0,
            invalids=previous["invalids"] or 0,
            affected_dates=json.loads(previous["affected_dates"] or "[]"),
        )

    import_id = store.begin_import(str(path), file_hash)
    total, valid, bad = parse_jsonl(raw_bytes.decode("utf-8", errors="replace"))
    new, n_duplicates, conflicts = store.classify(valid)
    affected = sorted({p.event.event_date for p in new})
    store.apply_import(
        import_id, str(path), new, conflicts, bad,
        counts={
            "total": total, "accepted": len(new), "duplicates": n_duplicates,
            "conflicts": len(conflicts), "invalids": len(bad),
            "affected_dates": affected,
        },
    )
    # A crash between here and mark_committed is finished by recover().
    store.rebuild_partitions(affected)
    store.mark_committed(import_id)
    store.refresh_stats()
    return IngestResult(
        file=str(path), file_hash=file_hash, status="committed",
        total_lines=total, accepted=len(new), duplicates=n_duplicates,
        conflicts=len(conflicts), invalids=len(bad), affected_dates=affected,
    )


def reprocess_quarantine(store: Store, strategy: str = "keep-existing") -> ReprocessResult:
    """Retry every pending quarantine record.

    * still malformed            -> stays pending
    * now valid, unknown id      -> ingested
    * now valid, identical twin  -> resolved as duplicate
    * conflicting content        -> resolved per ``strategy``:
      ``keep-existing`` (default) leaves it pending, ``use-quarantined``
      replaces the stored event with the quarantined version, ``drop``
      discards the quarantined record.
    """
    if strategy not in ("keep-existing", "use-quarantined", "drop"):
        raise ValueError(f"unknown strategy: {strategy!r}")
    store.recover()
    rows = store.pending_quarantine_rows()
    result = ReprocessResult(examined=len(rows))
    affected = set()
    with store.transaction():
        for row in rows:
            try:
                event = normalize_record(json.loads(row["raw"]))
            except (json.JSONDecodeError, ValidationError):
                result.still_pending += 1
                continue
            existing = store.get_event(event.event_id)
            if existing is None:
                store.insert_event(event, row["source_file"])
                store.resolve_quarantine(row["id"], "ingested")
                affected.add(event.event_date)
                result.ingested += 1
            elif existing["content_hash"] == event.content_hash:
                store.resolve_quarantine(row["id"], "duplicate")
                result.duplicates += 1
            elif strategy == "use-quarantined":
                affected.add(existing["event_date"])
                affected.add(event.event_date)
                store.replace_event(event, row["source_file"])
                store.resolve_quarantine(row["id"], "ingested")
                result.ingested += 1
            elif strategy == "drop":
                store.resolve_quarantine(row["id"], "dropped")
                result.dropped += 1
            else:  # keep-existing
                result.still_pending += 1
    if affected:
        store.rebuild_partitions(sorted(affected))
    # quarantine statuses feed the anomaly counts, so stats always refresh
    store.refresh_stats()
    result.affected_dates = sorted(affected)
    return result


def recompute(store: Store, dates: Optional[List[str]] = None) -> List[str]:
    """Rebuild partitions (given dates, or all) and statistics from the ledger."""
    store.recover()
    dates = sorted(dates) if dates else store.all_event_dates()
    store.rebuild_partitions(dates)
    store.refresh_stats()
    return dates
