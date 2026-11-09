"""Storage layer: SQLite ledger + date-partitioned Parquet + crash recovery.

Data directory layout::

    <data_dir>/
      ledger.db                            SQLite ledger (source of truth)
      events/date=YYYY-MM-DD/data.parquet  one file per partition date
      stats/line_hourly.parquet            DuckDB-built statistics

The ledger holds the canonical event set, the quarantine area and the import
journal. Parquet partitions and statistics are derived artifacts: they are
always rebuilt from the ledger (see `rebuild_partitions` / `refresh_stats`),
which is what makes late data and reprocessing produce results identical to a
one-shot load.

Crash safety
------------
* Every derived file is written to a temporary file and atomically renamed
  into place, so a crash never leaves a half-written partition visible.
* The import journal (`imports` table) tracks how far each import got:
  ``started`` -> ``events_committed`` -> ``committed``. `recover()` rolls back
  imports that never applied their events, finishes the partition/stat rebuild
  for those that did, and removes leftover temporary files.
* A ``stats_dirty`` flag in the meta table guarantees statistics are refreshed
  even if the process died between the partition swap and the stats build.
"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import duckdb

from .schema import Event

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    content_hash  TEXT NOT NULL,
    line          TEXT NOT NULL,
    station       TEXT NOT NULL,
    board_time    TEXT NOT NULL,
    receive_time  TEXT NOT NULL,
    card_id       TEXT,
    event_date    TEXT NOT NULL,
    source_file   TEXT,
    first_seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    raw         TEXT NOT NULL,
    event_id    TEXT,
    ex_line     TEXT,
    ex_date     TEXT,
    ex_hour     INTEGER,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TEXT NOT NULL,
    resolved_at TEXT
);
CREATE TABLE IF NOT EXISTS imports (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path      TEXT NOT NULL,
    file_hash      TEXT NOT NULL,
    status         TEXT NOT NULL,  -- started | events_committed | committed
    total_lines    INTEGER,
    accepted       INTEGER,
    duplicates     INTEGER,
    conflicts      INTEGER,
    invalids       INTEGER,
    affected_dates TEXT,           -- JSON array of partition dates
    started_at     TEXT NOT NULL,
    committed_at   TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

EVENT_INSERT = """
INSERT INTO events (event_id, content_hash, line, station, board_time,
                    receive_time, card_id, event_date, source_file, first_seen_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

QUARANTINE_INSERT = """
INSERT INTO quarantine (source_file, line_number, reason, raw, event_id,
                        ex_line, ex_date, ex_hour, status, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

EVENT_PARQUET_SCHEMA = (
    "event_id VARCHAR, line VARCHAR, station VARCHAR, board_time VARCHAR, "
    "receive_time VARCHAR, card_id VARCHAR, event_date VARCHAR"
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.events_dir = self.root / "events"
        self.stats_dir = self.root / "stats"
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.root / "ledger.db"))
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA_SQL)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # low-level helpers
    # ------------------------------------------------------------------
    @contextmanager
    def transaction(self):
        try:
            yield
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def _get_meta(self, key: str) -> Optional[str]:
        row = self.db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        self.db.commit()

    # ------------------------------------------------------------------
    # import journal
    # ------------------------------------------------------------------
    def begin_import(self, file_path: str, file_hash: str) -> int:
        with self.transaction():
            cur = self.db.execute(
                "INSERT INTO imports (file_path, file_hash, status, started_at) "
                "VALUES (?, ?, 'started', ?)",
                (file_path, file_hash, _utcnow()),
            )
        return cur.lastrowid

    def committed_import_by_hash(self, file_hash: str) -> Optional[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM imports WHERE file_hash = ? AND status = 'committed'",
            (file_hash,),
        ).fetchone()

    def mark_committed(self, import_id: int) -> None:
        with self.transaction():
            self.db.execute(
                "UPDATE imports SET status = 'committed', committed_at = ? WHERE id = ?",
                (_utcnow(), import_id),
            )

    def imports(self) -> List[sqlite3.Row]:
        return self.db.execute("SELECT * FROM imports ORDER BY id").fetchall()

    # ------------------------------------------------------------------
    # events
    # ------------------------------------------------------------------
    def classify(self, parsed):
        """Split parsed lines into (new, duplicate_count, conflicts).

        ``parsed`` items must carry a ``.event`` attribute. Dedupe key is the
        event_id: identical content hash -> duplicate; different content hash
        -> conflict. First occurrence within the batch wins.
        """
        new, conflicts = [], []
        n_duplicates = 0
        batch_hashes: Dict[str, str] = {}
        for item in parsed:
            ev = item.event
            content_hash = ev.content_hash
            existing = batch_hashes.get(ev.event_id)
            if existing is None:
                row = self.db.execute(
                    "SELECT content_hash FROM events WHERE event_id = ?",
                    (ev.event_id,),
                ).fetchone()
                existing = row["content_hash"] if row else None
            if existing is None:
                batch_hashes[ev.event_id] = content_hash
                new.append(item)
            elif existing == content_hash:
                n_duplicates += 1
            else:
                conflicts.append(item)
        return new, n_duplicates, conflicts

    def apply_import(self, import_id, source_file, new_parsed, conflict_parsed,
                     bad_lines, counts) -> None:
        """Atomically apply one import's events + quarantine + journal update."""
        now = _utcnow()
        with self.transaction():
            self.db.executemany(
                EVENT_INSERT,
                [
                    (p.event.event_id, p.event.content_hash, p.event.line,
                     p.event.station, p.event.board_time.isoformat(),
                     p.event.receive_time.isoformat(), p.event.card_id,
                     p.event.event_date, source_file, now)
                    for p in new_parsed
                ],
            )
            quarantine_rows = []
            for p in conflict_parsed:
                ev = p.event
                quarantine_rows.append((
                    source_file, p.line_number,
                    f"conflict: event_id '{ev.event_id}' already exists with "
                    f"different content",
                    p.raw, ev.event_id, ev.line, ev.event_date, ev.hour,
                    "pending", now,
                ))
            for b in bad_lines:
                quarantine_rows.append((
                    source_file, b.line_number, b.reason, b.raw,
                    b.hint.get("event_id"), b.hint.get("line"),
                    b.hint.get("date"), b.hint.get("hour"),
                    "pending", now,
                ))
            self.db.executemany(QUARANTINE_INSERT, quarantine_rows)
            self.db.execute(
                "UPDATE imports SET status = 'events_committed', total_lines = ?, "
                "accepted = ?, duplicates = ?, conflicts = ?, invalids = ?, "
                "affected_dates = ? WHERE id = ?",
                (counts["total"], counts["accepted"], counts["duplicates"],
                 counts["conflicts"], counts["invalids"],
                 json.dumps(counts["affected_dates"]), import_id),
            )

    def get_event(self, event_id: str) -> Optional[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()

    def insert_event(self, ev: Event, source_file: str) -> None:
        self.db.execute(
            EVENT_INSERT,
            (ev.event_id, ev.content_hash, ev.line, ev.station,
             ev.board_time.isoformat(), ev.receive_time.isoformat(),
             ev.card_id, ev.event_date, source_file, _utcnow()),
        )

    def replace_event(self, ev: Event, source_file: str) -> None:
        self.db.execute(
            "UPDATE events SET content_hash = ?, line = ?, station = ?, "
            "board_time = ?, receive_time = ?, card_id = ?, event_date = ?, "
            "source_file = ? WHERE event_id = ?",
            (ev.content_hash, ev.line, ev.station, ev.board_time.isoformat(),
             ev.receive_time.isoformat(), ev.card_id, ev.event_date,
             source_file, ev.event_id),
        )

    def events_count(self) -> int:
        return self.db.execute("SELECT count(*) AS c FROM events").fetchone()["c"]

    def ledger_events(self) -> List[sqlite3.Row]:
        return self.db.execute("SELECT * FROM events ORDER BY event_id").fetchall()

    def all_event_dates(self) -> List[str]:
        rows = self.db.execute(
            "SELECT DISTINCT event_date FROM events ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # quarantine
    # ------------------------------------------------------------------
    def quarantine_rows(self, status: Optional[str] = None) -> List[sqlite3.Row]:
        if status is None:
            return self.db.execute("SELECT * FROM quarantine ORDER BY id").fetchall()
        return self.db.execute(
            "SELECT * FROM quarantine WHERE status = ? ORDER BY id", (status,)
        ).fetchall()

    def pending_quarantine_rows(self) -> List[sqlite3.Row]:
        return self.quarantine_rows(status="pending")

    def resolve_quarantine(self, quarantine_id: int, resolution: str) -> None:
        self.db.execute(
            "UPDATE quarantine SET status = ?, resolved_at = ? WHERE id = ?",
            (resolution, _utcnow(), quarantine_id),
        )

    # ------------------------------------------------------------------
    # derived artifact: date-partitioned parquet
    # ------------------------------------------------------------------
    def rebuild_partitions(self, dates: Iterable[str]) -> None:
        """Rebuild the parquet partition of each date from the ledger.

        Each partition is written to a temp file and atomically renamed, so
        readers never see a half-written partition.
        """
        for date in sorted(set(dates)):
            rows = self.db.execute(
                "SELECT event_id, line, station, board_time, receive_time, "
                "card_id, event_date FROM events WHERE event_date = ? "
                "ORDER BY board_time, event_id",
                (date,),
            ).fetchall()
            part_dir = self.events_dir / f"date={date}"
            final = part_dir / "data.parquet"
            if not rows:
                if final.exists():
                    final.unlink()
                if part_dir.is_dir():
                    try:
                        part_dir.rmdir()
                    except OSError:
                        pass
                continue
            part_dir.mkdir(parents=True, exist_ok=True)
            tmp = part_dir / f".tmp-{uuid.uuid4().hex}.parquet"
            self._write_events_parquet([tuple(r) for r in rows], tmp)
            os.replace(tmp, final)

    @staticmethod
    def _write_events_parquet(rows, path: Path) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute(f"CREATE TABLE data ({EVENT_PARQUET_SCHEMA})")
            if rows:
                con.executemany("INSERT INTO data VALUES (?,?,?,?,?,?,?)", rows)
            con.execute(f"COPY data TO {_sql_literal(str(path))} (FORMAT PARQUET)")
        finally:
            con.close()

    # ------------------------------------------------------------------
    # derived artifact: DuckDB statistics
    # ------------------------------------------------------------------
    def refresh_stats(self) -> None:
        """Rebuild line/hour statistics from partitions + quarantine.

        Full refresh, so the result always equals a one-shot computation over
        the current ledger. Written atomically; the stats_dirty flag covers
        crashes between partition swap and stats build.
        """
        self._set_meta("stats_dirty", "1")
        tmp = self.stats_dir / f".tmp-{uuid.uuid4().hex}.parquet"
        final = self.stats_dir / "line_hourly.parquet"
        files = sorted(self.events_dir.glob("date=*/data.parquet"))
        pending = self.db.execute(
            "SELECT ex_date, ex_line, ex_hour FROM quarantine WHERE status = 'pending'"
        ).fetchall()
        con = duckdb.connect(":memory:")
        try:
            con.execute(
                "CREATE TEMP TABLE anomalies (ex_date VARCHAR, ex_line VARCHAR, "
                "ex_hour INTEGER)"
            )
            if pending:
                con.executemany(
                    "INSERT INTO anomalies VALUES (?,?,?)",
                    [(r["ex_date"], r["ex_line"], r["ex_hour"]) for r in pending],
                )
            if files:
                file_list = ", ".join(_sql_literal(str(f)) for f in files)
                events_sql = (
                    f"SELECT event_date, line, "
                    f"hour(CAST(board_time AS TIMESTAMP)) AS hour, "
                    f"coalesce(card_id, event_id) AS passenger "
                    f"FROM read_parquet([{file_list}], hive_partitioning = false)"
                )
            else:
                events_sql = (
                    "SELECT CAST(NULL AS VARCHAR) AS event_date, "
                    "CAST(NULL AS VARCHAR) AS line, "
                    "CAST(NULL AS INTEGER) AS hour, "
                    "CAST(NULL AS VARCHAR) AS passenger WHERE false"
                )
            query = f"""
                WITH ev AS ({events_sql}),
                agg AS (
                    SELECT event_date, line, hour,
                           count(*) AS swipes,
                           count(DISTINCT passenger) AS unique_passengers
                    FROM ev GROUP BY 1, 2, 3
                ),
                qa AS (
                    SELECT ex_date, ex_line, ex_hour, count(*) AS anomalies
                    FROM anomalies GROUP BY 1, 2, 3
                )
                SELECT coalesce(a.event_date, q.ex_date) AS event_date,
                       coalesce(a.line, q.ex_line)       AS line,
                       coalesce(a.hour, q.ex_hour)       AS hour,
                       coalesce(a.swipes, 0)::BIGINT            AS swipes,
                       coalesce(a.unique_passengers, 0)::BIGINT AS unique_passengers,
                       coalesce(q.anomalies, 0)::BIGINT         AS anomalies
                FROM agg a
                FULL OUTER JOIN qa q
                  ON a.event_date IS NOT DISTINCT FROM q.ex_date
                 AND a.line       IS NOT DISTINCT FROM q.ex_line
                 AND a.hour       IS NOT DISTINCT FROM q.ex_hour
                ORDER BY 1, 2, 3
            """
            con.execute(f"COPY ({query}) TO {_sql_literal(str(tmp))} (FORMAT PARQUET)")
        finally:
            con.close()
        os.replace(tmp, final)
        self._set_meta("stats_dirty", "0")

    def read_stats(self, date: Optional[str] = None) -> List[dict]:
        final = self.stats_dir / "line_hourly.parquet"
        if not final.exists():
            return []
        con = duckdb.connect(":memory:")
        try:
            source = f"read_parquet({_sql_literal(str(final))})"
            if date is None:
                cur = con.execute(f"SELECT * FROM {source} ORDER BY 1, 2, 3")
            else:
                cur = con.execute(
                    f"SELECT * FROM {source} WHERE event_date = ? ORDER BY 1, 2, 3",
                    [date],
                )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            con.close()

    def read_partition(self, date: str) -> List[dict]:
        path = self.events_dir / f"date={date}" / "data.parquet"
        if not path.exists():
            return []
        con = duckdb.connect(":memory:")
        try:
            cur = con.execute(
                f"SELECT * FROM read_parquet({_sql_literal(str(path))}) "
                f"ORDER BY board_time, event_id"
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            con.close()

    # ------------------------------------------------------------------
    # crash recovery
    # ------------------------------------------------------------------
    def recover(self) -> None:
        """Bring the store to a clean state after an interrupted run.

        Idempotent; called at the start of every pipeline operation.
        """
        # 1. leftover temp files from crashed atomic writes
        for pattern_dir in (self.events_dir, self.stats_dir):
            for tmp in pattern_dir.glob("**/.tmp-*.parquet"):
                tmp.unlink()
        # 2. imports that never applied their events -> roll back the journal
        with self.transaction():
            self.db.execute("DELETE FROM imports WHERE status = 'started'")
        # 3. imports whose events were applied but partitions/stats maybe not
        stuck = self.db.execute(
            "SELECT id, affected_dates FROM imports "
            "WHERE status = 'events_committed'"
        ).fetchall()
        for row in stuck:
            self.rebuild_partitions(json.loads(row["affected_dates"] or "[]"))
            self.mark_committed(row["id"])
        if stuck:
            self._set_meta("stats_dirty", "1")
        # 4. statistics older than the partitions -> rebuild
        if self._get_meta("stats_dirty") == "1":
            self.refresh_stats()

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------
    def status_summary(self) -> dict:
        per_date = self.db.execute(
            "SELECT event_date, count(*) AS c FROM events GROUP BY 1 ORDER BY 1"
        ).fetchall()
        q_status = self.db.execute(
            "SELECT status, count(*) AS c FROM quarantine GROUP BY status"
        ).fetchall()
        q_reasons = self.db.execute(
            "SELECT reason, count(*) AS c FROM quarantine "
            "WHERE status = 'pending' GROUP BY reason ORDER BY 2 DESC"
        ).fetchall()
        return {
            "data_dir": str(self.root),
            "imports": [dict(r) for r in self.imports()],
            "events_total": self.events_count(),
            "events_per_date": [(r["event_date"], r["c"]) for r in per_date],
            "quarantine_by_status": {r["status"]: r["c"] for r in q_status},
            "quarantine_pending_reasons": [(r["reason"], r["c"]) for r in q_reasons],
            "stats_dirty": self._get_meta("stats_dirty") == "1",
            "stats_file": str(self.stats_dir / "line_hourly.parquet"),
        }
