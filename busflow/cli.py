"""Command line interface.

    python -m busflow --data-dir DIR ingest FILE...
    python -m busflow --data-dir DIR status
    python -m busflow --data-dir DIR stats [--date YYYY-MM-DD]
    python -m busflow --data-dir DIR recompute (--all | --date YYYY-MM-DD ...)
    python -m busflow --data-dir DIR reprocess-quarantine [--strategy S]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import pipeline
from .store import Store


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="busflow",
        description="Local bus card-swipe event processor "
                    "(JSONL -> date-partitioned Parquet + DuckDB stats)",
    )
    parser.add_argument("--data-dir", default="busflow_data",
                        help="data directory (default: ./busflow_data)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="import JSONL files (idempotent)")
    p_ingest.add_argument("files", nargs="+", type=Path)

    sub.add_parser("status", help="show processing status")

    p_stats = sub.add_parser("stats", help="print line/hour statistics")
    p_stats.add_argument("--date", help="only show this date (YYYY-MM-DD)")

    p_recompute = sub.add_parser(
        "recompute", help="rebuild partitions and statistics from the ledger")
    group = p_recompute.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", action="append", metavar="YYYY-MM-DD",
                       help="date to rebuild (repeatable)")
    group.add_argument("--all", action="store_true", help="rebuild all dates")

    p_reproc = sub.add_parser("reprocess-quarantine",
                              help="retry quarantined records")
    p_reproc.add_argument(
        "--strategy", choices=["keep-existing", "use-quarantined", "drop"],
        default="keep-existing",
        help="how to resolve conflicts (default: keep-existing)")

    args = parser.parse_args(argv)
    store = Store(args.data_dir)
    try:
        if args.command == "ingest":
            return _cmd_ingest(store, args.files)
        if args.command == "status":
            store.recover()
            _print_status(store)
            return 0
        if args.command == "stats":
            store.recover()
            _print_stats(store, args.date)
            return 0
        if args.command == "recompute":
            dates = pipeline.recompute(store, None if args.all else args.date)
            print(f"recomputed {len(dates)} date(s): {', '.join(dates) or '-'}")
            return 0
        if args.command == "reprocess-quarantine":
            result = pipeline.reprocess_quarantine(store, args.strategy)
            print(f"examined {result.examined} quarantined record(s): "
                  f"{result.ingested} ingested, {result.duplicates} duplicates, "
                  f"{result.dropped} dropped, {result.still_pending} still pending")
            if result.affected_dates:
                print(f"rebuilt dates: {', '.join(result.affected_dates)}")
            return 0
    finally:
        store.close()
    return 2  # unreachable


def _cmd_ingest(store: Store, files) -> int:
    rc = 0
    for path in files:
        if not path.is_file():
            print(f"ERROR: {path}: no such file", file=sys.stderr)
            rc = 1
            continue
        r = pipeline.ingest_file(store, path)
        if r.status == "skipped_duplicate":
            print(f"{path}: skipped (identical file already imported)")
        else:
            dates = ", ".join(r.affected_dates) or "-"
            print(f"{path}: {r.total_lines} lines -> {r.accepted} accepted, "
                  f"{r.duplicates} duplicates, {r.conflicts} conflicts, "
                  f"{r.invalids} invalid; dates: {dates}")
    return rc


def _print_status(store: Store) -> None:
    s = store.status_summary()
    print(f"Data dir: {s['data_dir']}")
    print(f"Stats:    {'STALE (refresh pending)' if s['stats_dirty'] else 'up to date'}"
          f"  ({s['stats_file']})")
    print(f"\nImports ({len(s['imports'])}):")
    if not s["imports"]:
        print("  (none)")
    for imp in s["imports"]:
        print(f"  #{imp['id']} {imp['file_path']}")
        print(f"      status={imp['status']} lines={imp['total_lines']} "
              f"ok={imp['accepted']} dup={imp['duplicates']} "
              f"conflict={imp['conflicts']} invalid={imp['invalids']} "
              f"hash={imp['file_hash'][:12]}...")
    print(f"\nEvents: {s['events_total']} total across "
          f"{len(s['events_per_date'])} date(s)")
    for date, count in s["events_per_date"]:
        print(f"  {date}: {count}")
    by_status = s["quarantine_by_status"]
    pending = by_status.get("pending", 0)
    resolved = sum(c for st, c in by_status.items() if st != "pending")
    print(f"\nQuarantine: {pending} pending, {resolved} resolved")
    for reason, count in s["quarantine_pending_reasons"]:
        print(f"  [{count}] {reason}")


def _print_stats(store: Store, date) -> None:
    rows = store.read_stats(date)
    if not rows:
        print("(no statistics yet)")
        return
    header = ("date", "line", "hour", "swipes", "unique_passengers", "anomalies")
    print("%-11s %-8s %4s %7s %18s %9s" % header)
    for r in rows:
        print("%-11s %-8s %4s %7s %18s %9s" % (
            r["event_date"], r["line"], r["hour"],
            r["swipes"], r["unique_passengers"], r["anomalies"],
        ))


if __name__ == "__main__":
    sys.exit(main())
