"""busflow: local bus card-swipe event processing.

Reads JSONL event files (two input versions), normalizes records into a
unified schema, writes date-partitioned Parquet, and builds line/hour
statistics with DuckDB. Imports are idempotent, conflicts and malformed
records are quarantined, and interrupted runs recover cleanly.
"""

__version__ = "0.1.0"
