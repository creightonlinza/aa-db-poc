#!/usr/bin/env python3
import argparse
import gzip
import io
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import psycopg
import zstandard


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/analysis"
DEFAULT_BATCH_SIZE = 1000
FAST_BATCH_SIZE = 50_000
DEFAULT_GZIP_LEVEL = 6
FAST_GZIP_LEVEL = 1
SYNCHRONOUS_COMMIT_VALUES = {"on", "off", "local", "remote_write", "remote_apply"}


@dataclass
class FileStats:
    lines_seen: int = 0
    inserted: int = 0
    duplicates: int = 0
    bad_records: int = 0
    errors: int = 0


@dataclass
class RunStats:
    files_seen: int = 0
    files_with_errors: int = 0
    lines_seen: int = 0
    inserted: int = 0
    duplicates: int = 0
    bad_records: int = 0
    errors: int = 0


class ProgressReporter:
    def __init__(self, inline: bool | None = None) -> None:
        self._inline = sys.stdout.isatty() if inline is None else inline
        self._last_len = 0
        self._active = False

    def line(self, message: str) -> None:
        self.clear()
        print(message, flush=True)

    def progress(self, message: str) -> None:
        if not self._inline:
            print(message, flush=True)
            return

        padded = message.ljust(self._last_len)
        sys.stdout.write(f"\r{padded}")
        sys.stdout.flush()
        self._last_len = len(message)
        self._active = True

    def error(self, message: str) -> None:
        self.clear()
        print(message, file=sys.stderr, flush=True)

    def clear(self) -> None:
        if not self._inline or not self._active:
            return

        sys.stdout.write("\r" + (" " * self._last_len) + "\r")
        sys.stdout.flush()
        self._last_len = 0
        self._active = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream .zst-compressed JSONL analysis records into a lean Postgres lookup table."
    )
    parser.add_argument("input_path", help="Directory containing .zst files to ingest.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
        help="Postgres connection URL. Defaults to DATABASE_URL or local Docker Compose Postgres.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Use aggressive ingest defaults: all CPU workers, batch size 50000, "
            "gzip level 1, and synchronous_commit=off."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Rows per committed insert batch. Defaults to 1000, or 50000 with --fast.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Concurrent file ingest workers. Defaults to min(4, CPU count, source file count), "
            "or min(CPU count, source file count) with --fast."
        ),
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Print progress every N source lines per file. Use 0 to disable periodic progress.",
    )
    parser.add_argument(
        "--gzip-level",
        type=int,
        default=None,
        choices=range(0, 10),
        metavar="0-9",
        help="Per-record gzip compression level for stored payloads. Defaults to 6, or 1 with --fast.",
    )
    parser.add_argument(
        "--synchronous-commit",
        choices=sorted(SYNCHRONOUS_COMMIT_VALUES),
        default=None,
        help="Session synchronous_commit setting. Defaults to off with --fast, otherwise leaves Postgres default.",
    )
    return parser.parse_args()


def resolve_batch_size(requested_batch_size: int | None, fast: bool) -> int:
    if requested_batch_size is not None:
        return requested_batch_size
    return FAST_BATCH_SIZE if fast else DEFAULT_BATCH_SIZE


def resolve_gzip_level(requested_gzip_level: int | None, fast: bool) -> int:
    if requested_gzip_level is not None:
        return requested_gzip_level
    return FAST_GZIP_LEVEL if fast else DEFAULT_GZIP_LEVEL


def resolve_synchronous_commit(requested_value: str | None, fast: bool) -> str | None:
    if requested_value is not None:
        return requested_value
    return "off" if fast else None


def set_session_options(conn: psycopg.Connection, synchronous_commit: str | None) -> None:
    if synchronous_commit is None:
        return
    if synchronous_commit not in SYNCHRONOUS_COMMIT_VALUES:
        raise ValueError(f"Unsupported synchronous_commit value: {synchronous_commit}")
    with conn.cursor() as cur:
        cur.execute(f"SET synchronous_commit TO {synchronous_commit}")
    conn.commit()


def ensure_records_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_records (
              id text PRIMARY KEY,
              payload_gzip bytea NOT NULL
            )
            """
        )
    conn.commit()


def ensure_ingest_batch_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS ingest_batch (
              id text NOT NULL,
              payload_gzip bytea NOT NULL
            ) ON COMMIT DELETE ROWS
            """
        )
    conn.commit()


def ensure_database(conn: psycopg.Connection) -> None:
    ensure_records_table(conn)
    ensure_ingest_batch_table(conn)


def load_batch(conn: psycopg.Connection, batch: list[tuple[str, bytes]]) -> tuple[int, int]:
    if not batch:
        return 0, 0

    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE ingest_batch")
            with cur.copy("COPY ingest_batch (id, payload_gzip) FROM STDIN") as copy:
                for row in batch:
                    copy.write_row(row)
            cur.execute(
                """
                INSERT INTO analysis_records (id, payload_gzip)
                SELECT id, payload_gzip
                FROM ingest_batch
                ON CONFLICT (id) DO NOTHING
                """
            )
            inserted = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return inserted, len(batch) - inserted


def extract_track_id(record: object) -> str | None:
    if not isinstance(record, dict):
        return None
    meta = record.get("meta")
    if not isinstance(meta, dict):
        return None
    track_id = meta.get("track_id")
    if not isinstance(track_id, str) or not track_id:
        return None
    return track_id


def print_progress(
    reporter: ProgressReporter,
    worker_id: int | None,
    file_index: int,
    file_count: int,
    path: Path,
    stats: FileStats,
    pending_batch: int,
    started_at: float,
) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    rate = stats.lines_seen / elapsed
    worker = f"worker={worker_id} " if worker_id is not None else ""
    reporter.progress(
        "Progress "
        f"{worker}"
        f"file={file_index}/{file_count} "
        f"path={path} "
        f"line={stats.lines_seen} "
        f"inserted={stats.inserted} "
        f"duplicates={stats.duplicates} "
        f"pending_batch={pending_batch} "
        f"bad_records={stats.bad_records} "
        f"lines_per_sec={rate:.1f}",
    )


def process_record(
    reporter: ProgressReporter,
    path: Path,
    line_num: int,
    line_bytes: bytes,
    gzip_level: int,
) -> tuple[str, bytes] | None:
    payload = line_bytes.rstrip(b"\r\n")
    if not payload:
        return None

    try:
        record = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        reporter.error(f"Bad JSON skipped path={path} line={line_num}: {exc}")
        return None

    track_id = extract_track_id(record)
    if track_id is None:
        reporter.error(f"Missing meta.track_id skipped path={path} line={line_num}")
        return None

    return track_id, gzip.compress(payload, compresslevel=gzip_level, mtime=0)


def flush_batch(
    conn: psycopg.Connection,
    batch: list[tuple[str, bytes]],
    file_stats: FileStats,
) -> None:
    inserted, duplicates = load_batch(conn, batch)
    file_stats.inserted += inserted
    file_stats.duplicates += duplicates
    batch.clear()


def process_file(
    reporter: ProgressReporter,
    conn: psycopg.Connection,
    worker_id: int | None,
    path: Path,
    file_index: int,
    file_count: int,
    batch_size: int,
    progress_interval: int,
    gzip_level: int,
) -> FileStats:
    file_stats = FileStats()
    batch: list[tuple[str, bytes]] = []
    started_at = time.monotonic()

    worker = f" worker={worker_id}" if worker_id is not None else ""
    reporter.line(f"Processing file={file_index}/{file_count}{worker} path={path}")

    try:
        with path.open("rb") as compressed:
            dctx = zstandard.ZstdDecompressor()
            with dctx.stream_reader(compressed) as reader:
                buffered = io.BufferedReader(reader)
                while True:
                    line = buffered.readline()
                    if not line:
                        break

                    file_stats.lines_seen += 1
                    processed = process_record(reporter, path, file_stats.lines_seen, line, gzip_level)

                    if processed is None:
                        file_stats.bad_records += 1
                    else:
                        batch.append(processed)

                    if len(batch) >= batch_size:
                        flush_batch(conn, batch, file_stats)

                    if progress_interval and file_stats.lines_seen % progress_interval == 0:
                        print_progress(
                            reporter,
                            worker_id,
                            file_index,
                            file_count,
                            path,
                            file_stats,
                            len(batch),
                            started_at,
                        )
    except zstandard.ZstdError as exc:
        file_stats.errors += 1
        reporter.error(f"Zstandard error path={path} after_line={file_stats.lines_seen}: {exc}")

    if batch:
        flush_batch(conn, batch, file_stats)

    elapsed = max(time.monotonic() - started_at, 0.001)
    reporter.line(
        "Finished "
        f"{f'worker={worker_id} ' if worker_id is not None else ''}"
        f"file={file_index}/{file_count} "
        f"path={path} "
        f"lines={file_stats.lines_seen} "
        f"inserted={file_stats.inserted} "
        f"duplicates={file_stats.duplicates} "
        f"bad_records={file_stats.bad_records} "
        f"errors={file_stats.errors} "
        f"elapsed_sec={elapsed:.1f}",
    )
    return file_stats


def find_sources(sources_dir: Path) -> list[Path]:
    return sorted(path for path in sources_dir.glob("*.zst") if path.is_file())


def resolve_worker_count(requested_workers: int | None, source_count: int, fast: bool) -> int:
    if requested_workers is not None:
        return min(requested_workers, source_count)

    cpu_count = os.cpu_count() or 1
    default_limit = cpu_count if fast else min(4, cpu_count)
    return max(1, min(default_limit, source_count))


def add_file_stats(run_stats: RunStats, file_stats: FileStats) -> None:
    run_stats.lines_seen += file_stats.lines_seen
    run_stats.inserted += file_stats.inserted
    run_stats.duplicates += file_stats.duplicates
    run_stats.bad_records += file_stats.bad_records
    run_stats.errors += file_stats.errors
    if file_stats.errors:
        run_stats.files_with_errors += 1


def process_file_worker(
    database_url: str,
    path: Path,
    file_index: int,
    file_count: int,
    batch_size: int,
    progress_interval: int,
    gzip_level: int,
    synchronous_commit: str | None,
) -> tuple[int, FileStats]:
    reporter = ProgressReporter(inline=False)
    worker_id = os.getpid()
    with psycopg.connect(database_url) as conn:
        set_session_options(conn, synchronous_commit)
        ensure_ingest_batch_table(conn)
        file_stats = process_file(
            reporter=reporter,
            conn=conn,
            worker_id=worker_id,
            path=path,
            file_index=file_index,
            file_count=file_count,
            batch_size=batch_size,
            progress_interval=progress_interval,
            gzip_level=gzip_level,
        )
    return file_index, file_stats


def main() -> int:
    args = parse_args()
    batch_size = resolve_batch_size(args.batch_size, args.fast)
    gzip_level = resolve_gzip_level(args.gzip_level, args.fast)
    synchronous_commit = resolve_synchronous_commit(args.synchronous_commit, args.fast)

    if batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0")
    if args.workers is not None and args.workers <= 0:
        raise SystemExit("--workers must be greater than 0")
    if args.progress_interval < 0:
        raise SystemExit("--progress-interval must be 0 or greater")

    sources_dir = Path(args.input_path)
    sources = find_sources(sources_dir)
    if not sources:
        print(f"No .zst files found in {sources_dir}", flush=True)
        return 0

    run_stats = RunStats(files_seen=len(sources))
    worker_count = resolve_worker_count(args.workers, len(sources), args.fast)
    reporter = ProgressReporter(inline=worker_count == 1)
    started_at = time.monotonic()

    reporter.line(
        "Ingest starting "
        f"files={len(sources)} "
        f"workers={worker_count} "
        f"batch_size={batch_size} "
        f"gzip_level={gzip_level} "
        f"synchronous_commit={synchronous_commit or 'default'} "
        f"fast={args.fast}"
    )

    if worker_count == 1:
        with psycopg.connect(args.database_url) as conn:
            set_session_options(conn, synchronous_commit)
            ensure_database(conn)
            for file_index, path in enumerate(sources, start=1):
                file_stats = process_file(
                    reporter=reporter,
                    conn=conn,
                    worker_id=None,
                    path=path,
                    file_index=file_index,
                    file_count=len(sources),
                    batch_size=batch_size,
                    progress_interval=args.progress_interval,
                    gzip_level=gzip_level,
                )
                add_file_stats(run_stats, file_stats)
    else:
        with psycopg.connect(args.database_url) as conn:
            ensure_records_table(conn)

        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            futures = []
            for file_index, path in enumerate(sources, start=1):
                futures.append(
                    executor.submit(
                        process_file_worker,
                        args.database_url,
                        path,
                        file_index,
                        len(sources),
                        batch_size,
                        args.progress_interval,
                        gzip_level,
                        synchronous_commit,
                    )
                )

            for future in as_completed(futures):
                _file_index, file_stats = future.result()
                add_file_stats(run_stats, file_stats)

    elapsed = max(time.monotonic() - started_at, 0.001)
    reporter.line(
        "Run complete "
        f"files={run_stats.files_seen} "
        f"files_with_errors={run_stats.files_with_errors} "
        f"lines={run_stats.lines_seen} "
        f"inserted={run_stats.inserted} "
        f"duplicates={run_stats.duplicates} "
        f"bad_records={run_stats.bad_records} "
        f"errors={run_stats.errors} "
        f"elapsed_sec={elapsed:.1f}",
    )
    return 1 if run_stats.errors or run_stats.bad_records else 0


if __name__ == "__main__":
    raise SystemExit(main())
