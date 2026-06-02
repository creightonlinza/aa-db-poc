# Analysis Record Lookup POC

This POC streams `.zst` files containing JSONL records from a user-supplied directory, extracts each record ID from `meta.track_id`, stores the original JSON line as gzipped bytes in Postgres, and serves records by ID through a small FastAPI app.

The source files are never modified and decompressed data is never written to disk.

## Setup

Start local Postgres. Docker Compose creates the `aa-db-poc_postgres_data` volume automatically.

```sh
docker compose up -d
```

Create a virtualenv and install dependencies:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Ingest Records

```sh
.venv/bin/python ingest.py /path/to/jsonl-zst-files
```

By default the ingester and API use:

```text
postgresql://postgres:postgres@localhost:5432/analysis
```

Override it with `DATABASE_URL` or `--database-url`.

Useful options:

```sh
.venv/bin/python ingest.py /path/to/jsonl-zst-files --workers 8 --batch-size 5000 --progress-interval 10000 --gzip-level 6
```

For a dedicated ingest machine, set concurrency and batch size explicitly. On a machine with 700GB
RAM, the main place that memory helps is allowing larger batches without paging:

```sh
.venv/bin/python ingest.py /path/to/jsonl-zst-files --workers 40 --batch-size 100000 --gzip-level 6
```

`--workers` controls how many `.jsonl.zst` files are ingested concurrently. It defaults to
`min(4, CPU count, source file count)`. Use `--workers 1` for serial ingest, or raise it for
dedicated ingest machines:

```sh
.venv/bin/python ingest.py /path/to/jsonl-zst-files --workers 40 --batch-size 5000
```

Each worker uses its own Postgres connection and streams one file at a time, so memory stays bounded
by roughly `workers * batch-size * average gzipped payload size`. On the Docker Compose Postgres
setup, disk and WAL throughput may become the bottleneck before CPU or RAM are exhausted; start with
`--workers 8` and scale up while watching database throughput.

`--synchronous-commit off` can improve commit throughput during ingest, but recently committed rows
can be lost if Postgres or the host crashes before WAL is flushed. The table remains consistent, and
rerunning ingest will skip already-present IDs.

## Table

The script creates a lean lookup table:

```sql
CREATE TABLE IF NOT EXISTS analysis_records (
  id text PRIMARY KEY,
  payload_gzip bytea NOT NULL
);
```

Rows are inserted with `ON CONFLICT (id) DO NOTHING`, so reruns skip records that were already loaded before a previous failure.

## API Shape

Run the local API with one or more comma-separated API keys:

```sh
API_KEYS=dev-key1,dev-key2 .venv/bin/python -m uvicorn api:app --host 127.0.0.1 --port 8000
```

Health check:

```sh
curl -i http://127.0.0.1:8000/health
```

Fetch a record:

```sh
curl -H 'X-API-Key: dev-key' --compressed http://127.0.0.1:8000/analysis/<track-id>
```

`GET /analysis/{id}` returns the stored gzipped payload directly:

```text
Content-Type: application/json
Content-Encoding: gzip
Vary: Accept-Encoding
```

The data endpoint requires `X-API-Key`. `GET /health` is public.

## Error Handling

- Malformed JSON or missing `meta.track_id`: log the file and line, skip that record, continue.
- Zstandard decode error: keep already committed batches, log the file error, continue to the next source file when possible.
- The process exits nonzero if any input errors occurred.
