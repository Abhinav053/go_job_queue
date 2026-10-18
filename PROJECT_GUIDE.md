# Go Job Queue

A small HTTP job service backed by PostgreSQL. The API stores submitted jobs in
the `jobs` table, and three worker goroutines claim and process eligible jobs.
PostgreSQL keeps job state durable across application restarts.

## Requirements and startup

- Go version declared in `go.mod`.
- PostgreSQL with the schema in `schema.sql` applied.
- `DATABASE_URL` set to a PostgreSQL connection URL.

On PowerShell, set the connection URL for the current session and apply the
schema with:

```powershell
$env:DATABASE_URL = "postgres://postgres:password@localhost:5432/jobqueue?sslmode=disable"
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" "$env:DATABASE_URL" -f .\schema.sql
go run .
```

The service listens on port `8080`. Keep credentials in a local `.env` or shell
environment; `.env` files are ignored by Git. The application reads the
environment directly and does not load `.env` files itself.

## HTTP API

### `POST /jobs`

Accepts one JSON object:

```json
{
  "payload": "send a report",
  "max_attempts": 3,
  "timeout_seconds": 10,
  "run_at": "2026-10-19T12:00:00Z"
}
```

`payload` is required. `max_attempts` defaults to `3` and accepts `1` through
`5`. `timeout_seconds` defaults to `10` and accepts `1` through `300`. `run_at`
is optional RFC3339 and defaults to the current time; jobs scheduled more than
24 hours ahead are rejected. The API returns `202 Accepted`, the stored job,
and a `Location` header for its status URL.

### `GET /jobs/{id}`

Returns the stored job or `404` when no job has that ID.

### `GET /healthz`

Returns a simple liveness response. It does not check PostgreSQL connectivity.

## Worker behavior

Workers claim due `queued` and `retrying` rows inside PostgreSQL using
`FOR UPDATE SKIP LOCKED`, so concurrent workers do not claim the same job.
Each claim increments `attempts` and records a `locked_at` lease timestamp.
Jobs time out according to `timeout_seconds`. A payload equal to `fail`
simulates a processing failure; retry delays grow exponentially and cap at 256
seconds. After the final attempt, the job is marked `failed`.

A recovery loop checks for running jobs whose lease is older than ten minutes.
It makes them eligible for retry, or marks them failed when attempts are
exhausted. On shutdown, active workers are canceled and claimed jobs are
returned to the queue when possible. Press Ctrl+C to stop the service.

## Files

- `main.go`: database, worker, recovery loop, HTTP server, and graceful shutdown.
- `api.go`: HTTP routes, request validation, and PostgreSQL-backed job reads/writes.
- `worker.go`: atomic job claiming, processing, retries, and stale lease recovery.
- `job.go`: job model and status constants.
- `db.go`: PostgreSQL connection setup using `DATABASE_URL`.
- `schema.sql`: durable job table, lease column, and ready-job index.
- `go.mod` and `go.sum`: Go module dependencies and checksums.
