# Go Job Queue

A Go HTTP service that accepts jobs, stores them durably in PostgreSQL, and
processes them with concurrent worker goroutines. Multiple copies of the
service can share the same database: workers claim jobs using PostgreSQL row
locks so that two workers do not normally process the same eligible row at the
same time.

The current project uses PostgreSQL as both the durable job store and the
coordination mechanism. It does **not** use a separate message broker such as
RabbitMQ, Redis, or Kafka. A job is a row in the `jobs` table; workers poll and
claim rows from that table.

## Contents

- [Architecture](#architecture)
- [Job lifecycle](#job-lifecycle)
- [Repository files](#repository-files)
- [Data model and context](#data-model-and-context)
- [Run the service](#run-the-service)
- [End-to-end example](#end-to-end-example)
- [HTTP API](#http-api)
- [Worker behavior in detail](#worker-behavior-in-detail)
- [Shutdown](#shutdown)
- [Current limits and operational notes](#current-limits-and-operational-notes)

## Architecture

```mermaid
flowchart LR
    Client[HTTP client] -->|POST /jobs| API[Go HTTP API]
    API -->|INSERT job row| DB[(PostgreSQL jobs table)]
    Client -->|GET /jobs/id| API
    API -->|SELECT job state| DB

    subgraph Service process
        API
        W1[Worker goroutine 1]
        W2[Worker goroutine 2]
        W3[Worker goroutine 3]
        R[Recovery goroutine]
    end

    DB -->|atomic claim with SKIP LOCKED| W1
    DB -->|atomic claim with SKIP LOCKED| W2
    DB -->|atomic claim with SKIP LOCKED| W3
    W1 -->|update result / retry| DB
    W2 -->|update result / retry| DB
    W3 -->|update result / retry| DB
    R -->|recover stale running rows| DB
```

Each service process starts three workers and one stale-job recovery loop.
Additional service processes can use the same `DATABASE_URL`; PostgreSQL
coordinates their claims. The workers in a single process are goroutines, not
separate operating-system processes.

The main connection between components is the database:

1. The API validates an incoming request and inserts a `queued` row.
2. A worker polls for an eligible row and claims it in one SQL statement.
3. The claim changes the row to `running` and increments its attempt count.
4. The worker runs the job handler and writes a terminal state or a retry
   schedule back to PostgreSQL.
5. A client fetches the latest state by calling `GET /jobs/{id}`.

Because the job row is committed to PostgreSQL, it can remain available after
the Go process exits. This is more durable and shareable than an in-memory
channel, but it also means database availability and schema correctness are
essential to the service.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: API inserts job
    queued --> running: worker claims due job
    retrying --> running: worker claims due retry
    running --> completed: handler returns nil
    running --> retrying: handler fails and attempts remain
    running --> failed: last allowed attempt fails
    running --> queued: worker is shutting down
    running --> retrying: stale lease recovered, attempts remain
    running --> failed: stale lease recovered, attempts exhausted
```

- **`queued`**: newly submitted and not yet claimed, or released during worker
  shutdown.
- **`running`**: a worker claimed the job; `locked_at` records when the claim
  happened.
- **`retrying`**: an attempt failed and `run_at` is set to the next eligible
  retry time.
- **`completed`**: the handler succeeded.
- **`failed`**: attempts were exhausted, or recovery found a stale running job
  that had already used all configured attempts.

`attempts` is incremented when the job is claimed, before the handler starts.
Therefore a job that times out or whose worker exits after claiming has already
consumed an attempt.

## Repository files

All application files are in the repository root.

| File | Responsibility |
| --- | --- |
| [`main.go`](./main.go) | Program entry point. Opens PostgreSQL, starts workers and recovery, starts the HTTP server, and coordinates graceful shutdown. |
| [`api.go`](./api.go) | HTTP routes, JSON response helper, job request validation, PostgreSQL insert/read queries, and database health check. |
| [`worker.go`](./worker.go) | Atomic job claiming, handler execution, status persistence, retry scheduling, stale-job recovery, polling loops, and per-worker lifecycle. |
| [`job.go`](./job.go) | Shared `Job` data structure, job statuses, and the `JobHandler` function type. |
| [`db.go`](./db.go) | Reads `DATABASE_URL`, creates a `pgxpool.Pool`, and checks database connectivity. |
| [`schema.sql`](./schema.sql) | Creates the job table, status/field constraints, lease column, and partial index for eligible jobs. Apply this before starting the service. |
| [`go.mod`](./go.mod) | Go module name and Go version directive, plus the direct PostgreSQL driver/pool dependency and its indirect dependencies. |
| [`go.sum`](./go.sum) | Module checksums used by Go to verify dependency contents. |
| [`.gitignore`](./.gitignore) | Excludes build outputs, local `.env` files, editor directories, and OS metadata from Git. |
| [`PROJECT_GUIDE.md`](./PROJECT_GUIDE.md) | Short pointer to this detailed guide. |
| `.env` (local, ignored) | A local environment file may exist, but the application does not load it. Set `DATABASE_URL` in the process environment yourself. Do not commit credentials. |
| `go-job-queue.exe` (local build output) | A compiled Windows executable may exist in the working folder. It is generated output, not source, and is ignored by Git. Rebuild it from source when needed. |

There is no separate `registry.go` in the current database-backed version:
PostgreSQL replaces the earlier in-memory registry.

## Function-by-function reference

This section names the executable functions in the Go source and what each
contributes to the request or worker flow.

### `main.go`

- **`main`**: creates a ten-second startup context and calls `openDB`; fails
  startup if configuration or database connection fails. It starts three
  `workerLoop` goroutines and one `recoveryLoop`, constructs the API and HTTP
  server, waits for a termination signal or server exit, shuts down HTTP, then
  cancels and joins worker/recovery goroutines. The database pool is closed
  when `main` returns.

### `db.go`

- **`openDB(ctx)`**: reads `DATABASE_URL`, errors if missing, builds a
  `pgxpool.Pool`, and verifies connectivity with a ping bounded by five
  seconds. It closes the pool if ping fails and returns the ready pool on
  success.

### `api.go`

- **`NewAPI(db)`**: creates an API object holding the shared PostgreSQL pool.
- **`(a *API) Routes()`**: registers `POST /jobs`, `GET /jobs/{id}`, and
  `GET /healthz` on a `net/http` `ServeMux`.
- **`writeJSON(w, status, value)`**: sets the JSON content type, writes the
  status code, and encodes the value as JSON.
- **`(a *API) createJob(w, r)`**: limits and decodes the request body,
  validates fields and scheduling time, inserts a queued row, and returns the
  created job and its status URL.
- **`(a *API) getJob(w, r)`**: validates the route ID, selects the matching
  database row, maps a missing row to `404`, and returns other query failures
  as `500`.
- **`(a *API) health(w, r)`**: pings the database; reports `200` when it is
  reachable and `503` otherwise.

### `worker.go`

- **`claimJob(ctx, db)`**: atomically chooses one due queued/retrying row that
  is not locked by another claim, marks it running, increments attempts, sets
  its lease timestamp, and returns its data.
- **`updateJob(ctx, db, id, status, runAt, jobErr)`**: writes a job's next
  status, schedule, error, lock clearing, and update time to PostgreSQL.
- **`executeJob(ctx, job)`**: sample handler. It waits two seconds while
  observing cancellation, returns a simulated error for the payload `fail`,
  and logs/returns success for other payloads.
- **`processJob(rootCtx, db, job)`**: runs the handler with a per-job timeout,
  then persists success, a scheduled retry, a final failure, or release back
  to the queue on worker shutdown.
- **`recoverStaleJobs(ctx, db)`**: moves running rows with expired ten-minute
  leases to retrying or failed based on their attempt count.
- **`recoveryLoop(ctx, db)`**: runs stale-row recovery immediately and then
  every 30 seconds until canceled; logs recovery errors while still active.
- **`workerLoop(ctx, workerID, db, wg)`**: logs worker startup, repeatedly
  attempts to claim one job, processes claimed jobs, treats no eligible rows
  as normal, and waits for cancellation or the next 250-millisecond poll.

### `job.go`

This file declares data/types rather than executable functions:

- **`JobStatus`** and its constants enumerate the persisted lifecycle values.
- **`Job`** defines the fields scanned from PostgreSQL and serialized by the
  API.
- **`JobHandler`** defines the context-aware function signature for processing
  a job.

### `schema.sql`

This file contains SQL statements rather than Go functions. It creates the
`jobs` table and constraints, adds `locked_at` if needed for an existing table,
and creates the partial index workers use to find due queued/retrying jobs.

## Data model and context

### `JobStatus` and constants (`job.go`)

`JobStatus` is a named string type used for the persisted lifecycle value.
Constants define `queued`, `running`, `retrying`, `completed`, and `failed`.
The SQL schema also restricts stored values to this set.

### `Job` (`job.go`)

`Job` is the data shape used by the API and workers:

| Field | JSON name | Meaning |
| --- | --- | --- |
| `ID` | `id` | PostgreSQL-generated `BIGINT` identifier, represented by Go `int64`. |
| `Payload` | `payload` | Input string passed to the job handler. |
| `Status` | `status` | Current lifecycle state. |
| `Attempts` | `attempts` | Number of times a worker has claimed the job. |
| `MaxAttempts` | `max_attempts` | Maximum number of claims/handler attempts allowed. |
| `TimeoutSeconds` | `timeout_seconds` | Timeout for one execution attempt. |
| `RunAt` | `run_at` | Earliest time a worker should claim the job. |
| `LastError` | `last_error` | Most recently recorded processing or recovery error; omitted from JSON when empty. |
| `CreatedAt` | `created_at` | Database creation timestamp. |
| `UpdatedAt` | `updated_at` | Most recent database update timestamp. |

PostgreSQL is the source of truth; a `Job` value returned to Go is a scanned
snapshot of a row, not the durable record itself.

### `JobHandler` (`job.go`)

```go
type JobHandler func(context.Context, Job) error
```

This function type defines the unit of job execution. The current sample
implementation is `executeJob` in `worker.go`. A successful handler returns
`nil`; an error triggers a retry or final failure, depending on the attempt
count. A production handler should observe its context so cancellation and
timeout signals can stop its work.

### Context usage

`context.Context` carries cancellation and deadline information. Contexts in
this application have separate purposes:

1. **Startup context (`main.go`)**: a ten-second deadline bounds initial
   PostgreSQL setup. `openDB` derives a five-second ping deadline from it.
2. **HTTP request context (`api.go`)**: `r.Context()` is passed to database
   insert, select, and ping calls. If a client disconnects or the request is
   canceled, the database operation can stop.
3. **Worker root context (`main.go`, `worker.go`)**: `workerCtx` is shared by
   the worker and recovery goroutines and is canceled during shutdown.
4. **Per-job execution context (`processJob`)**: a timeout derived from the
   worker root context limits one handler execution to `TimeoutSeconds`.
5. **Result-save context (`processJob`)**: a fresh context based on
   `context.Background()` allows up to five seconds to persist the outcome even
   if the worker root context has just been canceled.
6. **HTTP shutdown context (`main.go`)**: gives the HTTP server five seconds
   to finish active requests.

Cancellation is cooperative. `context` does not forcibly kill a goroutine;
handlers must cooperate with the context for execution to stop promptly.

## Run the service

### Requirements

- The Go version declared in [`go.mod`](./go.mod).
- A running PostgreSQL instance and a database created for this application.
- The `DATABASE_URL` environment variable containing a PostgreSQL connection
  URL.
- The schema in [`schema.sql`](./schema.sql) applied to that database.

The application does not load `.env` automatically. Set the variable in the
shell that launches the service. In PowerShell, for example:

```powershell
$env:DATABASE_URL = "postgres://USER:PASSWORD@localhost:5432/jobqueue?sslmode=disable"
```

Replace the placeholder credentials and database name with your own values.
Avoid putting real secrets into source control or shared logs.

Create the database if it does not already exist, then apply the schema (if
`psql` is installed and available on `PATH`):

```powershell
createdb jobqueue
psql $env:DATABASE_URL -f .\schema.sql
```

Start the application from the repository root:

```powershell
go run .
```

The HTTP server listens on port `8080`. The application exits during startup
with an error if `DATABASE_URL` is missing, PostgreSQL cannot be reached, or
worker initialization cannot proceed. The current code requires the schema to
be present; it does not automatically apply migrations.

To build a Windows executable:

```powershell
go build -o .\go-job-queue.exe .
```

Then run it in a shell where `DATABASE_URL` is set:

```powershell
.\go-job-queue.exe
```

## End-to-end example

The API and workers run in the same Go process, but job rows are stored in
PostgreSQL.

> **Current implementation caveat:** the schema permits `last_error` to be
> `NULL`, but the API scans it directly into a Go string. As a result, the
> insert response may be `500` even if PostgreSQL inserted the row, and a later
> lookup of a row with no error may also return `500`. See
> [Current limits and operational notes](#current-limits-and-operational-notes)
> before interpreting this as a guaranteed successful request.

### 1. Submit a job

In another PowerShell terminal:

```powershell
curl.exe -i -X POST http://localhost:8080/jobs `
  -H "Content-Type: application/json" `
  -d '{"payload":"send report","max_attempts":3,"timeout_seconds":10}'
```

The API:

1. Limits the body to 1 MiB and decodes exactly one JSON object.
2. Rejects unknown fields and invalid values.
3. Applies defaults where the request omitted numeric settings.
4. Inserts a `queued` row into PostgreSQL.
5. Returns `202 Accepted`, the inserted row as JSON, and a `Location` header
   such as `/jobs/42`.

### 2. Worker claims it

Each worker checks PostgreSQL every 250 milliseconds. Once the job is due, a
worker atomically changes its state to `running`, increments `attempts`, sets
`locked_at`, and reads the claimed row back. `FOR UPDATE SKIP LOCKED` lets
other workers skip rows already locked by a competing claim, including workers
in other service instances sharing this database.

### 3. Handler runs

`processJob` gives `executeJob` a context with the requested timeout. The
demonstration handler waits about two seconds, then logs the payload and returns
success. If its context is canceled first, it returns the context error.

### 4. Result is saved and queried

On success, the worker updates the row to `completed`, clears the lock and
previous error, and updates `updated_at`. Fetch the latest stored state with:

```powershell
curl.exe http://localhost:8080/jobs/42
```

The ID is generated by PostgreSQL; use the `id` returned by the POST response
rather than assuming it is `42`.

### Retry example

The sample handler treats a trimmed payload equal to `"fail"` as a processing
error. Submit it with multiple attempts:

```powershell
curl.exe -i -X POST http://localhost:8080/jobs `
  -H "Content-Type: application/json" `
  -d '{"payload":"fail","max_attempts":3,"timeout_seconds":10}'
```

After each failed attempt, the worker persists `retrying` with a future
`run_at`. The next claim occurs after that time. Once the configured final
attempt fails, the job becomes `failed` and `last_error` contains the failure
message.

## HTTP API

Routes are registered by `API.Routes` in [`api.go`](./api.go). They use the
Go `net/http` method-aware `ServeMux` patterns.

### `POST /jobs`

Accepts a JSON object with these fields:

| Field | Required? | Default and constraints |
| --- | --- | --- |
| `payload` | Yes | Trimmed; cannot be empty. |
| `max_attempts` | No | Defaults to `3`; must be between `1` and `10`. |
| `timeout_seconds` | No | Defaults to `10`; must be between `1` and `300`. |
| `run_at` | No | Defaults to current UTC time. If specified, must be RFC3339 and no more than 24 hours ahead. Past timestamps are accepted and already eligible. |

The body must contain exactly one JSON object, may not contain unknown fields,
and is limited to 1 MiB. Invalid input returns `400 Bad Request`. A database
insert error returns `500 Internal Server Error`. Success returns
`202 Accepted`.

### `GET /jobs/{id}`

Accepts a positive base-10 integer ID. Invalid IDs return `400`; missing rows
return `404`; successful lookups return `200` and the current job record.
Database errors return `500`.

### `GET /healthz`

Pings PostgreSQL using the request context. It returns `200` with
`{"status":"ok"}` when the database responds and `503 Service Unavailable`
with `{"status":"database unavailable"}` if it does not.

## Worker behavior in detail

The worker functions live in [`worker.go`](./worker.go).

### Claiming and concurrency

`claimJob` selects one due row whose status is `queued` or `retrying`, ordered
by `run_at` and then `id`. The selection uses `FOR UPDATE SKIP LOCKED`, and a
single SQL statement updates the selected row to `running`, increments
`attempts`, records a lease timestamp in `locked_at`, and returns the row's
fields. This atomic database operation is the core coordination mechanism for
multiple workers and multiple service processes.

If no row is eligible, the query returns `pgx.ErrNoRows`; that is normal, not a
worker failure. Other claim errors are logged, and the worker tries again on
its next polling tick.

### Execution and retries

`processJob` executes the handler once for each successful claim. On success it
persists `completed`. On error:

- If `attempts >= max_attempts`, it persists `failed`.
- Otherwise it calculates exponential backoff and persists `retrying` with
  `run_at` set to the next retry time.

The delay is `1, 2, 4, 8, ...` seconds, capped at 256 seconds. Because
`attempts` increases at claim time, it also counts attempts interrupted by
timeouts or process failures after a claim.

### Lease recovery

`recoveryLoop` calls `recoverStaleJobs` immediately and then every 30 seconds.
Rows still marked `running` with a `locked_at` older than ten minutes are
treated as abandoned:

- They become `retrying` and immediately eligible if attempts remain.
- They become `failed` if the attempt limit has been reached.

The recovery update clears `locked_at` and records
`worker lease expired; recovered` in `last_error`.

The lease is a crash-recovery mechanism, not a strict execution guarantee. If
work legitimately runs longer than ten minutes, another worker may recover and
claim the row while the original handler is still running. The normal API
timeout is at most five minutes, but external handlers must still be designed
to tolerate duplicate execution.

### Polling

Each `workerLoop` polls at a 250 millisecond interval. It can claim and process
only one job at a time; while the handler runs, that worker is occupied. Other
worker goroutines continue polling. Future-scheduled jobs remain in the
database and do not occupy a worker while waiting for their `run_at`.

## Shutdown

`main` listens for Ctrl+C (`os.Interrupt`) or `SIGTERM`. On shutdown:

1. The HTTP server is given five seconds to finish active requests. If graceful
   shutdown fails, the server is closed.
2. The worker root context is canceled.
3. Workers stop polling. A job whose handler is canceled by this signal is
   returned to `queued` with `run_at` set to now, so it can be attempted by a
   later-running worker process.
4. The recovery goroutine exits, and `main` waits for all workers and recovery
   to finish.
5. The deferred database-pool close runs when `main` returns.

The job release write uses a separate five-second context so it can still be
attempted after worker cancellation. If that database write fails, the stale
lease recovery loop can later handle the row, once a service is running again.

## Current limits and operational notes

- **PostgreSQL is required** for startup, submissions, status reads, worker
  claims, and health checks.
- **No dedicated broker:** PostgreSQL is also the queue and coordination
  system. This is a valid architecture for many workloads, but very high
  throughput may eventually call for a broker or a more specialized queue.
- **At-least-once behavior:** workers can execute a job more than once after a
  crash, lease expiration, or uncertain completion. Handlers should be
  idempotent or use an idempotency key for external side effects.
- **No auth or authorization:** the HTTP endpoints are unauthenticated in the
  current implementation; restrict network access or add an authentication
  layer before exposing them publicly.
- **Sample handler only:** `executeJob` simulates work; replace it with
  application-specific logic.
- **No automatic schema migration:** apply `schema.sql` before running the
  service and manage future schema changes explicitly.
- **Nullable `last_error` scanning:** the schema allows `last_error` to be
  `NULL`, while the API scans it directly into a Go `string` in its insert and
  lookup queries. PostgreSQL drivers generally cannot scan SQL `NULL` into a
  plain string, so those operations may return a scan error for rows whose
  `last_error` has not been set. In the insert path, the row may already have
  been committed even if the API then returns `500`; retrying the request could
  therefore create a duplicate job. The worker claim query already uses
  `COALESCE`; API queries or the schema should use a compatible nullable/default
  representation as well.
- **Readiness vs liveness:** `/healthz` checks database connectivity, but it
  does not verify worker progress, queue age, or whether jobs are accumulating.

[`PROJECT_GUIDE.md`](./PROJECT_GUIDE.md) is a short pointer to this README.
