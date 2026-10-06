# The ukue file format, version 1

A ukue file is an SQLite 3 database with two tables in it. This document
describes those tables and the steps for adding, claiming and finishing a job,
so that a program in any language can share a file with the `ukue` command and
the Go package. All it needs is SQLite.

The [Python example](examples/python/add_job.py) adds jobs this way with
nothing but Python's standard library.

## What stays the same

The version of the format is stored in the file, in `ukue_meta` under
`format_version`. This document describes version 1.

Within version 1, later releases of ukue may add indexes, `ukue_meta` keys and
columns that have default values. They won't remove or rename anything, and
they won't change what an existing column means. A program written against
this document keeps working.

A change that older programs couldn't handle safely gets a new version
number. ukue refuses to open a file whose version is newer than it knows, and
other programs should do the same.

## The file

- **SQLite 3.** Any page size and encoding.
- **WAL journal mode.** When ukue adds its tables to a file, it switches the
  file to WAL, and SQLite records that in the file, so every program that
  opens it uses WAL too. WAL lets programs read while another one writes.
  ukue doesn't change the mode again after that.
- **The header mark.** A file that ukue creates for itself has
  `application_id` set to `0x756B7565` (1969976677), the letters "ukue". The
  tables can also live inside an application's own database, next to its
  tables, and then `application_id` is left as the application set it.
- **One machine.** Many processes on one machine can use the file at the same
  time. Programs on other machines should go through `ukue serve` rather than
  opening the file over a network file system, which doesn't give SQLite the
  locking and shared memory it needs.
- **Times** are whole milliseconds since 1970-01-01 00:00:00 UTC.

## Tables

### ukue_meta

```sql
CREATE TABLE ukue_meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
) WITHOUT ROWID;
```

| Key | Value |
|---|---|
| `format_version` | `1` |
| `created_at` | When the file was set up, in milliseconds |
| `created_by` | The program that set it up, such as `ukue 0.1.0 (Go)` |

### ukue_jobs

```sql
CREATE TABLE ukue_jobs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	queue        TEXT    NOT NULL CHECK (length(queue) BETWEEN 1 AND 200),
	payload      BLOB    NOT NULL,
	state        TEXT    NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'running', 'done', 'dead')),
	priority     INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
	attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
	max_attempts INTEGER NOT NULL DEFAULT 10 CHECK (max_attempts >= 1),
	run_at       INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
	lease_until  INTEGER,
	token        TEXT,
	last_error   TEXT,
	created_at   INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
	updated_at   INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
);
```

| Column | Meaning |
|---|---|
| `id` | The job's number. `AUTOINCREMENT` means a number is never used twice, even after its job is deleted |
| `queue` | The queue's name, 1 to 200 characters. Workers ask for one queue at a time |
| `payload` | The job's data, as bytes. ukue never reads it. Text is stored as UTF-8 |
| `state` | `ready`, `running`, `done` or `dead`; see below |
| `priority` | Among the jobs that are due, the highest priority runs first. From -100 to 100 |
| `attempts` | How many times the job has been claimed |
| `max_attempts` | How many attempts the job gets before it is dead |
| `run_at` | When a ready job may start. In the future, the job is waiting (ukue calls it delayed) |
| `lease_until` | When a running job's lease runs out. Set only while running |
| `token` | A random value given to the worker that claimed the job. Set only while running |
| `last_error` | What the last failed attempt reported |
| `created_at`, `updated_at` | When the job was added, and last changed |

The defaults let a writer add a job with nothing but a queue and a payload.

### Indexes

```sql
CREATE INDEX ukue_jobs_due    ON ukue_jobs (queue, priority, run_at, id) WHERE state = 'ready';
CREATE INDEX ukue_jobs_next   ON ukue_jobs (queue, run_at)               WHERE state = 'ready';
CREATE INDEX ukue_jobs_leases ON ukue_jobs (lease_until)                 WHERE state = 'running';
CREATE INDEX ukue_jobs_state  ON ukue_jobs (state, queue, id);
```

The indexes keep claims fast when many jobs wait for later. Other programs
don't need to create them; ukue creates them along with the tables.

## States

A job is in one of four states. ukue also speaks of **delayed** jobs, which
are ready jobs whose `run_at` is still in the future.

| From | To | When |
|---|---|---|
| (new) | ready | A program adds the job |
| ready | running | A worker claims it |
| running | done, or deleted | The worker finishes it |
| running | ready | The attempt failed or the lease ran out, and attempts are left |
| running | dead | The last attempt failed, or the failure was permanent |
| dead | ready | Someone retries it |

- **ready**: waiting for a worker. Due once `run_at` has passed.
- **running**: held by one worker until `lease_until`.
- **done**: finished. Kept only when a program asks for that; by default a
  finished job is deleted.
- **dead**: failed on every attempt, or failed in a way that trying again
  won't fix. It stays until a person retries or deletes it.

## How to work with the file

Every step below is one transaction. Programs should set two things on each
connection first:

```sql
PRAGMA busy_timeout = 5000;   -- wait up to 5 s for another writer instead of failing
PRAGMA synchronous = FULL;    -- make each commit durable
```

`now` below means the current time in milliseconds.

### Add a job

```sql
INSERT INTO ukue_jobs (queue, payload, priority, max_attempts, run_at)
VALUES (?, ?, ?, ?, ?);
```

Every column but `queue` and `payload` can be left out. Store the payload as
a BLOB; programs that read jobs accept TEXT as well.

### Claim a job

Do all of this in one transaction that starts with `BEGIN IMMEDIATE`. It
takes the write lock before reading, so two workers can never pick the same
job.

1. Put back jobs whose lease ran out, in every queue. For each row with
   `state = 'running' AND (lease_until IS NULL OR lease_until <= now)`:
   if `attempts >= max_attempts`, set `state = 'dead'`; otherwise set
   `state = 'ready'` and `run_at` to `now` plus a backoff delay. In both
   cases set `lease_until` and `token` to NULL, `last_error` to a note that
   the lease expired, and `updated_at` to `now`.
2. Pick the job: among the rows with `queue = ?`, `state = 'ready'` and
   `run_at <= now`, the highest `priority`, then the smallest `run_at`, then
   the smallest `id`.
3. Take it:
   ```sql
   UPDATE ukue_jobs
   SET state = 'running', attempts = attempts + 1, lease_until = ?, token = ?, updated_at = ?
   WHERE id = ?;
   ```
   `lease_until` is `now` plus the lease, and `token` is a new random value
   of at least 128 bits, written as text.
4. `COMMIT`. The worker keeps the `id` and the `token`.

### Finish a job

```sql
DELETE FROM ukue_jobs WHERE id = ? AND state = 'running' AND token = ?;
```

To keep finished jobs, set `state = 'done'` and clear `lease_until` and
`token` instead. If no row changed, the worker no longer holds the job: its
lease ran out and the job went back on the queue, or someone deleted it.

### Report a failure

```sql
UPDATE ukue_jobs
SET state = 'ready', run_at = ?, lease_until = NULL, token = NULL, last_error = ?, updated_at = ?
WHERE id = ? AND state = 'running' AND token = ?;
```

`run_at` is `now` plus a backoff delay. When the job has used its last
attempt (`attempts >= max_attempts`), or the failure is one that trying again
won't fix, set `state = 'dead'` instead and leave `run_at` alone.

ukue's backoff after attempt *n* is 10 seconds times 2^(*n*-1), at most an
hour, plus up to a tenth more at random. Other programs may pick their own.

### Renew a lease

```sql
UPDATE ukue_jobs SET lease_until = ?, updated_at = ?
WHERE id = ? AND state = 'running' AND token = ?;
```

A worker running a long job renews its lease before it runs out. ukue renews
every third of the lease.

### Give a job back

A worker that is shutting down can return a job without using up the
attempt:

```sql
UPDATE ukue_jobs
SET state = 'ready', attempts = max(attempts - 1, 0), run_at = ?, lease_until = NULL, token = NULL, updated_at = ?
WHERE id = ? AND state = 'running' AND token = ?;
```

### Retry a dead job

```sql
UPDATE ukue_jobs SET state = 'ready', attempts = 0, run_at = ?, updated_at = ?
WHERE id = ? AND state = 'dead';
```

### Delete jobs

```sql
DELETE FROM ukue_jobs WHERE id = ?;
DELETE FROM ukue_jobs WHERE state = 'dead';   -- or 'done', and a queue if you like
```

## Rules

1. Change a running job only together with its token. Deleting is the one
   exception.
2. Don't keep a transaction open while doing a job's work. Claim, commit, do
   the work, then finish or fail in a new transaction.
3. Claim with `BEGIN IMMEDIATE`.
4. Use the four states only, and times in milliseconds.

Followed together, these rules give **at-least-once** delivery: every job runs
until a worker finishes it, and a job whose worker dies after doing the work
but before finishing it runs again. Work that must not happen twice, such as
a payment, should carry its own check, for example the job's `id` used as an
idempotency key.
