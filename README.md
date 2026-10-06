# ukue

**µkue, a micro queue: a job queue that lives in one file.**

ukue keeps background jobs in a single SQLite file. Your program adds jobs to
it. Workers take them, run them and mark them done. A job that fails is tried
again after a growing delay. A job can wait for a set time before it runs. A
job that keeps failing is set aside as a dead letter for a person to look at.

There's no server to run next to your app. Embed ukue as a library, or start
the one small `ukue` binary when programs in other languages, or on other
machines, need the same queue. It's the same file either way.

Version 0.1.0 · Apache License 2.0 · [ukue.com](https://ukue.com)

- [What a job queue does](#what-a-job-queue-does)
- [Quick start](#quick-start)
- [Use it from Go](#use-it-from-go)
- [Use it from any language](#use-it-from-any-language)
- [How a job moves](#how-a-job-moves)
- [The ukue command](#the-ukue-command)
- [What ukue promises, and what it doesn't](#what-ukue-promises-and-what-it-doesnt)
- [How it was tested](#how-it-was-tested)
- [Build it yourself](#build-it-yourself)
- [The name](#the-name)
- [License](#license)

## What a job queue does

A job queue is a to-do list for software. When a user signs up, your app
writes "send Dana a welcome email" on the list and answers the user right
away, instead of making them wait while it talks to the mail server. A worker
takes tasks off the list and does them.

The list has to cope with things going wrong. If the mail server is down, the
task goes back on the list with a later time, and the wait grows after each
failure. If it fails too many times, it moves to a separate pile so a person
can see what happened. Some tasks go on the list with a start time, like
"send the reminder in three days". If a worker crashes halfway through a task,
the task must not get lost.

Small teams often install Redis or RabbitMQ just to have that list. That's a
second service to install, secure, watch, back up and pay for, to send a few
emails in the background. ukue keeps the list in one file instead. Nothing
runs beside your app, the list survives crashes and restarts, and copying the
file is a backup.

Under the hood the file is an SQLite database with a fixed, documented
layout, written up in [FORMAT.md](FORMAT.md). SQLite already handles crash
safety and locking, and ukue adds the queue rules on top. Any SQLite tool can
open the file, and any language that can write SQLite can add jobs to it.

## Quick start

Download the `ukue` binary for your system from the
[latest release](https://github.com/ukue-queue/ukue/releases/latest) and put
it on your PATH. Then:

```sh
ukue init jobs.ukue
ukue add jobs.ukue email '{"to": "dana@example.com"}'
ukue add jobs.ukue email --delay 1h '{"to": "sam@example.com"}'
ukue stats jobs.ukue
```

`ukue work` runs a command for each job, with the payload on its standard
input:

```sh
ukue work jobs.ukue email -- ./send-email.sh
```

Exit status 0 marks the job done. Any other status fails the attempt, and the
job is tried again later. Status 65 means the payload itself is bad, and the
job goes straight to the dead letters. The command also gets `UKUE_JOB_ID`,
`UKUE_QUEUE`, `UKUE_ATTEMPT`, `UKUE_MAX_ATTEMPTS` and `UKUE_FILE` in its
environment.

## Use it from Go

```sh
go get github.com/ukue-queue/ukue
```

```go
import (
	"github.com/ukue-queue/ukue"
	_ "github.com/mattn/go-sqlite3"
)

q, err := ukue.Open("jobs.ukue")
if err != nil {
	log.Fatal(err)
}
defer q.Close()

// Add a job, now or for later.
q.Enqueue(ctx, "email", []byte(`{"to":"dana@example.com"}`))
q.Enqueue(ctx, "email", []byte(`{"to":"sam@example.com"}`), ukue.Delay(time.Hour))

// Run jobs until ctx is cancelled.
err = q.Work(ctx, "email", func(ctx context.Context, job *ukue.Job) error {
	return sendEmail(ctx, job.Payload)
}, ukue.Concurrency(4))
```

A handler that returns nil marks the job done. An error fails the attempt.
Wrap the error in `ukue.Permanent` to skip the retries, or in
`ukue.RetryAfter` to choose the delay. A panic counts as a failure. While a
handler runs, `Work` renews the job's lease. When `ctx` is cancelled, `Work`
stops taking jobs, gives running handlers 30 seconds to finish, and puts back
any job whose handler stopped early, without counting the attempt.

The package talks to SQLite through `database/sql` and works with any SQLite
driver registered as `sqlite3` or `sqlite`. The examples use
[go-sqlite3](https://github.com/mattn/go-sqlite3). To keep jobs in your app's
own SQLite database, open it yourself and pass it to `ukue.OpenDB`. Then
`EnqueueTx` adds a job inside your own transaction, so the job exists only if
the rest of the transaction commits.

The full API is on [pkg.go.dev](https://pkg.go.dev/github.com/ukue-queue/ukue),
and [examples/go](examples/go/main.go) is a program you can run.

## Use it from any language

**Over HTTP.** `ukue serve` puts the file behind a small JSON API:

```sh
ukue serve jobs.ukue                     # http://127.0.0.1:7660
curl -X POST localhost:7660/v1/jobs  -d '{"queue": "email", "payload": {"to": "dana@example.com"}}'
curl -X POST localhost:7660/v1/claim -d '{"queue": "email", "wait": 20}'
```

A claimed job comes with a token. Send it back to `/v1/jobs/{id}/ack` when the
work is done, or to `/v1/jobs/{id}/fail` when it isn't. [API.md](API.md)
covers every endpoint, and
[examples/python/http_worker.py](examples/python/http_worker.py) is a worker
in plain Python. To listen beyond your own machine, set a token in
`UKUE_TOKEN` or `--token-file`; clients then send it as
`Authorization: Bearer <token>`. Without a token, the server answers only
requests addressed to `localhost`, `127.0.0.1` or `::1`, and web pages from
other sites can't change anything through it.

**Straight into the file.** A program on the same machine can add a job with
one SQLite `INSERT`, without going through ukue at all:

```sh
python3 examples/python/add_job.py jobs.ukue email '{"to": "dana@example.com"}'
```

[FORMAT.md](FORMAT.md) gives the steps for claiming and finishing jobs too,
for anyone who wants to write a worker in another language without the HTTP
server.

## How a job moves

| From | To | When |
|---|---|---|
| (new) | ready | A program adds the job. With a delay, it waits until its time comes |
| ready | running | A worker claims it, and holds it under a lease |
| running | done | The worker finishes it. The job is deleted, unless you keep finished jobs |
| running | ready | The attempt failed, or the worker stopped answering and the lease ran out. The job waits out the backoff first |
| running | dead | The last attempt failed, or the failure was marked permanent |
| dead | ready | Someone retries it |

Among the jobs that are due, workers take the highest priority first, then
the one that has waited longest. Priorities run from -100 to 100, and the
default is 0.

| Setting | Default | Change it with |
|---|---|---|
| Attempts before a job is dead | 10 | `ukue.MaxAttempts`, `--attempts`, `max_attempts` |
| Wait after a failure | 10 s, doubling each time, at most 1 hour, plus up to a tenth at random | `ukue.WithBackoff` |
| Lease | 1 minute, renewed every 20 seconds while the job runs | `ukue.Lease`, `--lease`, `lease` |
| Longest idle wait between looks | 1 second | `ukue.PollInterval`, `--poll` |
| Time to finish on shutdown | 30 seconds | `ukue.ShutdownGrace`, `--grace` |

With the defaults, a job that keeps failing gets 10 attempts spread over
about an hour and a half before it is dead.

## The ukue command

| Command | Does |
|---|---|
| `ukue init FILE` | Creates an empty ukue file |
| `ukue add FILE QUEUE [PAYLOAD]` | Adds a job. The payload comes from standard input when left out. `--delay`, `--at`, `--attempts`, `--priority` |
| `ukue work FILE QUEUE -- COMMAND` | Runs COMMAND for each job. `--concurrency`, `--timeout`, `--lease`, `--poll`, `--grace`, `--keep-done`, `--quiet` |
| `ukue serve FILE` | Serves the HTTP API. `--addr`, `--token-file`, `--allow-no-token`, `--max-body`, `--keep-done` |
| `ukue stats FILE` | Counts jobs per queue and state, and how long the oldest due job has waited |
| `ukue list FILE` | Lists jobs. `--queue`, `--state`, `--limit`, `--after`, `--json` |
| `ukue show FILE ID` | Shows one job with its payload and last error |
| `ukue retry FILE ID...` | Moves dead jobs back to ready. `--all` for every dead job, with `--queue` to pick one queue |
| `ukue purge FILE --state dead` | Deletes dead jobs, or finished ones with `--state done` |
| `ukue delete FILE ID...` | Deletes jobs |
| `ukue version` | Prints the version |

Options can go before or after the file name. Only `init` and `add` create a
file. Every other command needs an existing ukue file and refuses anything
else, so a typo in the path is caught and other SQLite files are left alone.

When `ukue work` stops, on Ctrl-C, SIGTERM, a `--timeout` or a lost lease, it
sends SIGTERM to the command and everything the command started, and SIGKILL
10 seconds later if they're still running. On Ctrl-C it first gives running
commands the `--grace` period to finish.

## What ukue promises, and what it doesn't

- **At least once.** Every job runs until a worker finishes it. A worker that
  dies after doing the work but before marking it done leaves a job that runs
  again. Make handlers safe to repeat, for example by using the job's ID as an
  idempotency key where a repeat would hurt.
- **Durable.** ukue runs SQLite with `synchronous = FULL`, so a job that was
  added is still there after a crash or a power cut, as long as the disk
  really writes what it is told to flush. `EnqueueTx` commits on your own
  connection, so set `synchronous = FULL` there too.
- **One machine per file.** Many processes on one machine can share the file.
  Programs on other machines go through `ukue serve`. Don't put the file on a
  network file system: SQLite's locking doesn't work there.
- **Small, and fast enough for small teams.** Each change is a transaction
  flushed to disk, so speed depends mostly on the disk. On a two-core cloud
  machine, ukue added about 4,000 jobs a second, and a worker ran about 900
  jobs a second (the middle of three runs). A queue that needs tens of
  thousands of jobs a second across many machines wants a different tool.

## How it was tested

The tests run every operation, every state change and the HTTP API, and then
some harder cases. These results are recorded in [test/results](test/results):

- **Killed workers.** Four worker processes shared one file while one of them
  was killed with SIGKILL at a random moment, 250 times. All 8,000 jobs
  finished. None was lost or left stuck, and SQLite's integrity check passed
  during and after the run. 329 jobs ran more than once, as at-least-once
  delivery allows, because their worker was killed between starting the work
  and marking it done.
- **A worker killed while holding a job,** 20 times in a row. Each job went
  back on the queue once its lease ran out, and finished on the second
  attempt.
- **Processes sharing a file.** Four processes with four workers each ran
  2,000 jobs, and every job ran exactly once.
- **A locked file.** Another program held the write lock longer than the busy
  timeout just as a job finished. The worker kept renewing the lease and
  trying to mark the job done until it could, and the job ran once.
- **Python and Go at once.** Python, using its own copy of SQLite, added 200
  jobs while Go workers took them. Every job ran once and the file stayed
  intact. The Python HTTP worker was tested against the server too.
- **Many jobs waiting.** With 100,000 jobs scheduled for later, an empty claim
  took 21 microseconds, and adding, claiming and finishing a job took 1.1
  milliseconds. Claims look jobs up through indexes, so a long schedule
  doesn't slow them down.
- **The race detector** found nothing across the whole suite.

To run it all yourself: `sh scripts/record-tests.sh`.

## Build it yourself

You need Go 1.25 or later and a C compiler, because go-sqlite3 compiles
SQLite with cgo.

```sh
go install github.com/ukue-queue/ukue/cmd/ukue@latest
```

From a clone, `go build ./cmd/ukue` builds the binary and `go test ./...`
runs the tests. The release binaries are built by
[the release workflow](.github/workflows/release.yml) on GitHub.

## The name

Read µkue as "micro queue". µ is the Greek letter mu, the sign for micro, and
it stands in for micro in names like µTorrent. Keyboards and web addresses
have no µ, so it's written with the lookalike u. Kue was a well-known job
queue for Node.js, written in 2011, and ukue borrows its name and its basic
features, delayed jobs and retries with backoff, without the Redis server Kue
needed.

## License

Copyright ukue.com 2026. Licensed under the [Apache License 2.0](LICENSE). The
`ukue` command also contains go-sqlite3 (MIT), SQLite (public domain) and the
Go runtime (BSD), as listed in [NOTICE](NOTICE) and
[THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
