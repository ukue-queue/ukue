# The ukue HTTP API

`ukue serve FILE` puts a ukue file behind a small HTTP API, so programs in any
language, and on other machines, can add and run jobs. Go programs can mount
the same API in their own server with `server.New`.

```sh
ukue serve jobs.ukue                     # listens on 127.0.0.1:7660
UKUE_TOKEN=s3cret ukue serve --addr :7660 jobs.ukue
```

## The basics

- Requests and answers are JSON. Errors come back as `{"error": "..."}` with
  a 4xx or 5xx status.
- **Token.** When the server has a token (`UKUE_TOKEN` or `--token-file`),
  every request except `/v1/health` must send
  `Authorization: Bearer <token>`. The server refuses to listen beyond this
  machine without one, unless started with `--allow-no-token`.
- **Durations** can be a number of seconds (`30`, `0.5`) or a Go duration
  string (`"90s"`, `"1h30m"`).
- **Times** in answers are RFC 3339 in UTC with milliseconds, such as
  `"2026-10-06T09:00:00.000Z"`.
- **Payloads.** In a request, `payload` that is a JSON string is stored as its
  text, and any other JSON value is stored as compact JSON text. Use
  `payload_base64` for binary data. In answers, a payload that is valid UTF-8
  comes back as the string `payload`, and anything else as `payload_base64`.
- **Size.** A request body may be up to 1 MiB unless the server was started
  with `--max-body`.

## Adding jobs

### POST /v1/jobs

```json
{"queue": "email", "payload": {"to": "dana@example.com"}, "delay": "10m", "max_attempts": 5, "priority": 0}
```

Only `queue` is required. Use `delay` or `run_at` (an RFC 3339 time), not
both. Answers `201` with `{"id": 42}`.

## Running jobs

### POST /v1/claim

```json
{"queue": "email", "lease": 60, "wait": 20}
```

Takes the next due job and holds it for `lease` (default 60 seconds). With
`wait`, the request waits up to that long, at most 30 seconds, for a job to
arrive. Answers `200` with the job, or `204` with no body when there is none.

```json
{
  "id": 42,
  "queue": "email",
  "payload": "{\"to\":\"dana@example.com\"}",
  "attempt": 1,
  "max_attempts": 5,
  "priority": 0,
  "token": "6f1c0e2b9d4a47c3a1e5b8f2d7c90a13",
  "lease_until": "2026-10-06T09:01:00.000Z",
  "created_at": "2026-10-06T09:00:00.000Z",
  "last_error": ""
}
```

Keep the `token`. The calls below need it, and it proves the worker still
holds the job.

### POST /v1/jobs/{id}/ack

`{"token": "..."}`. The job is done. Answers `{"ok": true}`.

### POST /v1/jobs/{id}/fail

```json
{"token": "...", "error": "SMTP server said 451", "retry_in": 30}
```

The attempt failed. The job goes back on the queue after the usual backoff,
or after `retry_in` if given. Send `"dead": true` instead for a failure that
trying again won't fix, and the job goes straight to the dead letters. A job
on its last attempt is dead either way. Answers `{"state": "ready", "run_at": "..."}`
or `{"state": "dead"}`.

### POST /v1/jobs/{id}/extend

`{"token": "...", "lease": 60}`. Renews the lease for a long job. Answers
`{"lease_until": "..."}`.

### POST /v1/jobs/{id}/release

`{"token": "..."}`. Puts the job straight back on the queue without counting
the attempt, for a worker that is shutting down.

If the worker no longer holds the job, because its lease ran out or the job
was deleted, these four calls answer `409`. The job may already be running
somewhere else.

## Looking and tidying up

| Request | Does |
|---|---|
| `GET /v1/stats` | Counts per queue: `ready`, `delayed`, `running`, `expired`, `dead`, `done`, and `oldest_due` |
| `GET /v1/jobs?queue=&state=&after=&limit=` | Lists jobs in ID order. `state` is `ready`, `delayed`, `running`, `dead` or `done`; `after` is the last ID of the previous page; `limit` is at most 1000 |
| `GET /v1/jobs/{id}` | One job, in any state |
| `DELETE /v1/jobs/{id}` | Deletes a job |
| `POST /v1/jobs/{id}/retry` | Moves a dead job back to ready with its attempts reset |
| `POST /v1/retry` | `{"queue": "email"}` moves every dead job back to ready; leave out `queue` for all queues |
| `POST /v1/purge` | `{"state": "dead"}` or `{"state": "done"}` deletes those jobs; add `queue` to limit it |
| `GET /v1/health` | `{"ok": true, "version": "0.1.0", "format_version": 1}`, no token needed |

## A worker in a few lines

With curl and jq:

```sh
job=$(curl -s -X POST localhost:7660/v1/claim -d '{"queue":"email","wait":20}')
id=$(echo "$job" | jq -r .id)
token=$(echo "$job" | jq -r .token)
# ... do the work ...
curl -s -X POST localhost:7660/v1/jobs/$id/ack -d "{\"token\":\"$token\"}"
```

A complete worker in Python's standard library is in
[examples/python/http_worker.py](examples/python/http_worker.py).
