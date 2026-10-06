#!/usr/bin/env python3
# Copyright ukue.com 2026
# SPDX-License-Identifier: Apache-2.0
"""A ukue worker in plain Python, talking to "ukue serve" over HTTP.

Start the server, then this worker:

    ukue serve jobs.ukue
    python3 http_worker.py email

It claims jobs from the queue one at a time, waiting up to 20 seconds for
each, prints the payload and marks the job done. A job whose handler raises
an exception is failed and tried again later. Put your own work in handle().

Set UKUE_URL if the server isn't at http://127.0.0.1:7660, and UKUE_TOKEN if
it needs a token. --once stops after one job, which the tests use.
"""
import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("UKUE_URL", "http://127.0.0.1:7660").rstrip("/")
TOKEN = os.environ.get("UKUE_TOKEN", "")


def call(method, path, body=None):
    """Send one request and return the decoded JSON, or None for 204."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            if resp.status == 204:
                return None
            return json.load(resp)
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        raise RuntimeError(f"{method} {path}: HTTP {e.code}: {detail}") from None


def handle(job):
    """Do the work for one job. Raise an exception to fail the attempt."""
    print(f"job {job['id']} (try {job['attempt']} of {job['max_attempts']}): {job.get('payload')}", flush=True)


def main():
    args = [a for a in sys.argv[1:] if a != "--once"]
    once = "--once" in sys.argv[1:]
    if len(args) != 1:
        raise SystemExit("usage: http_worker.py QUEUE [--once]")
    queue = args[0]
    while True:
        job = call("POST", "/v1/claim", {"queue": queue, "wait": 20, "lease": 60})
        if job is None:
            if once:
                raise SystemExit("no job")
            continue
        try:
            handle(job)
        except Exception as e:  # report every failure back to ukue
            call("POST", f"/v1/jobs/{job['id']}/fail", {"token": job["token"], "error": repr(e)})
        else:
            call("POST", f"/v1/jobs/{job['id']}/ack", {"token": job["token"]})
        if once:
            return


if __name__ == "__main__":
    main()
