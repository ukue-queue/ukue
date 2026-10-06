#!/usr/bin/env python3
# Copyright ukue.com 2026
# SPDX-License-Identifier: Apache-2.0
"""Add a job to a ukue file with nothing but Python's standard library.

A ukue file is an SQLite database with a documented layout (FORMAT.md), so
any program that can write SQLite can add jobs to it. This script is one
INSERT statement plus a version check.

    python3 add_job.py jobs.ukue email '{"to": "dana@example.com"}'
    python3 add_job.py jobs.ukue reports --delay 3600 'monthly'

It prints the new job's ID. Create the file first with "ukue init jobs.ukue"
(or let any ukue command create it).
"""
import argparse
import sqlite3
import sys

FORMAT_VERSION = 1

# Milliseconds since 1970 in UTC, worked out inside SQLite so that every
# writer of the file uses the same clock format.
NOW_MS = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"


def add_job(path, queue, payload, delay_seconds=0.0, max_attempts=10, priority=0):
    """Add one job and return its ID. payload may be str or bytes."""
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    con = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    try:
        # Make the commit durable, as ukue itself does.
        con.execute("PRAGMA synchronous = FULL")
        row = con.execute(
            "SELECT value FROM ukue_meta WHERE key = 'format_version'"
        ).fetchone()
        if row is None or int(row[0]) != FORMAT_VERSION:
            raise SystemExit(f"{path}: not a ukue file in format version {FORMAT_VERSION}")
        cur = con.execute(
            f"""INSERT INTO ukue_jobs (queue, payload, max_attempts, priority, run_at)
                VALUES (?, ?, ?, ?, {NOW_MS} + ?)""",
            (queue, payload, max_attempts, priority, int(delay_seconds * 1000)),
        )
        return cur.lastrowid
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            raise SystemExit(f"{path}: not a ukue file (run: ukue init {path})")
        raise
    finally:
        con.close()


def main():
    p = argparse.ArgumentParser(description="Add a job to a ukue file.")
    p.add_argument("file")
    p.add_argument("queue")
    p.add_argument("payload", nargs="?", help="the payload; read from stdin when left out")
    p.add_argument("--delay", type=float, default=0, help="seconds to wait before the job can run")
    p.add_argument("--attempts", type=int, default=10, help="tries before the job is dead")
    p.add_argument("--priority", type=int, default=0, help="higher runs first among due jobs")
    a = p.parse_args()
    payload = a.payload if a.payload is not None else sys.stdin.buffer.read()
    print(add_job(a.file, a.queue, payload, a.delay, a.attempts, a.priority))


if __name__ == "__main__":
    main()
