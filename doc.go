// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package ukue is a job queue that lives in one file.
//
// A ukue file is a SQLite database with a small, documented layout (see
// FORMAT.md in the repository). Programs add jobs to it, workers take jobs
// from it, and every change to a job is one SQLite transaction, so a crash at
// any moment leaves the queue in a consistent state.
//
// Jobs that fail are tried again after a growing delay. Jobs can wait for a
// set time before they run. Jobs that keep failing move to the dead-letter
// state, where a person can look at them and retry them. A worker holds each
// job it takes under a lease. If the worker stops without finishing the job,
// the lease runs out and the job goes back on the queue.
//
// The package talks to SQLite through database/sql, so it needs a SQLite
// driver. Import one next to it:
//
//	import (
//		"github.com/ukue-queue/ukue"
//		_ "github.com/mattn/go-sqlite3"
//	)
//
//	q, err := ukue.Open("jobs.ukue")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer q.Close()
//
//	id, err := q.Enqueue(ctx, "email", []byte(`{"to":"dana@example.com"}`))
//
//	err = q.Work(ctx, "email", func(ctx context.Context, job *ukue.Job) error {
//		return sendEmail(ctx, job.Payload)
//	}, ukue.Concurrency(4))
//
// Delivery is at least once. A job whose worker dies after doing the work but
// before acknowledging it runs again, so handlers should be safe to repeat.
package ukue
