// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

// This example embeds ukue in a Go program: it adds a few jobs, then runs a
// worker that processes them until you press Ctrl-C.
//
//	go run ./examples/go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ukue-queue/ukue"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	q, err := ukue.Open("example.ukue")
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Add three jobs: one now, one in two seconds, and one that fails for good.
	for _, p := range []struct {
		payload string
		opts    []ukue.EnqueueOption
	}{
		{"welcome email for dana@example.com", nil},
		{"reminder email for dana@example.com", []ukue.EnqueueOption{ukue.Delay(2 * time.Second)}},
		{"bad payload", nil},
	} {
		id, err := q.Enqueue(ctx, "email", []byte(p.payload), p.opts...)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("added job", id)
	}

	handler := func(ctx context.Context, job *ukue.Job) error {
		if string(job.Payload) == "bad payload" {
			// Retrying won't help, so send it straight to the dead letters.
			return ukue.Permanent(errors.New("can't read this payload"))
		}
		fmt.Printf("sending: %s (try %d)\n", job.Payload, job.Attempt)
		return nil
	}

	fmt.Println("working; press Ctrl-C to stop")
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := q.Work(ctx, "email", handler, ukue.Concurrency(2), ukue.Logger(logger)); err != nil {
		log.Fatal(err)
	}
}
