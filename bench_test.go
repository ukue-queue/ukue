// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Every operation below is a committed SQLite transaction with
// synchronous = FULL, so the numbers mostly measure how fast the disk
// flushes.

func BenchmarkEnqueue(b *testing.B) {
	q, _ := openTemp(b)
	payload := []byte(`{"to":"dana@example.com","template":"welcome"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(bg, "email", payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	q, _ := openTemp(b)
	payload := []byte(`{"to":"dana@example.com","template":"welcome"}`)
	b.SetParallelism(2) // 2 x GOMAXPROCS goroutines
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Enqueue(bg, "email", payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkClaimAck measures taking a job and marking it done: two
// transactions per job.
func BenchmarkClaimAck(b *testing.B) {
	q, _ := openTemp(b)
	payload := []byte(`{"to":"dana@example.com","template":"welcome"}`)
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(bg, "email", payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j, err := q.Claim(bg, "email", time.Minute)
		if err != nil || j == nil {
			b.Fatal(j, err)
		}
		if err := q.Ack(bg, j); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWork measures the whole path a job takes through Work with eight
// handlers at once.
func BenchmarkWork(b *testing.B) {
	q, _ := openTemp(b)
	for i := 0; i < b.N; i++ {
		if _, err := q.Enqueue(bg, "email", []byte("x")); err != nil {
			b.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(b.N)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	b.ResetTimer()
	go q.Work(ctx, "email", func(context.Context, *Job) error {
		wg.Done()
		return nil
	}, Concurrency(8), PollInterval(10*time.Millisecond))
	wg.Wait()
	b.StopTimer()
}
