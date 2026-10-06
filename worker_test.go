// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the time is up.
func waitFor(t testing.TB, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func queueEmpty(t testing.TB, q *Queue) func() bool {
	return func() bool {
		st, err := q.Stats(bg)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range st {
			if s.Ready+s.Delayed+s.Running+s.Expired > 0 {
				return false
			}
		}
		return true
	}
}

func TestWorkRunsEveryJob(t *testing.T) {
	q, _ := openTemp(t)
	const n = 300
	for i := 0; i < n; i++ {
		must[int64](t)(q.Enqueue(bg, "q", []byte{byte(i)}))
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error)
	go func() {
		done <- q.Work(ctx, "q", func(ctx context.Context, j *Job) error {
			mu.Lock()
			seen[j.ID]++
			mu.Unlock()
			return nil
		}, Concurrency(8), PollInterval(20*time.Millisecond))
	}()
	waitFor(t, 30*time.Second, "all jobs", queueEmpty(t, q))
	// A job added now wakes the idle worker at once, well before its poll.
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("late")))
	waitFor(t, 5*time.Second, "the late job", func() bool { mu.Lock(); defer mu.Unlock(); return seen[id] == 1 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(seen) != n+1 {
		t.Fatalf("%d of %d jobs ran", len(seen), n+1)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %d ran %d times", id, c)
		}
	}
}

func TestWorkRetriesAndRecoversFromPanics(t *testing.T) {
	q, _ := openTemp(t, WithBackoff(10*time.Millisecond, 20*time.Millisecond), WithKeepDone(true))
	ok := must[int64](t)(q.Enqueue(bg, "q", []byte("third time lucky")))
	bad := must[int64](t)(q.Enqueue(bg, "q", []byte("always panics"), MaxAttempts(2)))
	perm := must[int64](t)(q.Enqueue(bg, "q", []byte("permanent")))
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go q.Work(ctx, "q", func(ctx context.Context, j *Job) error {
		switch string(j.Payload) {
		case "third time lucky":
			if j.Attempt == 1 {
				return errors.New("first try fails")
			}
			if j.Attempt == 2 {
				panic("second try panics")
			}
			return nil
		case "always panics":
			panic("nope")
		default:
			return Permanent(errors.New("bad input"))
		}
	}, PollInterval(5*time.Millisecond))
	waitFor(t, 10*time.Second, "jobs to settle", queueEmpty(t, q))
	for id, want := range map[int64]struct {
		state    State
		attempts int
		errHas   string
	}{
		ok:   {StateDone, 3, "second try panics"},
		bad:  {StateDead, 2, "panic: nope"},
		perm: {StateDead, 1, "bad input"},
	} {
		info := must[*JobInfo](t)(q.Get(bg, id))
		if info.State != want.state || info.Attempts != want.attempts || !strings.Contains(info.LastError, want.errHas) {
			t.Errorf("job %d: %s after %d attempts, last error %.60q", id, info.State, info.Attempts, info.LastError)
		}
	}
}

func TestWorkRenewsTheLeaseOfALongJob(t *testing.T) {
	q, _ := openTemp(t)
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("slow")))
	var runs atomic.Int32
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go q.Work(ctx, "q", func(ctx context.Context, j *Job) error {
		if runs.Add(1) == 1 {
			close(started)
		}
		time.Sleep(time.Second) // more than three leases
		return nil
	}, Lease(300*time.Millisecond), PollInterval(10*time.Millisecond))
	<-started
	// A second worker keeps trying to take the job while it runs.
	stolen := make(chan *Job, 1)
	go func() {
		for ctx.Err() == nil {
			if j, _ := q.Claim(ctx, "q", time.Minute); j != nil {
				stolen <- j
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	waitFor(t, 10*time.Second, "the slow job", queueEmpty(t, q))
	select {
	case j := <-stolen:
		t.Fatalf("job %d was taken by a second worker while it ran", j.ID)
	default:
	}
	if runs.Load() != 1 {
		t.Fatalf("job %d ran %d times", id, runs.Load())
	}
}

func TestWorkShutdownPutsJobsBack(t *testing.T) {
	q, _ := openTemp(t)
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("stuck")))
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error)
	go func() {
		done <- q.Work(ctx, "q", func(ctx context.Context, j *Job) error {
			close(started)
			<-ctx.Done() // a handler that only stops when told
			return ctx.Err()
		}, ShutdownGrace(50*time.Millisecond))
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Work did not return after shutdown")
	}
	info := must[*JobInfo](t)(q.Get(bg, id))
	if info.State != StateReady || info.Attempts != 0 {
		t.Fatalf("after shutdown the job is %s with %d attempts", info.State, info.Attempts)
	}
}

func TestWorkFinishesRunningJobsWithinGrace(t *testing.T) {
	q, _ := openTemp(t, WithKeepDone(true))
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("almost done")))
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error)
	go func() {
		done <- q.Work(ctx, "q", func(ctx context.Context, j *Job) error {
			close(started)
			time.Sleep(200 * time.Millisecond)
			return nil
		}, ShutdownGrace(5*time.Second))
	}()
	<-started
	cancel()
	<-done
	if info := must[*JobInfo](t)(q.Get(bg, id)); info.State != StateDone {
		t.Fatalf("job is %s", info.State)
	}
}

func TestGoroutinesClaimEachJobOnce(t *testing.T) {
	q, _ := openTemp(t)
	const n, workers = 3000, 16
	for i := 0; i < n; i++ {
		must[int64](t)(q.Enqueue(bg, "q", nil))
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, err := q.Claim(bg, "q", time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if j == nil {
					return
				}
				mu.Lock()
				seen[j.ID]++
				mu.Unlock()
				if err := q.Ack(bg, j); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	took := time.Since(start)
	if len(seen) != n {
		t.Fatalf("%d of %d jobs claimed", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %d claimed %d times", id, c)
		}
	}
	t.Logf("%d jobs claimed and acknowledged by %d goroutines in %s: %.0f jobs/s", n, workers, took.Round(time.Millisecond), float64(n)/took.Seconds())
}
