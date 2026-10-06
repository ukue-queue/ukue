// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Handler does the work of one job. Returning nil marks the job done.
// Returning an error fails the attempt; wrap it with Permanent to stop
// retries, or with RetryAfter to choose the delay. The context is cancelled
// if the worker shuts down past its grace period or loses the job's lease.
type Handler func(ctx context.Context, job *Job) error

type workOptions struct {
	concurrency int
	poll        time.Duration
	lease       time.Duration
	grace       time.Duration
	log         *slog.Logger
}

// WorkOption configures Work.
type WorkOption func(*workOptions)

// Concurrency sets how many jobs run at once. The default is 1.
func Concurrency(n int) WorkOption {
	return func(o *workOptions) { o.concurrency = n }
}

// PollInterval sets the longest an idle worker waits before looking for
// jobs again. Jobs added through the same Queue wake it at once, and it also
// wakes when a delayed job becomes due. The default is 1 second.
func PollInterval(d time.Duration) WorkOption {
	return func(o *workOptions) { o.poll = d }
}

// Lease sets how long a job is held before it goes back on the queue if the
// worker stops answering. While a handler runs, the lease is renewed every
// third of this time. The default is 1 minute.
func Lease(d time.Duration) WorkOption {
	return func(o *workOptions) { o.lease = d }
}

// ShutdownGrace sets how long running handlers may keep going after Work's
// context is cancelled, before their own contexts are cancelled. The default
// is 30 seconds.
func ShutdownGrace(d time.Duration) WorkOption {
	return func(o *workOptions) { o.grace = d }
}

// Logger sets where Work reports what it does. By default it reports nothing.
func Logger(l *slog.Logger) WorkOption {
	return func(o *workOptions) { o.log = l }
}

// Work takes jobs from a queue and runs them with h until ctx is cancelled.
// Then it stops taking jobs, waits for the running ones, and returns nil.
//
// A job whose handler returns nil is acknowledged. A job whose handler
// returns an error or panics is failed. A job that is still running when the
// grace period ends is put back on its queue without counting the attempt,
// provided its handler returns.
func (q *Queue) Work(ctx context.Context, queue string, h Handler, opts ...WorkOption) error {
	if h == nil {
		return invalidf("nil handler")
	}
	if err := checkQueueName(queue); err != nil {
		return err
	}
	o := workOptions{concurrency: 1, poll: time.Second, lease: DefaultLease, grace: 30 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	switch {
	case o.concurrency < 1:
		return invalidf("concurrency must be at least 1")
	case o.poll <= 0:
		return invalidf("poll interval must be positive")
	case o.lease < 3*time.Millisecond:
		return invalidf("lease is too short")
	case o.grace < 0:
		return invalidf("grace period can't be negative")
	}
	if o.log == nil {
		o.log = slog.New(slog.DiscardHandler)
	}

	// Handlers run under jobBase, which outlives ctx by the grace period.
	jobBase, stopJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer stopJobs()
	stopAfter := context.AfterFunc(ctx, func() {
		time.AfterFunc(o.grace, stopJobs)
	})
	defer stopAfter()

	slots := make(chan struct{}, o.concurrency)
	var wg sync.WaitGroup
	failures := 0
	closed := false
loop:
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wake := q.Wake()
		job, err := q.Claim(ctx, queue, o.lease)
		if err != nil {
			<-slots
			if errors.Is(err, ErrClosed) {
				closed = true
				break loop
			}
			if ctx.Err() != nil {
				break loop
			}
			failures++
			wait := min(o.poll*time.Duration(failures), 30*time.Second)
			o.log.Warn("claim failed", "queue", queue, "err", err, "retry_in", wait)
			if !sleep(ctx, wait, nil) {
				break loop
			}
			continue
		}
		failures = 0
		if job == nil {
			<-slots
			wait := o.poll
			if t, ok, err := q.nextChange(ctx, queue); err == nil && ok {
				wait = min(wait, max(time.Until(t), time.Millisecond))
			}
			if !sleep(ctx, wait, wake) {
				break loop
			}
			continue
		}
		o.log.Info("job started", "queue", queue, "id", job.ID, "attempt", job.Attempt)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			q.run(ctx, jobBase, job, h, o)
		}()
	}
	wg.Wait()
	if closed {
		return ErrClosed
	}
	return nil
}

// sleep waits for d, for wake to close, or for ctx to end. It reports false
// if ctx ended.
func sleep(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return ctx.Err() == nil
	case <-t.C:
		return true
	}
}

var errLeaseGone = errors.New("ukue: the job's lease was lost while it ran")

func (q *Queue) run(ctx, jobBase context.Context, job *Job, h Handler, o workOptions) {
	jctx, cancel := context.WithCancelCause(jobBase)
	defer cancel(nil)
	bg := context.WithoutCancel(ctx)

	// The lease is renewed until the job's result is recorded. leaseUntil
	// holds the end of the last renewal that went through.
	var leaseUntil atomic.Int64
	leaseUntil.Store(job.LeaseUntil.UnixNano())
	var hb sync.WaitGroup
	hbStop := make(chan struct{})
	hb.Add(1)
	go func() {
		defer hb.Done()
		t := time.NewTicker(o.lease / 3)
		defer t.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-t.C:
				until, err := q.extend(bg, job.ID, job.Token, o.lease)
				if err == nil {
					leaseUntil.Store(until.UnixNano())
					continue
				}
				if errors.Is(err, ErrLeaseLost) || time.Now().UnixNano() >= leaseUntil.Load() {
					// Another worker may hold the job now, so this one stops.
					cancel(errLeaseGone)
					return
				}
				o.log.Warn("lease renewal failed; trying again", "queue", job.Queue, "id", job.ID, "err", err)
			}
		}
	}()
	stopHeartbeat := func() {
		close(hbStop)
		hb.Wait()
	}

	err := callHandler(jctx, h, job)
	if errors.Is(context.Cause(jctx), errLeaseGone) {
		stopHeartbeat()
		o.log.Warn("job lease lost; another worker may run it again", "queue", job.Queue, "id", job.ID)
		return
	}

	// settle records the result, trying again after passing errors such as
	// a lock held longer than the busy timeout. It gives up when the job is
	// no longer held or its lease runs out, since then the job runs again
	// anyway.
	settle := func(step func() error) error {
		wait := 50 * time.Millisecond
		for {
			serr := step()
			if serr == nil || errors.Is(serr, ErrLeaseLost) || errors.Is(serr, ErrClosed) || errors.Is(serr, ErrInvalid) {
				return serr
			}
			if errors.Is(context.Cause(jctx), errLeaseGone) || time.Now().Add(wait).UnixNano() >= leaseUntil.Load() {
				return serr
			}
			o.log.Warn("could not record the job's result; trying again", "queue", job.Queue, "id", job.ID, "err", serr)
			time.Sleep(wait)
			wait = min(2*wait, 2*time.Second)
		}
	}

	switch {
	case err == nil:
		aerr := settle(func() error { return q.Ack(bg, job) })
		stopHeartbeat()
		if aerr != nil {
			o.log.Warn("job finished but could not be marked done; it may run again", "queue", job.Queue, "id", job.ID, "err", aerr)
			return
		}
		o.log.Info("job done", "queue", job.Queue, "id", job.ID, "attempt", job.Attempt)

	case ctx.Err() != nil && jctx.Err() != nil:
		// Shutting down: give the job back without using up an attempt.
		rerr := settle(func() error { return q.Release(bg, job) })
		stopHeartbeat()
		if rerr != nil {
			o.log.Warn("could not put the job back during shutdown", "queue", job.Queue, "id", job.ID, "err", rerr)
			return
		}
		o.log.Info("job put back for shutdown", "queue", job.Queue, "id", job.ID)

	default:
		var res FailResult
		ferr := settle(func() (e error) {
			res, e = q.Fail(bg, job, err)
			return e
		})
		stopHeartbeat()
		switch {
		case ferr != nil:
			o.log.Warn("job failed but that could not be recorded", "queue", job.Queue, "id", job.ID, "err", ferr, "cause", err)
		case res.State == StateDead:
			o.log.Warn("job dead", "queue", job.Queue, "id", job.ID, "attempt", job.Attempt, "err", err)
		default:
			o.log.Info("job failed, will retry", "queue", job.Queue, "id", job.ID, "attempt", job.Attempt,
				"retry_at", res.RunAt.UTC().Format(time.RFC3339), "err", err)
		}
	}
}

func callHandler(ctx context.Context, h Handler, job *Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}
