// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
	"unicode/utf8"
)

// State is where a job stands.
//
// In the file a job is in one of four states: ready, running, done or dead.
// A ready job whose start time is still in the future is reported as
// delayed.
type State string

const (
	StateReady   State = "ready"   // waiting for a worker, and due now
	StateDelayed State = "delayed" // waiting for its start time
	StateRunning State = "running" // held by a worker under a lease
	StateDone    State = "done"    // finished; kept only with WithKeepDone
	StateDead    State = "dead"    // failed too often, or failed for good
)

// ParseState reads a state name.
func ParseState(s string) (State, error) {
	switch st := State(s); st {
	case StateReady, StateDelayed, StateRunning, StateDone, StateDead:
		return st, nil
	}
	return "", invalidf("unknown state %q (want ready, delayed, running, done or dead)", s)
}

// ErrInvalid is matched, with errors.Is, by every error about a bad
// argument, such as an empty queue name or a negative delay.
var ErrInvalid = errors.New("ukue: invalid argument")

type invalidError struct{ msg string }

func (e *invalidError) Error() string        { return e.msg }
func (e *invalidError) Is(target error) bool { return target == ErrInvalid }

func invalidf(format string, a ...any) error {
	return &invalidError{msg: "ukue: " + fmt.Sprintf(format, a...)}
}

// Job is a job a worker has claimed.
type Job struct {
	ID          int64
	Queue       string
	Payload     []byte
	Attempt     int // this attempt, counting from 1
	MaxAttempts int
	Priority    int
	CreatedAt   time.Time
	LeaseUntil  time.Time // the job goes back on the queue at this time unless the lease is extended
	Token       string    // proves this worker holds the job
	LastError   string    // what the previous attempt reported, if it failed
}

// JobInfo describes a job in any state.
type JobInfo struct {
	ID          int64
	Queue       string
	State       State
	Payload     []byte
	Priority    int
	Attempts    int // attempts started so far
	MaxAttempts int
	RunAt       time.Time // when the job may next start
	LeaseUntil  time.Time // zero unless running
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// QueueStats counts the jobs in one queue.
type QueueStats struct {
	Queue   string
	Ready   int64 // due now
	Delayed int64 // waiting for their start time
	Running int64 // held by a worker whose lease is still valid
	Expired int64 // held by a worker whose lease ran out; they go back on the queue at the next claim
	Dead    int64
	Done    int64
	// OldestDue is the start time of the job that has waited longest among
	// those due now. It is zero when nothing is due.
	OldestDue time.Time
}

// FailResult says what happened to a failed job.
type FailResult struct {
	State State     // StateReady, or StateDead when the job won't be tried again
	RunAt time.Time // when the next attempt may start; zero when dead
}

type enqueueOptions struct {
	delay       time.Duration
	at          time.Time
	maxAttempts int
	priority    int
}

// EnqueueOption sets something about a new job.
type EnqueueOption func(*enqueueOptions)

// Delay makes the job wait d before it can run.
func Delay(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.delay = d }
}

// At makes the job wait until t before it can run.
func At(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.at = t }
}

// MaxAttempts sets how many times this job is tried before it moves to the
// dead-letter state.
func MaxAttempts(n int) EnqueueOption {
	return func(o *enqueueOptions) { o.maxAttempts = n }
}

// Priorities run from MinPriority to MaxPriority. The range is small so
// that finding the next job stays a handful of index lookups.
const (
	MinPriority = -100
	MaxPriority = 100
)

// Priority sets the job's priority, from MinPriority to MaxPriority. Among
// the jobs that are due, workers take the highest priority first, then the
// oldest. The default is 0.
func Priority(p int) EnqueueOption {
	return func(o *enqueueOptions) { o.priority = p }
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks an error as one that trying again won't fix, such as a
// malformed payload. A handler that returns it sends the job straight to
// the dead-letter state.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

type retryAfterError struct {
	err error
	d   time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter asks for the next attempt in d instead of after the usual
// backoff, for example when a service answered "try again in 30 seconds".
// The attempt still counts toward the job's limit.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err, d}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// backoff returns the wait after the given failed attempt, counting from 1.
func (q *Queue) backoff(attempt int) time.Duration {
	d := q.o.backoffMax
	if attempt < 1 {
		attempt = 1
	}
	if attempt <= 62 {
		if b := q.o.backoffBase << (attempt - 1); b > 0 && b < d && b>>(attempt-1) == q.o.backoffBase {
			d = b
		}
	}
	jitter := time.Duration(rand.Int64N(int64(d)/10 + 1))
	if d > math.MaxInt64-jitter {
		return math.MaxInt64
	}
	return d + jitter
}

const maxErrorText = 4000

func errorText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > maxErrorText {
		i := maxErrorText
		for i > 0 && !utf8.RuneStart(s[i]) {
			i--
		}
		s = s[:i] + " [cut]"
	}
	return s
}
