// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const insertJob = `INSERT INTO ukue_jobs
	(queue, payload, state, priority, attempts, max_attempts, run_at, created_at, updated_at)
	VALUES (?, ?, 'ready', ?, 0, ?, ?, ?, ?)`

func checkQueueName(queue string) error {
	if !utf8.ValidString(queue) {
		return invalidf("queue name is not valid UTF-8")
	}
	if strings.ContainsRune(queue, 0) {
		return invalidf("queue name contains a NUL character")
	}
	switch n := utf8.RuneCountInString(queue); {
	case n == 0:
		return invalidf("empty queue name")
	case n > MaxQueueNameLength:
		return invalidf("queue name is longer than %d characters", MaxQueueNameLength)
	}
	return nil
}

func (q *Queue) insertArgs(queue string, payload []byte, opts []EnqueueOption) ([]any, error) {
	if err := checkQueueName(queue); err != nil {
		return nil, err
	}
	var o enqueueOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.maxAttempts == 0 {
		o.maxAttempts = q.o.maxAttempts
	}
	switch {
	case o.maxAttempts < 1:
		return nil, invalidf("max attempts must be at least 1")
	case o.delay < 0:
		return nil, invalidf("delay can't be negative")
	case o.delay > 0 && !o.at.IsZero():
		return nil, invalidf("give a delay or a start time, not both")
	case o.priority < MinPriority || o.priority > MaxPriority:
		return nil, invalidf("priority must be from %d to %d", MinPriority, MaxPriority)
	}
	now := q.now()
	runAt := now.Add(o.delay)
	if !o.at.IsZero() {
		runAt = o.at
	}
	if payload == nil {
		payload = []byte{}
	}
	nowMs := now.UnixMilli()
	return []any{queue, payload, o.priority, o.maxAttempts, runAt.UnixMilli(), nowMs, nowMs}, nil
}

// Enqueue adds a job to a queue and returns its ID. The payload is stored
// exactly as given; ukue never looks inside it.
func (q *Queue) Enqueue(ctx context.Context, queue string, payload []byte, opts ...EnqueueOption) (int64, error) {
	args, err := q.insertArgs(queue, payload, opts)
	if err != nil {
		return 0, err
	}
	res, err := q.exec(ctx, insertJob, args...)
	if err != nil {
		return 0, fmt.Errorf("ukue: enqueue: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("ukue: enqueue: %w", err)
	}
	q.signal()
	return id, nil
}

// EnqueueTx adds a job inside a transaction the program already started on
// the same database, so the job exists only if the rest of the transaction
// commits. The Queue must have been made with OpenDB on that database.
// Workers find the job at their next look after the commit. The commit is as
// durable as the program's own connection makes it, so set synchronous = FULL
// there too if the job must survive a power cut.
func (q *Queue) EnqueueTx(ctx context.Context, tx *sql.Tx, queue string, payload []byte, opts ...EnqueueOption) (int64, error) {
	args, err := q.insertArgs(queue, payload, opts)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, insertJob, args...)
	if err != nil {
		return 0, fmt.Errorf("ukue: enqueue: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("ukue: enqueue: %w", err)
	}
	q.signal()
	return id, nil
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand doesn't fail on supported systems
	}
	return hex.EncodeToString(b[:])
}

const leaseExpiredText = "lease expired: the worker stopped, or lost touch, before finishing the job"

// Claim takes the next job that is due on a queue and holds it under a lease
// of the given length. It returns nil and no error when nothing is due.
//
// Among the due jobs, Claim takes the highest priority first, and within a
// priority the job that has waited longest. Before looking, it puts back
// every job whose lease has run out, in any queue. Each of those counts as a
// failed attempt, so a job that keeps crashing its worker ends up in the
// dead-letter state.
func (q *Queue) Claim(ctx context.Context, queue string, lease time.Duration) (*Job, error) {
	if err := checkQueueName(queue); err != nil {
		return nil, err
	}
	if lease == 0 {
		lease = DefaultLease
	}
	if lease < time.Millisecond {
		return nil, invalidf("lease must be at least a millisecond")
	}
	// Look first without the write lock, so idle workers polling an empty
	// queue don't hold up the programs adding jobs.
	if ok, err := q.mayHaveWork(ctx, queue); err != nil || !ok {
		return nil, err
	}
	var job *Job
	err := q.writeTx(ctx, func(c *sql.Conn) error {
		now := q.now()
		nowMs := now.UnixMilli()
		if err := q.reap(ctx, c, nowMs); err != nil {
			return err
		}
		id, ok, err := findDue(ctx, c, queue, nowMs)
		if err != nil || !ok {
			return err
		}
		var j Job
		var createdMs int64
		var lastErr sql.NullString
		err = c.QueryRowContext(ctx, `SELECT payload, attempts, max_attempts, priority, created_at, last_error
			FROM ukue_jobs WHERE id = ?`, id).Scan(&j.Payload, &j.Attempt, &j.MaxAttempts, &j.Priority, &createdMs, &lastErr)
		if err != nil {
			return fmt.Errorf("ukue: claim: %w", err)
		}
		token := newToken()
		untilMs := now.Add(lease).UnixMilli()
		if _, err := c.ExecContext(ctx, `UPDATE ukue_jobs
			SET state = 'running', attempts = attempts + 1, lease_until = ?, token = ?, updated_at = ?
			WHERE id = ?`, untilMs, token, nowMs, id); err != nil {
			return fmt.Errorf("ukue: claim: %w", err)
		}
		j.ID = id
		j.Queue = queue
		j.Attempt++
		j.Token = token
		j.LeaseUntil = time.UnixMilli(untilMs)
		j.CreatedAt = time.UnixMilli(createdMs)
		j.LastError = lastErr.String
		if j.Payload == nil {
			j.Payload = []byte{}
		}
		job = &j
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// mayHaveWork reports whether a claim could find something: a due job in
// the queue, or an expired lease anywhere.
func (q *Queue) mayHaveWork(ctx context.Context, queue string) (bool, error) {
	c, err := q.conn(ctx)
	if err != nil {
		return false, err
	}
	defer c.Close()
	nowMs := q.now().UnixMilli()
	var due, expired bool
	err = c.QueryRowContext(ctx, `SELECT
		EXISTS (SELECT 1 FROM ukue_jobs WHERE queue = ? AND state = 'ready' AND run_at <= ?),
		EXISTS (SELECT 1 FROM ukue_jobs WHERE state = 'running' AND (lease_until IS NULL OR lease_until <= ?))`,
		queue, nowMs, nowMs).Scan(&due, &expired)
	if err != nil {
		return false, fmt.Errorf("ukue: claim: %w", err)
	}
	return due || expired, nil
}

// findDue returns the ID of the job to claim next. It goes down the queue's
// priorities from the highest, and in each one looks for the oldest due
// job. Every step is an index lookup, and there are at most 201 priorities,
// so a long list of jobs scheduled for later doesn't slow it down.
func findDue(ctx context.Context, c *sql.Conn, queue string, nowMs int64) (int64, bool, error) {
	var pr sql.NullInt64
	if err := c.QueryRowContext(ctx, `SELECT max(priority) FROM ukue_jobs WHERE queue = ? AND state = 'ready'`, queue).Scan(&pr); err != nil {
		return 0, false, fmt.Errorf("ukue: claim: %w", err)
	}
	for pr.Valid {
		var id int64
		err := c.QueryRowContext(ctx, `SELECT id FROM ukue_jobs
			WHERE queue = ? AND state = 'ready' AND priority = ? AND run_at <= ?
			ORDER BY run_at, id LIMIT 1`, queue, pr.Int64, nowMs).Scan(&id)
		if err == nil {
			return id, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, false, fmt.Errorf("ukue: claim: %w", err)
		}
		if err := c.QueryRowContext(ctx, `SELECT max(priority) FROM ukue_jobs
			WHERE queue = ? AND state = 'ready' AND priority < ?`, queue, pr.Int64).Scan(&pr); err != nil {
			return 0, false, fmt.Errorf("ukue: claim: %w", err)
		}
	}
	return 0, false, nil
}

// reap puts back running jobs whose lease ran out. It runs inside Claim's
// write transaction.
func (q *Queue) reap(ctx context.Context, c *sql.Conn, nowMs int64) error {
	rows, err := c.QueryContext(ctx, `SELECT id, attempts, max_attempts FROM ukue_jobs
		WHERE state = 'running' AND (lease_until IS NULL OR lease_until <= ?)`, nowMs)
	if err != nil {
		return fmt.Errorf("ukue: find expired leases: %w", err)
	}
	type expired struct {
		id                    int64
		attempts, maxAttempts int
	}
	var list []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.attempts, &e.maxAttempts); err != nil {
			rows.Close()
			return fmt.Errorf("ukue: find expired leases: %w", err)
		}
		list = append(list, e)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("ukue: find expired leases: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ukue: find expired leases: %w", err)
	}
	for _, e := range list {
		var err error
		if e.attempts >= e.maxAttempts {
			_, err = c.ExecContext(ctx, `UPDATE ukue_jobs
				SET state = 'dead', lease_until = NULL, token = NULL, last_error = ?, updated_at = ?
				WHERE id = ? AND state = 'running'`, leaseExpiredText, nowMs, e.id)
		} else {
			runAt := nowMs + q.backoff(e.attempts).Milliseconds()
			_, err = c.ExecContext(ctx, `UPDATE ukue_jobs
				SET state = 'ready', run_at = ?, lease_until = NULL, token = NULL, last_error = ?, updated_at = ?
				WHERE id = ? AND state = 'running'`, runAt, leaseExpiredText, nowMs, e.id)
		}
		if err != nil {
			return fmt.Errorf("ukue: put back expired job %d: %w", e.id, err)
		}
	}
	return nil
}

func oneRow(res sql.Result, err error, op string, none error) error {
	if err != nil {
		return fmt.Errorf("ukue: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("ukue: %s: %w", op, err)
	}
	if n == 0 {
		return none
	}
	return nil
}

// Ack records that a job finished. The job is deleted, or kept as done with
// WithKeepDone. It returns ErrLeaseLost if this worker no longer holds the job.
func (q *Queue) Ack(ctx context.Context, job *Job) error {
	if job == nil {
		return invalidf("ack: nil job")
	}
	var res sql.Result
	var err error
	if q.o.keepDone {
		res, err = q.exec(ctx, `UPDATE ukue_jobs
			SET state = 'done', lease_until = NULL, token = NULL, updated_at = ?
			WHERE id = ? AND state = 'running' AND token = ?`, q.now().UnixMilli(), job.ID, job.Token)
	} else {
		res, err = q.exec(ctx, `DELETE FROM ukue_jobs WHERE id = ? AND state = 'running' AND token = ?`, job.ID, job.Token)
	}
	return oneRow(res, err, "ack", ErrLeaseLost)
}

// Fail records that an attempt failed, with cause as the reason. The job
// waits out the backoff and goes back on its queue, or moves to the
// dead-letter state when it has used all its attempts or cause is marked
// Permanent. It returns ErrLeaseLost if this worker no longer holds the job.
func (q *Queue) Fail(ctx context.Context, job *Job, cause error) (FailResult, error) {
	if job == nil {
		return FailResult{}, invalidf("fail: nil job")
	}
	now := q.now()
	nowMs := now.UnixMilli()
	msg := errorText(cause)
	if msg == "" {
		msg = "failed"
	}
	if IsPermanent(cause) || job.Attempt >= job.MaxAttempts {
		res, err := q.exec(ctx, `UPDATE ukue_jobs
			SET state = 'dead', lease_until = NULL, token = NULL, last_error = ?, updated_at = ?
			WHERE id = ? AND state = 'running' AND token = ?`, msg, nowMs, job.ID, job.Token)
		if err := oneRow(res, err, "fail", ErrLeaseLost); err != nil {
			return FailResult{}, err
		}
		return FailResult{State: StateDead}, nil
	}
	delay := q.backoff(job.Attempt)
	var ra *retryAfterError
	if errors.As(cause, &ra) {
		delay = max(ra.d, 0)
	}
	runAtMs := now.Add(delay).UnixMilli()
	res, err := q.exec(ctx, `UPDATE ukue_jobs
		SET state = 'ready', run_at = ?, lease_until = NULL, token = NULL, last_error = ?, updated_at = ?
		WHERE id = ? AND state = 'running' AND token = ?`, runAtMs, msg, nowMs, job.ID, job.Token)
	if err := oneRow(res, err, "fail", ErrLeaseLost); err != nil {
		return FailResult{}, err
	}
	if delay == 0 {
		q.signal()
	}
	return FailResult{State: StateReady, RunAt: time.UnixMilli(runAtMs)}, nil
}

// Extend renews a job's lease so it runs out lease from now. Work does this
// on its own while a handler runs. It returns ErrLeaseLost if this worker no
// longer holds the job.
func (q *Queue) Extend(ctx context.Context, job *Job, lease time.Duration) error {
	if job == nil {
		return invalidf("extend: nil job")
	}
	until, err := q.extend(ctx, job.ID, job.Token, lease)
	if err != nil {
		return err
	}
	job.LeaseUntil = until
	return nil
}

func (q *Queue) extend(ctx context.Context, id int64, token string, lease time.Duration) (time.Time, error) {
	if lease == 0 {
		lease = DefaultLease
	}
	if lease < time.Millisecond {
		return time.Time{}, invalidf("lease must be at least a millisecond")
	}
	now := q.now()
	untilMs := now.Add(lease).UnixMilli()
	res, err := q.exec(ctx, `UPDATE ukue_jobs SET lease_until = ?, updated_at = ?
		WHERE id = ? AND state = 'running' AND token = ?`, untilMs, now.UnixMilli(), id, token)
	if err := oneRow(res, err, "extend", ErrLeaseLost); err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(untilMs), nil
}

// Release puts a claimed job straight back on its queue without counting
// the attempt, for a worker that is shutting down.
func (q *Queue) Release(ctx context.Context, job *Job) error {
	if job == nil {
		return invalidf("release: nil job")
	}
	nowMs := q.now().UnixMilli()
	res, err := q.exec(ctx, `UPDATE ukue_jobs
		SET state = 'ready', attempts = max(attempts - 1, 0), run_at = ?, lease_until = NULL, token = NULL, updated_at = ?
		WHERE id = ? AND state = 'running' AND token = ?`, nowMs, nowMs, job.ID, job.Token)
	if err := oneRow(res, err, "release", ErrLeaseLost); err != nil {
		return err
	}
	q.signal()
	return nil
}

// Retry moves a dead job back to ready with its attempts reset. It returns
// ErrNotFound if there is no dead job with that ID.
func (q *Queue) Retry(ctx context.Context, id int64) error {
	nowMs := q.now().UnixMilli()
	res, err := q.exec(ctx, `UPDATE ukue_jobs
		SET state = 'ready', attempts = 0, run_at = ?, updated_at = ?
		WHERE id = ? AND state = 'dead'`, nowMs, nowMs, id)
	if err := oneRow(res, err, "retry", ErrNotFound); err != nil {
		return err
	}
	q.signal()
	return nil
}

// RetryAll moves every dead job in a queue back to ready, or in all queues
// when queue is empty. It returns how many jobs it moved.
func (q *Queue) RetryAll(ctx context.Context, queue string) (int64, error) {
	nowMs := q.now().UnixMilli()
	query := `UPDATE ukue_jobs SET state = 'ready', attempts = 0, run_at = ?, updated_at = ? WHERE state = 'dead'`
	args := []any{nowMs, nowMs}
	if queue != "" {
		query += ` AND queue = ?`
		args = append(args, queue)
	}
	res, err := q.exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("ukue: retry: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("ukue: retry: %w", err)
	}
	if n > 0 {
		q.signal()
	}
	return n, nil
}

// Purge deletes the dead or done jobs of a queue, or of all queues when queue
// is empty. It returns how many jobs it deleted.
func (q *Queue) Purge(ctx context.Context, state State, queue string) (int64, error) {
	if state != StateDead && state != StateDone {
		return 0, invalidf("purge deletes dead or done jobs only")
	}
	query := `DELETE FROM ukue_jobs WHERE state = ?`
	args := []any{string(state)}
	if queue != "" {
		query += ` AND queue = ?`
		args = append(args, queue)
	}
	res, err := q.exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("ukue: purge: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("ukue: purge: %w", err)
	}
	return n, nil
}

// Delete removes one job in any state. A worker that holds it gets
// ErrLeaseLost when it reports back.
func (q *Queue) Delete(ctx context.Context, id int64) error {
	res, err := q.exec(ctx, `DELETE FROM ukue_jobs WHERE id = ?`, id)
	return oneRow(res, err, "delete", ErrNotFound)
}

const infoColumns = `id, queue, state, payload, priority, attempts, max_attempts, run_at, lease_until, last_error, created_at, updated_at`

func scanInfo(sc interface{ Scan(...any) error }, nowMs int64) (JobInfo, error) {
	var ji JobInfo
	var state string
	var runAt, created, updated int64
	var lease sql.NullInt64
	var lastErr sql.NullString
	if err := sc.Scan(&ji.ID, &ji.Queue, &state, &ji.Payload, &ji.Priority, &ji.Attempts, &ji.MaxAttempts,
		&runAt, &lease, &lastErr, &created, &updated); err != nil {
		return JobInfo{}, err
	}
	ji.State = State(state)
	if ji.State == StateReady && runAt > nowMs {
		ji.State = StateDelayed
	}
	if ji.Payload == nil {
		ji.Payload = []byte{}
	}
	ji.RunAt = time.UnixMilli(runAt)
	if lease.Valid {
		ji.LeaseUntil = time.UnixMilli(lease.Int64)
	}
	ji.LastError = lastErr.String
	ji.CreatedAt = time.UnixMilli(created)
	ji.UpdatedAt = time.UnixMilli(updated)
	return ji, nil
}

// Get returns one job. It returns ErrNotFound if there is no job with that ID.
func (q *Queue) Get(ctx context.Context, id int64) (*JobInfo, error) {
	c, err := q.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	ji, err := scanInfo(c.QueryRowContext(ctx, `SELECT `+infoColumns+` FROM ukue_jobs WHERE id = ?`, id), q.now().UnixMilli())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ukue: get: %w", err)
	}
	return &ji, nil
}

// ListFilter chooses which jobs List returns.
type ListFilter struct {
	Queue   string // one queue, or all when empty
	State   State  // one state, or all when empty
	AfterID int64  // only jobs with a larger ID, for paging
	Limit   int    // at most this many; 100 when zero, and never more than 1000
}

// List returns jobs in ID order.
func (q *Queue) List(ctx context.Context, f ListFilter) ([]JobInfo, error) {
	nowMs := q.now().UnixMilli()
	where := []string{"id > ?"}
	args := []any{f.AfterID}
	if f.Queue != "" {
		where = append(where, "queue = ?")
		args = append(args, f.Queue)
	}
	switch f.State {
	case "":
	case StateReady:
		where = append(where, "state = 'ready' AND run_at <= ?")
		args = append(args, nowMs)
	case StateDelayed:
		where = append(where, "state = 'ready' AND run_at > ?")
		args = append(args, nowMs)
	case StateRunning, StateDone, StateDead:
		where = append(where, "state = ?")
		args = append(args, string(f.State))
	default:
		return nil, invalidf("unknown state %q", f.State)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 1000)
	args = append(args, limit)

	c, err := q.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rows, err := c.QueryContext(ctx, `SELECT `+infoColumns+` FROM ukue_jobs WHERE `+strings.Join(where, " AND ")+` ORDER BY id LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("ukue: list: %w", err)
	}
	defer rows.Close()
	var out []JobInfo
	for rows.Next() {
		ji, err := scanInfo(rows, nowMs)
		if err != nil {
			return nil, fmt.Errorf("ukue: list: %w", err)
		}
		out = append(out, ji)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ukue: list: %w", err)
	}
	return out, nil
}

// Stats counts the jobs in every queue that has any.
func (q *Queue) Stats(ctx context.Context) ([]QueueStats, error) {
	nowMs := q.now().UnixMilli()
	c, err := q.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	rows, err := c.QueryContext(ctx, `SELECT queue,
		sum(state = 'ready' AND run_at <= ?),
		sum(state = 'ready' AND run_at > ?),
		sum(state = 'running' AND lease_until > ?),
		sum(state = 'running' AND (lease_until IS NULL OR lease_until <= ?)),
		sum(state = 'dead'),
		sum(state = 'done'),
		min(CASE WHEN state = 'ready' AND run_at <= ? THEN run_at END)
		FROM ukue_jobs GROUP BY queue ORDER BY queue`, nowMs, nowMs, nowMs, nowMs, nowMs)
	if err != nil {
		return nil, fmt.Errorf("ukue: stats: %w", err)
	}
	defer rows.Close()
	var out []QueueStats
	for rows.Next() {
		var s QueueStats
		var oldest sql.NullInt64
		if err := rows.Scan(&s.Queue, &s.Ready, &s.Delayed, &s.Running, &s.Expired, &s.Dead, &s.Done, &oldest); err != nil {
			return nil, fmt.Errorf("ukue: stats: %w", err)
		}
		if oldest.Valid {
			s.OldestDue = time.UnixMilli(oldest.Int64)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ukue: stats: %w", err)
	}
	return out, nil
}

// nextChange returns the earliest time a job in the queue becomes due or any
// lease runs out, so an idle worker knows when to look again.
func (q *Queue) nextChange(ctx context.Context, queue string) (time.Time, bool, error) {
	c, err := q.conn(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	defer c.Close()
	var t sql.NullInt64
	// SQLite's two-argument min() is NULL if either side is, hence coalesce.
	err = c.QueryRowContext(ctx, `SELECT min(coalesce(d, l), coalesce(l, d)) FROM (SELECT
		(SELECT min(run_at) FROM ukue_jobs WHERE queue = ? AND state = 'ready') AS d,
		(SELECT min(lease_until) FROM ukue_jobs WHERE state = 'running') AS l)`, queue).Scan(&t)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("ukue: %w", err)
	}
	if !t.Valid {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(t.Int64), true, nil
}
