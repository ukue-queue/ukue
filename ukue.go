// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"
)

// Version is the version of this package and of the ukue command.
const Version = "0.1.0"

// FormatVersion is the version of the file layout this package reads and
// writes. It is stored in the ukue_meta table under the key format_version.
const FormatVersion = 1

// ApplicationID is written to the SQLite header of a new standalone ukue
// file. It is the ASCII text "ukue" read as a big-endian number.
const ApplicationID = 0x756B7565

// Defaults used when an option is not given.
const (
	DefaultMaxAttempts = 10
	DefaultBackoffBase = 10 * time.Second
	DefaultBackoffMax  = time.Hour
	DefaultLease       = time.Minute
	DefaultBusyTimeout = 5 * time.Second
)

// MaxQueueNameLength is the longest queue name, in characters.
const MaxQueueNameLength = 200

var (
	// ErrLeaseLost means a worker no longer holds the job it is trying to
	// acknowledge, fail, extend or release. Its lease ran out and the job
	// went back on the queue, or the job was deleted.
	ErrLeaseLost = errors.New("ukue: lease lost: the job is no longer held by this worker")

	// ErrNotFound means no job matched.
	ErrNotFound = errors.New("ukue: job not found")

	// ErrClosed means the Queue was closed.
	ErrClosed = errors.New("ukue: queue is closed")
)

var errNoDriver = errors.New(`ukue: no SQLite driver is registered; import one, for example _ "github.com/mattn/go-sqlite3"`)

// Queue is an open ukue file. It is safe for use by many goroutines, and
// many processes on the same machine can open the same file at once.
type Queue struct {
	db    *sql.DB
	ownDB bool
	o     options
	now   func() time.Time

	mu     sync.Mutex
	wake   chan struct{}
	closed bool
}

type options struct {
	busyTimeout time.Duration
	keepDone    bool
	maxAttempts int
	backoffBase time.Duration
	backoffMax  time.Duration
}

// Option configures a Queue.
type Option func(*options)

// WithBusyTimeout sets how long an operation waits for another connection or
// process to finish writing before it gives up. The default is 5 seconds.
func WithBusyTimeout(d time.Duration) Option {
	return func(o *options) { o.busyTimeout = d }
}

// WithKeepDone keeps finished jobs in the file in the done state instead of
// deleting them. Remove them later with Purge.
func WithKeepDone(keep bool) Option {
	return func(o *options) { o.keepDone = keep }
}

// WithMaxAttempts sets how many times a job is tried before it moves to the
// dead-letter state, for jobs that don't set their own. The default is 10.
func WithMaxAttempts(n int) Option {
	return func(o *options) { o.maxAttempts = n }
}

// WithBackoff sets the delay before a failed job is tried again. The delay
// after the first failure is base, and it doubles after each further failure
// up to max. Up to a tenth more is added at random, so jobs that failed
// together don't all come back at the same moment. The defaults are 10
// seconds and 1 hour.
func WithBackoff(base, max time.Duration) Option {
	return func(o *options) { o.backoffBase, o.backoffMax = base, max }
}

func defaultOptions() options {
	return options{
		busyTimeout: DefaultBusyTimeout,
		maxAttempts: DefaultMaxAttempts,
		backoffBase: DefaultBackoffBase,
		backoffMax:  DefaultBackoffMax,
	}
}

func (o options) validate() error {
	switch {
	case o.busyTimeout < 0:
		return invalidf("busy timeout can't be negative")
	case o.maxAttempts < 1:
		return invalidf("max attempts must be at least 1")
	case o.backoffBase <= 0 || o.backoffMax <= 0:
		return invalidf("backoff delays must be positive")
	case o.backoffMax < o.backoffBase:
		return invalidf("the longest backoff can't be shorter than the first")
	}
	return nil
}

// Open opens the ukue file at path, creating it if it doesn't exist. It uses
// whichever SQLite driver the program registered under the name "sqlite3" or
// "sqlite". The path is passed to the driver as it is.
func Open(path string, opts ...Option) (*Queue, error) {
	if path == "" {
		return nil, invalidf("empty file path")
	}
	name := driverName()
	if name == "" {
		return nil, errNoDriver
	}
	db, err := sql.Open(name, path)
	if err != nil {
		return nil, fmt.Errorf("ukue: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	q, err := newQueue(db, true, opts)
	if err != nil {
		db.Close()
		return nil, err
	}
	return q, nil
}

// OpenDB uses a database the program already opened. The ukue tables are
// created in it if they aren't there yet, next to any tables of its own,
// which lets EnqueueTx add a job in the same transaction as the program's own
// data. Close doesn't close db.
func OpenDB(db *sql.DB, opts ...Option) (*Queue, error) {
	if db == nil {
		return nil, invalidf("nil database")
	}
	return newQueue(db, false, opts)
}

func driverName() string {
	drivers := sql.Drivers()
	for _, name := range []string{"sqlite3", "sqlite"} {
		if slices.Contains(drivers, name) {
			return name
		}
	}
	return ""
}

func newQueue(db *sql.DB, own bool, opts []Option) (*Queue, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	q := &Queue{db: db, ownDB: own, o: o, now: time.Now, wake: make(chan struct{})}
	if err := q.init(context.Background(), own); err != nil {
		return nil, err
	}
	return q, nil
}

// Close closes the file, unless the Queue was made with OpenDB.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.wake)
	q.mu.Unlock()
	if q.ownDB {
		return q.db.Close()
	}
	return nil
}

// DB returns the database the Queue uses.
func (q *Queue) DB() *sql.DB { return q.db }

// Wake returns a channel that is closed the next time this Queue adds a job
// or puts one back on a queue. Jobs added by other processes don't close it,
// so waiting code should also poll.
func (q *Queue) Wake() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.wake
}

func (q *Queue) signal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	close(q.wake)
	q.wake = make(chan struct{})
}

func (q *Queue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// conn takes a connection from the pool and sets it up for one operation.
// The settings are made each time because database/sql may hand out a new
// connection, and other drivers don't share go-sqlite3's defaults.
func (q *Queue) conn(ctx context.Context) (*sql.Conn, error) {
	if q.isClosed() {
		return nil, ErrClosed
	}
	c, err := q.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("ukue: %w", err)
	}
	ms := strconv.FormatInt(q.o.busyTimeout.Milliseconds(), 10)
	// FULL makes every commit durable, which a queue needs: a job that was
	// added must still be there after a power cut. go-sqlite3 builds SQLite
	// with NORMAL as the default for WAL files, so it is set here.
	for _, pragma := range []string{"PRAGMA busy_timeout = " + ms, "PRAGMA synchronous = FULL"} {
		if _, err := c.ExecContext(ctx, pragma); err != nil {
			c.Close()
			return nil, fmt.Errorf("ukue: %s: %w", pragma, err)
		}
	}
	return c, nil
}

// exec runs one statement in its own transaction.
func (q *Queue) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	c, err := q.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.ExecContext(ctx, query, args...)
}

// writeTx runs fn inside BEGIN IMMEDIATE, which takes the write lock at the
// start so two workers can't both read the same job as free.
func (q *Queue) writeTx(ctx context.Context, fn func(c *sql.Conn) error) error {
	c, err := q.conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("ukue: begin: %w", err)
	}
	if err := fn(c); err != nil {
		rollback(c)
		return err
	}
	// The commit must not be cut short by a cancelled context.
	if _, err := c.ExecContext(context.WithoutCancel(ctx), "COMMIT"); err != nil {
		rollback(c)
		return fmt.Errorf("ukue: commit: %w", err)
	}
	return nil
}

// rollback ends an open transaction. If that fails, the connection is thrown
// away so it can't go back to the pool with a transaction still open.
func rollback(c *sql.Conn) {
	if _, err := c.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		_ = c.Raw(func(any) error { return driver.ErrBadConn })
	}
}

// The schema of format version 1. FORMAT.md describes it for other writers.
var schema = []string{
	`CREATE TABLE ukue_meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
) WITHOUT ROWID`,
	`CREATE TABLE ukue_jobs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	queue        TEXT    NOT NULL CHECK (length(queue) BETWEEN 1 AND 200),
	payload      BLOB    NOT NULL,
	state        TEXT    NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'running', 'done', 'dead')),
	priority     INTEGER NOT NULL DEFAULT 0,
	attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
	max_attempts INTEGER NOT NULL DEFAULT 10 CHECK (max_attempts >= 1),
	run_at       INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
	lease_until  INTEGER,
	token        TEXT,
	last_error   TEXT,
	created_at   INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
	updated_at   INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER))
)`,
	`CREATE INDEX ukue_jobs_due ON ukue_jobs (queue, priority, run_at, id) WHERE state = 'ready'`,
	`CREATE INDEX ukue_jobs_next ON ukue_jobs (queue, run_at) WHERE state = 'ready'`,
	`CREATE INDEX ukue_jobs_leases ON ukue_jobs (lease_until) WHERE state = 'running'`,
	`CREATE INDEX ukue_jobs_state ON ukue_jobs (state, queue, id)`,
}

func (q *Queue) init(ctx context.Context, standalone bool) error {
	c, err := q.conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	// WAL lets readers carry on while a writer commits. The mode is stored
	// in the file, so every program that opens it later uses WAL too. On a
	// file system without shared memory SQLite keeps its old mode, and ukue
	// still works, with less concurrency.
	var mode string
	if err := c.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("ukue: read journal mode: %w", err)
	}
	if mode != "wal" && mode != "memory" {
		if err := c.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
			return fmt.Errorf("ukue: set journal mode: %w", err)
		}
	}

	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("ukue: begin: %w", err)
	}
	if err := q.initSchema(ctx, c, standalone); err != nil {
		rollback(c)
		return err
	}
	if _, err := c.ExecContext(ctx, "COMMIT"); err != nil {
		rollback(c)
		return fmt.Errorf("ukue: commit: %w", err)
	}
	return nil
}

func (q *Queue) initSchema(ctx context.Context, c *sql.Conn, standalone bool) error {
	var n int
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'ukue_meta'`).Scan(&n); err != nil {
		return fmt.Errorf("ukue: read schema: %w", err)
	}
	if n == 1 {
		var v string
		err := c.QueryRowContext(ctx, `SELECT value FROM ukue_meta WHERE key = 'format_version'`).Scan(&v)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("ukue: not a ukue file: the ukue_meta table has no format_version")
		}
		if err != nil {
			return fmt.Errorf("ukue: read format version: %w", err)
		}
		ver, err := strconv.Atoi(v)
		if err != nil || ver < 1 {
			return fmt.Errorf("ukue: not a ukue file: format_version is %q", v)
		}
		if ver > FormatVersion {
			return fmt.Errorf("ukue: the file uses format version %d, and this ukue reads up to version %d; update ukue", ver, FormatVersion)
		}
		return nil
	}

	if standalone {
		// Mark a brand-new file as a ukue file in the SQLite header. A
		// database that already holds other tables keeps its own mark.
		var tables int
		var appID int64
		if err := c.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master`).Scan(&tables); err != nil {
			return fmt.Errorf("ukue: read schema: %w", err)
		}
		if err := c.QueryRowContext(ctx, `PRAGMA application_id`).Scan(&appID); err != nil {
			return fmt.Errorf("ukue: read application id: %w", err)
		}
		if tables == 0 && appID == 0 {
			if _, err := c.ExecContext(ctx, "PRAGMA application_id = "+strconv.Itoa(ApplicationID)); err != nil {
				return fmt.Errorf("ukue: set application id: %w", err)
			}
		}
	}

	for _, stmt := range schema {
		if _, err := c.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ukue: create schema: %w", err)
		}
	}
	now := strconv.FormatInt(q.now().UnixMilli(), 10)
	for _, kv := range [][2]string{
		{"format_version", strconv.Itoa(FormatVersion)},
		{"created_at", now},
		{"created_by", "ukue " + Version + " (Go)"},
	} {
		if _, err := c.ExecContext(ctx, `INSERT INTO ukue_meta (key, value) VALUES (?, ?)`, kv[0], kv[1]); err != nil {
			return fmt.Errorf("ukue: write ukue_meta: %w", err)
		}
	}
	return nil
}
