// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var bg = context.Background()

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func openTemp(t testing.TB, opts ...Option) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jobs.ukue")
	q, err := Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q, path
}

// newClockQueue returns a Queue whose clock moves only when the test says.
func newClockQueue(t testing.TB, opts ...Option) (*Queue, *clock) {
	t.Helper()
	q, _ := openTemp(t, opts...)
	c := &clock{t: time.UnixMilli(1_790_000_000_000)}
	q.now = c.now
	return q, c
}

func must[T any](t testing.TB) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func integrity(t testing.TB, q *Queue) string {
	t.Helper()
	var s string
	if err := q.DB().QueryRow(`PRAGMA integrity_check`).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func claim(t testing.TB, q *Queue, queue string, lease time.Duration) *Job {
	t.Helper()
	j, err := q.Claim(bg, queue, lease)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestOpenWritesFormat(t *testing.T) {
	q, path := openTemp(t)
	var mode, ver string
	var appID int64
	for _, r := range []struct {
		query string
		dst   any
	}{
		{"PRAGMA journal_mode", &mode},
		{"PRAGMA application_id", &appID},
		{"SELECT value FROM ukue_meta WHERE key = 'format_version'", &ver},
	} {
		if err := q.DB().QueryRow(r.query).Scan(r.dst); err != nil {
			t.Fatal(err)
		}
	}
	if mode != "wal" || appID != ApplicationID || ver != "1" {
		t.Fatalf("journal_mode %q, application_id %d, format_version %q", mode, appID, ver)
	}
	q.Close()

	// Opening again keeps everything.
	q2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// A newer format is refused, with a clear message.
	if _, err := q2.DB().Exec("UPDATE ukue_meta SET value = '2' WHERE key = 'format_version'"); err != nil {
		t.Fatal(err)
	}
	q2.Close()
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "format version 2") {
		t.Fatalf("want a format version error, got %v", err)
	}
}

func TestOpenDBSharesAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)"); err != nil {
		t.Fatal(err)
	}
	q, err := OpenDB(db)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var appID int64
	db.QueryRow("PRAGMA application_id").Scan(&appID)
	if appID != 0 {
		t.Fatalf("a shared database's application_id was changed to %d", appID)
	}

	// The job exists only if the transaction commits.
	tx, _ := db.Begin()
	tx.Exec("INSERT INTO orders (item) VALUES ('lamp')")
	if _, err := q.EnqueueTx(bg, tx, "receipts", []byte("order 1")); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if j := claim(t, q, "receipts", time.Minute); j != nil {
		t.Fatal("a job from a rolled-back transaction was claimed")
	}
	tx, _ = db.Begin()
	tx.Exec("INSERT INTO orders (item) VALUES ('desk')")
	if _, err := q.EnqueueTx(bg, tx, "receipts", []byte("order 2")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	j := claim(t, q, "receipts", time.Minute)
	if j == nil || string(j.Payload) != "order 2" {
		t.Fatalf("got %+v", j)
	}
}

func TestEnqueueClaimAck(t *testing.T) {
	q, c := newClockQueue(t)
	for i := 1; i <= 3; i++ {
		must[int64](t)(q.Enqueue(bg, "email", []byte(fmt.Sprintf("job %d", i))))
		c.add(time.Millisecond)
	}
	must[int64](t)(q.Enqueue(bg, "other", nil))
	for i := 1; i <= 3; i++ {
		j := claim(t, q, "email", time.Minute)
		if j == nil || string(j.Payload) != fmt.Sprintf("job %d", i) || j.Attempt != 1 || j.Queue != "email" {
			t.Fatalf("claim %d: got %+v", i, j)
		}
		if err := q.Ack(bg, j); err != nil {
			t.Fatal(err)
		}
		if err := q.Ack(bg, j); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("second ack: want ErrLeaseLost, got %v", err)
		}
	}
	if j := claim(t, q, "email", time.Minute); j != nil {
		t.Fatalf("queue should be empty, got job %d", j.ID)
	}
	if j := claim(t, q, "other", time.Minute); j == nil || len(j.Payload) != 0 || j.Payload == nil {
		t.Fatalf("nil payload should come back as empty, got %+v", j)
	}
}

func TestPriorityThenAge(t *testing.T) {
	q, c := newClockQueue(t)
	add := func(p int, name string) {
		must[int64](t)(q.Enqueue(bg, "q", []byte(name), Priority(p)))
		c.add(time.Millisecond)
	}
	add(0, "a")
	add(5, "b")
	add(-1, "c")
	add(5, "d")
	add(0, "e")
	var got []string
	for {
		j := claim(t, q, "q", time.Minute)
		if j == nil {
			break
		}
		got = append(got, string(j.Payload))
	}
	if strings.Join(got, "") != "bdaec" {
		t.Fatalf("order %v, want b d a e c", got)
	}
}

func TestDelayAndAt(t *testing.T) {
	q, c := newClockQueue(t)
	must[int64](t)(q.Enqueue(bg, "q", []byte("later"), Delay(10*time.Second)))
	must[int64](t)(q.Enqueue(bg, "q", []byte("at"), At(c.now().Add(20*time.Second))))
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("a delayed job ran early")
	}
	c.add(9999 * time.Millisecond)
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("a delayed job ran 1 ms early")
	}
	c.add(time.Millisecond)
	if j := claim(t, q, "q", time.Minute); j == nil || string(j.Payload) != "later" {
		t.Fatalf("got %+v", j)
	}
	c.add(10 * time.Second)
	if j := claim(t, q, "q", time.Minute); j == nil || string(j.Payload) != "at" {
		t.Fatalf("got %+v", j)
	}
}

func TestFailBacksOffThenDies(t *testing.T) {
	q, c := newClockQueue(t, WithBackoff(10*time.Second, 15*time.Second))
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("x"), MaxAttempts(3)))
	want := []time.Duration{10 * time.Second, 15 * time.Second} // 20 s is capped at 15
	for attempt := 1; attempt <= 3; attempt++ {
		j := claim(t, q, "q", time.Minute)
		if j == nil || j.ID != id || j.Attempt != attempt {
			t.Fatalf("attempt %d: got %+v", attempt, j)
		}
		if attempt > 1 && j.LastError != fmt.Sprintf("boom %d", attempt-1) {
			t.Fatalf("attempt %d: LastError %q", attempt, j.LastError)
		}
		res, err := q.Fail(bg, j, fmt.Errorf("boom %d", attempt))
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 3 {
			if res.State != StateDead {
				t.Fatalf("after the last attempt: %+v", res)
			}
			break
		}
		wait := res.RunAt.Sub(c.now())
		if res.State != StateReady || wait < want[attempt-1] || wait > want[attempt-1]+want[attempt-1]/10 {
			t.Fatalf("attempt %d: state %s, wait %s, want %s plus up to a tenth", attempt, res.State, wait, want[attempt-1])
		}
		if j := claim(t, q, "q", time.Minute); j != nil {
			t.Fatal("a failed job ran before its backoff ended")
		}
		c.add(wait)
	}
	info := must[*JobInfo](t)(q.Get(bg, id))
	if info.State != StateDead || info.Attempts != 3 || info.LastError != "boom 3" {
		t.Fatalf("got %+v", info)
	}
}

func TestPermanentAndRetryAfter(t *testing.T) {
	q, c := newClockQueue(t)
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("x")))
	j := claim(t, q, "q", time.Minute)
	res, err := q.Fail(bg, j, RetryAfter(errors.New("busy"), 3*time.Second))
	if err != nil || res.State != StateReady || res.RunAt.Sub(c.now()) != 3*time.Second {
		t.Fatalf("RetryAfter: %+v %v", res, err)
	}
	c.add(3 * time.Second)
	j = claim(t, q, "q", time.Minute)
	res, err = q.Fail(bg, j, Permanent(errors.New("bad payload")))
	if err != nil || res.State != StateDead {
		t.Fatalf("Permanent: %+v %v", res, err)
	}
	if info := must[*JobInfo](t)(q.Get(bg, id)); info.Attempts != 2 || info.LastError != "bad payload" {
		t.Fatalf("got %+v", info)
	}
}

func TestExpiredLeaseGoesBack(t *testing.T) {
	q, c := newClockQueue(t, WithBackoff(time.Second, time.Second))
	id := must[int64](t)(q.Enqueue(bg, "q", []byte("x"), MaxAttempts(2)))
	first := claim(t, q, "q", 30*time.Second)
	c.add(29 * time.Second)
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("a job was taken while its lease was valid")
	}
	if err := q.Extend(bg, first, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	c.add(29 * time.Second)
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("a job was taken while its extended lease was valid")
	}
	if st := must[[]QueueStats](t)(q.Stats(bg)); st[0].Running != 1 || st[0].Expired != 0 {
		t.Fatalf("stats %+v", st)
	}
	c.add(2 * time.Second)
	if st := must[[]QueueStats](t)(q.Stats(bg)); st[0].Running != 0 || st[0].Expired != 1 {
		t.Fatalf("stats %+v", st)
	}
	// The next claim puts it back with a backoff, as a failed attempt.
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("an expired job ran again before its backoff")
	}
	c.add(1100 * time.Millisecond)
	second := claim(t, q, "q", 30*time.Second)
	if second == nil || second.ID != id || second.Attempt != 2 || second.LastError != leaseExpiredText {
		t.Fatalf("got %+v", second)
	}
	// The first worker no longer holds it.
	if err := q.Ack(bg, first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack: %v", err)
	}
	if _, err := q.Fail(bg, first, errors.New("late")); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale fail: %v", err)
	}
	if err := q.Extend(bg, first, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale extend: %v", err)
	}
	// It has used both attempts, so when this lease runs out too it is dead.
	c.add(31 * time.Second)
	if j := claim(t, q, "q", time.Minute); j != nil {
		t.Fatal("job ran a third time")
	}
	if info := must[*JobInfo](t)(q.Get(bg, id)); info.State != StateDead || info.LastError != leaseExpiredText {
		t.Fatalf("got %+v", info)
	}
}

func TestReleaseDoesNotUseAnAttempt(t *testing.T) {
	q, _ := newClockQueue(t)
	must[int64](t)(q.Enqueue(bg, "q", []byte("x")))
	j := claim(t, q, "q", time.Minute)
	if err := q.Release(bg, j); err != nil {
		t.Fatal(err)
	}
	if j2 := claim(t, q, "q", time.Minute); j2 == nil || j2.Attempt != 1 {
		t.Fatalf("got %+v", j2)
	}
}

func TestRetryPurgeDeleteKeepDone(t *testing.T) {
	q, _ := newClockQueue(t, WithKeepDone(true))
	a := must[int64](t)(q.Enqueue(bg, "q", []byte("a"), MaxAttempts(1)))
	b := must[int64](t)(q.Enqueue(bg, "q", []byte("b"), MaxAttempts(1)))
	d := must[int64](t)(q.Enqueue(bg, "r", []byte("d"), MaxAttempts(1)))
	for range 2 {
		q.Fail(bg, claim(t, q, "q", time.Minute), errors.New("x"))
	}
	q.Fail(bg, claim(t, q, "r", time.Minute), errors.New("x"))
	if err := q.Retry(bg, a); err != nil {
		t.Fatal(err)
	}
	if err := q.Retry(bg, a); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry of a job that isn't dead: %v", err)
	}
	j := claim(t, q, "q", time.Minute)
	if j == nil || j.ID != a || j.Attempt != 1 {
		t.Fatalf("got %+v", j)
	}
	if err := q.Ack(bg, j); err != nil {
		t.Fatal(err)
	}
	if info := must[*JobInfo](t)(q.Get(bg, a)); info.State != StateDone {
		t.Fatalf("kept job: %+v", info)
	}
	if n := must[int64](t)(q.RetryAll(bg, "r")); n != 1 {
		t.Fatalf("RetryAll moved %d", n)
	}
	if n := must[int64](t)(q.Purge(bg, StateDead, "")); n != 1 {
		t.Fatalf("purged %d dead", n)
	}
	if _, err := q.Get(bg, b); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged job still there: %v", err)
	}
	if n := must[int64](t)(q.Purge(bg, StateDone, "q")); n != 1 {
		t.Fatalf("purged %d done", n)
	}
	if _, err := q.Purge(bg, StateReady, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("purge of ready jobs: %v", err)
	}
	if err := q.Delete(bg, d); err != nil {
		t.Fatal(err)
	}
	if err := q.Delete(bg, d); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func TestListAndStats(t *testing.T) {
	q, c := newClockQueue(t, WithKeepDone(true))
	add := func(queue, payload string, opts ...EnqueueOption) {
		must[int64](t)(q.Enqueue(bg, queue, []byte(payload), opts...))
		c.add(time.Millisecond)
	}
	add("a", "running")
	add("a", "ready")
	add("a", "delayed", Delay(time.Hour))
	add("b", "dead", MaxAttempts(1))
	add("b", "done")
	if j := claim(t, q, "a", time.Minute); string(j.Payload) != "running" {
		t.Fatalf("claimed %q", j.Payload)
	}
	q.Fail(bg, claim(t, q, "b", time.Minute), errors.New("x"))
	q.Ack(bg, claim(t, q, "b", time.Minute))

	count := map[State]int{}
	for _, j := range must[[]JobInfo](t)(q.List(bg, ListFilter{})) {
		if string(j.Payload) != string(j.State) {
			t.Errorf("job %q is %s", j.Payload, j.State)
		}
		count[j.State]++
	}
	if len(count) != 5 {
		t.Fatalf("states %v", count)
	}
	for _, st := range []State{StateReady, StateDelayed, StateRunning, StateDead, StateDone} {
		got := must[[]JobInfo](t)(q.List(bg, ListFilter{State: st}))
		if len(got) != 1 || got[0].State != st {
			t.Fatalf("filter %s: %+v", st, got)
		}
	}
	if got := must[[]JobInfo](t)(q.List(bg, ListFilter{Queue: "b"})); len(got) != 2 {
		t.Fatalf("queue filter: %d jobs", len(got))
	}
	page := must[[]JobInfo](t)(q.List(bg, ListFilter{Limit: 2}))
	next := must[[]JobInfo](t)(q.List(bg, ListFilter{Limit: 2, AfterID: page[1].ID}))
	if len(page) != 2 || len(next) != 2 || next[0].ID != page[1].ID+1 {
		t.Fatalf("paging: %d then %d", len(page), len(next))
	}
	st := must[[]QueueStats](t)(q.Stats(bg))
	if len(st) != 2 {
		t.Fatalf("stats %+v", st)
	}
	a, b := st[0], st[1]
	if a.Queue != "a" || a.Ready != 1 || a.Delayed != 1 || a.Running != 1 || a.OldestDue.IsZero() ||
		b.Queue != "b" || b.Dead != 1 || b.Done != 1 || !b.OldestDue.IsZero() {
		t.Fatalf("stats %+v", st)
	}
}

func TestBadArguments(t *testing.T) {
	q, c := newClockQueue(t)
	long := strings.Repeat("é", MaxQueueNameLength+1)
	for name, err := range map[string]error{
		"empty queue":     second(q.Enqueue(bg, "", nil)),
		"long queue":      second(q.Enqueue(bg, long, nil)),
		"bad utf-8":       second(q.Enqueue(bg, "\xff", nil)),
		"negative delay":  second(q.Enqueue(bg, "q", nil, Delay(-time.Second))),
		"delay and at":    second(q.Enqueue(bg, "q", nil, Delay(time.Second), At(c.now()))),
		"zero attempts":   second(q.Enqueue(bg, "q", nil, MaxAttempts(-1))),
		"claim no queue":  second(q.Claim(bg, "", time.Minute)),
		"unknown state":   second(ParseState("waiting")),
		"list bad state":  second(q.List(bg, ListFilter{State: "x"})),
		"nil handler":     q.Work(bg, "q", nil),
		"nul in queue":    second(q.Enqueue(bg, "a\x00b", nil)),
		"priority high":   second(q.Enqueue(bg, "q", nil, Priority(MaxPriority+1))),
		"priority low":    second(q.Enqueue(bg, "q", nil, Priority(MinPriority-1))),
		"tiny lease":      second(q.Claim(bg, "q", 500*time.Microsecond)),
		"zero concurency": q.Work(bg, "q", func(context.Context, *Job) error { return nil }, Concurrency(0)),
	} {
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", name, err)
		}
	}
	if _, err := q.Enqueue(bg, strings.Repeat("é", MaxQueueNameLength), nil); err != nil {
		t.Errorf("a %d-character name was refused: %v", MaxQueueNameLength, err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "x"), WithMaxAttempts(0)); !errors.Is(err, ErrInvalid) {
		t.Errorf("WithMaxAttempts(0): %v", err)
	}
}

func second[T any](_ T, err error) error { return err }

func TestErrorTextIsCut(t *testing.T) {
	q, _ := newClockQueue(t)
	must[int64](t)(q.Enqueue(bg, "q", nil))
	j := claim(t, q, "q", time.Minute)
	q.Fail(bg, j, errors.New(strings.Repeat("ü", 3000)))
	info := must[*JobInfo](t)(q.Get(bg, j.ID))
	if len(info.LastError) > maxErrorText+10 || !strings.HasSuffix(info.LastError, " [cut]") || !strings.HasPrefix(info.LastError, "üü") {
		t.Fatalf("last error is %d bytes", len(info.LastError))
	}
}

func TestClosedQueue(t *testing.T) {
	q, _ := openTemp(t)
	q.Close()
	if _, err := q.Enqueue(bg, "q", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue after close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestNoDriverMessage(t *testing.T) {
	if driverName() == "" {
		t.Fatal("the test build registers go-sqlite3")
	}
	if !strings.Contains(errNoDriver.Error(), "github.com/mattn/go-sqlite3") {
		t.Fatal(errNoDriver)
	}
}

func TestPriorityRangeIsInTheFile(t *testing.T) {
	q, _ := openTemp(t)
	for _, p := range []int{MinPriority, MaxPriority} {
		if _, err := q.Enqueue(bg, "q", nil, Priority(p)); err != nil {
			t.Fatalf("priority %d: %v", p, err)
		}
	}
	// Other writers meet the same limit, through the CHECK constraint.
	if _, err := q.DB().Exec(`INSERT INTO ukue_jobs (queue, payload, priority) VALUES ('q', x'', 1000)`); err == nil {
		t.Fatal("the file took priority 1000")
	}
}

func TestExistingOnlyChangesNothing(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(filepath.Join(dir, "missing.ukue"), WithExistingOnly()); err == nil {
		t.Fatal("opened a missing file")
	}
	if _, err := os.Stat(filepath.Join(dir, "missing.ukue")); !os.IsNotExist(err) {
		t.Fatal("a missing file was created")
	}
	app := filepath.Join(dir, "app.db")
	db, err := sql.Open("sqlite3", app)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
	if _, err := Open(app, WithExistingOnly()); err == nil || !strings.Contains(err.Error(), "not a ukue file") {
		t.Fatalf("opened an app database: %v", err)
	}
	var mode string
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "delete" {
		t.Fatalf("journal mode changed to %s", mode)
	}
	// A real ukue file opens fine.
	q, path := openTemp(t)
	q.Close()
	q2, err := Open(path, WithExistingOnly())
	if err != nil {
		t.Fatal(err)
	}
	q2.Close()
}

func TestJournalModeIsSetOnlyWhenTheTablesAreCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := OpenDB(db); err != nil {
		t.Fatal(err)
	}
	var mode string
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "wal" {
		t.Fatalf("first open: journal mode %s", mode)
	}
	// The app chooses another mode; opening again leaves it.
	db.QueryRow("PRAGMA journal_mode = DELETE").Scan(&mode)
	if _, err := OpenDB(db); err != nil {
		t.Fatal(err)
	}
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if mode != "delete" {
		t.Fatalf("second open changed the journal mode to %s", mode)
	}
}

func TestInMemoryQueueWorksFromManyGoroutines(t *testing.T) {
	q, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := q.Enqueue(bg, "q", nil); err != nil {
					t.Error(err)
					return
				}
				if _, err := q.Claim(bg, "q", time.Minute); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestBackoffDoesNotOverflow(t *testing.T) {
	q, _ := openTemp(t, WithBackoff(time.Second, math.MaxInt64))
	for _, attempt := range []int{1, 30, 62, 63, 64, 1000} {
		if d := q.backoff(attempt); d <= 0 {
			t.Fatalf("attempt %d: backoff %d", attempt, d)
		}
	}
}
