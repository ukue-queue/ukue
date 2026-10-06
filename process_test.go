// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package ukue

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These tests start copies of the test binary as separate worker processes
// on one file, and kill some of them with SIGKILL in the middle of their
// work. TestMain sends the copies to their role instead of the tests.

func TestMain(m *testing.M) {
	switch os.Getenv("UKUE_TEST_ROLE") {
	case "worker":
		os.Exit(childWorker())
	case "holder":
		os.Exit(childHolder())
	}
	os.Exit(m.Run())
}

func envDuration(name string) time.Duration {
	d, err := time.ParseDuration(os.Getenv(name))
	if err != nil {
		panic(name + ": " + err.Error())
	}
	return d
}

// childWorker works the "chaos" queue until SIGTERM, recording every run of
// every job in the runs table.
func childWorker() int {
	q, err := Open(os.Getenv("UKUE_TEST_FILE"), WithKeepDone(true), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer q.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	pid := os.Getpid()
	err = q.Work(ctx, "chaos", func(ctx context.Context, j *Job) error {
		if _, err := q.exec(ctx, `INSERT INTO runs (job_id, pid, attempt) VALUES (?, ?, ?)`, j.ID, pid, j.Attempt); err != nil {
			return err
		}
		time.Sleep(time.Duration(rand.IntN(4000)) * time.Microsecond)
		return nil
	}, Concurrency(4), Lease(envDuration("UKUE_TEST_LEASE")), PollInterval(20*time.Millisecond), ShutdownGrace(5*time.Second))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// childHolder claims one job, says which, and then hangs until it is killed.
func childHolder() int {
	q, err := Open(os.Getenv("UKUE_TEST_FILE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	j, err := q.Claim(context.Background(), "held", envDuration("UKUE_TEST_LEASE"))
	if err != nil || j == nil {
		fmt.Fprintln(os.Stderr, "no job:", err)
		return 1
	}
	fmt.Printf("claimed %d\n", j.ID)
	time.Sleep(time.Hour)
	return 0
}

func startChild(t testing.TB, role, file string, lease time.Duration) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "UKUE_TEST_ROLE="+role, "UKUE_TEST_FILE="+file, "UKUE_TEST_LEASE="+lease.String())
	cmd.Stderr = os.Stderr
	return cmd
}

func setupChaos(t testing.TB, n int) (*Queue, string) {
	t.Helper()
	q, path := openTemp(t, WithKeepDone(true), WithBackoff(20*time.Millisecond, 100*time.Millisecond))
	if _, err := q.DB().Exec(`CREATE TABLE runs (job_id INTEGER NOT NULL, pid INTEGER NOT NULL, attempt INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		must[int64](t)(q.Enqueue(bg, "chaos", []byte(strconv.Itoa(i)), MaxAttempts(1000)))
	}
	return q, path
}

func doneCount(t testing.TB, q *Queue) int {
	var n int
	if err := q.DB().QueryRow(`SELECT count(*) FROM ukue_jobs WHERE state = 'done'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestProcessesShareAFile runs four worker processes on one file and checks
// that every job ran exactly once.
func TestProcessesShareAFile(t *testing.T) {
	const n = 2000
	q, path := setupChaos(t, n)
	var workers []*exec.Cmd
	start := time.Now()
	for i := 0; i < 4; i++ {
		c := startChild(t, "worker", path, 5*time.Second)
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		workers = append(workers, c)
	}
	waitFor(t, 2*time.Minute, "all jobs done", func() bool { return doneCount(t, q) == n })
	took := time.Since(start)
	for _, c := range workers {
		c.Process.Signal(syscall.SIGTERM)
		if err := c.Wait(); err != nil {
			t.Errorf("worker %d: %v", c.Process.Pid, err)
		}
	}
	var runs, distinct, pids int
	q.DB().QueryRow(`SELECT count(*), count(DISTINCT job_id), count(DISTINCT pid) FROM runs`).Scan(&runs, &distinct, &pids)
	if runs != n || distinct != n {
		t.Fatalf("%d runs of %d distinct jobs, want %d of %d", runs, distinct, n, n)
	}
	if pids != 4 {
		t.Errorf("only %d of 4 processes got work", pids)
	}
	if s := integrity(t, q); s != "ok" {
		t.Fatalf("integrity_check: %s", s)
	}
	t.Logf("%d jobs, 4 processes with 4 workers each: every job ran exactly once, in %s (%.0f jobs/s, each run writing a row of its own as well)",
		n, took.Round(time.Millisecond), float64(n)/took.Seconds())
}

// TestKilledWorkersJobRunsAgain kills a worker that holds a job, again and
// again, and checks the job comes back and finishes every time.
func TestKilledWorkersJobRunsAgain(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	q, path := openTemp(t, WithKeepDone(true), WithBackoff(50*time.Millisecond, 50*time.Millisecond))
	const rounds = 20
	const lease = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	for i := 0; i < rounds; i++ {
		id := must[int64](t)(q.Enqueue(bg, "held", []byte(strconv.Itoa(i))))
		holder := startChild(t, "holder", path, lease)
		out, err := holder.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}
		line, err := bufio.NewReader(out).ReadString('\n')
		if err != nil || strings.TrimSpace(line) != fmt.Sprintf("claimed %d", id) {
			t.Fatalf("holder said %q, %v", line, err)
		}
		killedAt := time.Now()
		holder.Process.Kill()
		holder.Wait()

		info := must[*JobInfo](t)(q.Get(bg, id))
		if info.State != StateRunning {
			t.Fatalf("round %d: right after the kill the job is %s", i, info.State)
		}
		// A worker in this process finishes the job once the lease runs out.
		var ranAt atomic.Int64
		wctx, stop := context.WithCancel(ctx)
		go q.Work(wctx, "held", func(ctx context.Context, j *Job) error {
			ranAt.Store(time.Now().UnixNano())
			return nil
		}, PollInterval(10*time.Millisecond))
		waitFor(t, 10*time.Second, "the job to finish", func() bool {
			return must[*JobInfo](t)(q.Get(bg, id)).State == StateDone
		})
		stop()
		info = must[*JobInfo](t)(q.Get(bg, id))
		if info.Attempts != 2 || info.LastError != leaseExpiredText {
			t.Fatalf("round %d: %d attempts, last error %q", i, info.Attempts, info.LastError)
		}
		if d := time.Unix(0, ranAt.Load()).Sub(killedAt); d < lease-50*time.Millisecond {
			t.Fatalf("round %d: the job ran again %s after the kill, before the %s lease ran out", i, d, lease)
		}
	}
	if s := integrity(t, q); s != "ok" {
		t.Fatalf("integrity_check: %s", s)
	}
	t.Logf("%d workers killed with SIGKILL while holding a job: all %d jobs came back after the %s lease and finished on the second attempt",
		rounds, rounds, lease)
}

// TestChaosKills keeps four worker processes busy on one file and kills one
// at random every few dozen milliseconds, mid-job, mid-claim or mid-commit.
// No job may be lost, none may be left stuck, and the file must stay intact.
func TestChaosKills(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const n, kills = 8000, 250
	const lease = 400 * time.Millisecond
	q, path := setupChaos(t, n)
	workers := make([]*exec.Cmd, 4)
	startWorker := func(i int) {
		c := startChild(t, "worker", path, lease)
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		workers[i] = c
	}
	for i := range workers {
		startWorker(i)
	}
	start := time.Now()
	killed, checks := 0, 0
	for killed < kills {
		time.Sleep(time.Duration(20+rand.IntN(60)) * time.Millisecond)
		i := rand.IntN(len(workers))
		workers[i].Process.Kill()
		workers[i].Wait()
		killed++
		startWorker(i)
		if killed%50 == 0 {
			if s := integrity(t, q); s != "ok" {
				t.Fatalf("integrity_check after %d kills: %s", killed, s)
			}
			checks++
		}
		if doneCount(t, q) == n {
			break
		}
	}
	waitFor(t, 3*time.Minute, "all jobs done", func() bool { return doneCount(t, q) == n })
	took := time.Since(start)
	for _, c := range workers {
		c.Process.Signal(syscall.SIGTERM)
		c.Wait()
	}

	var other, runs, distinct, again, maxAttempt int
	q.DB().QueryRow(`SELECT count(*) FROM ukue_jobs WHERE state != 'done'`).Scan(&other)
	q.DB().QueryRow(`SELECT count(*), count(DISTINCT job_id) FROM runs`).Scan(&runs, &distinct)
	q.DB().QueryRow(`SELECT count(*) FROM (SELECT job_id FROM runs GROUP BY job_id HAVING count(*) > 1)`).Scan(&again)
	q.DB().QueryRow(`SELECT max(attempts) FROM ukue_jobs`).Scan(&maxAttempt)
	if other != 0 || distinct != n {
		t.Fatalf("%d jobs not done, %d of %d jobs ever ran", other, distinct, n)
	}
	s := integrity(t, q)
	if s != "ok" {
		t.Fatalf("integrity_check: %s", s)
	}
	t.Logf("%d jobs, %d SIGKILLs at random moments in %s: 0 jobs lost, 0 stuck, integrity_check ok (%d checks during the run and one after); "+
		"%d runs in all: %d jobs ran more than once because their worker was killed between starting the work and marking it done; "+
		"the most attempts any job needed was %d",
		n, killed, took.Round(time.Millisecond), checks, runs, again, maxAttempt)
}
