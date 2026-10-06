// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests check the file format from another SQLite: Python's sqlite3
// module, through examples/python/add_job.py.

func python(t *testing.T) string {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found")
	}
	return py
}

func addJobPy(t *testing.T, py, file, queue, payload string, extra ...string) int64 {
	t.Helper()
	args := append([]string{"-I", filepath.Join("examples", "python", "add_job.py"), file, queue}, extra...)
	cmd := exec.Command(py, args...)
	cmd.Stdin = strings.NewReader(payload)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("add_job.py: %v: %s", err, out)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatalf("add_job.py printed %q", out)
	}
	return id
}

func TestPythonAddsJobs(t *testing.T) {
	py := python(t)
	q, path := openTemp(t)
	id := addJobPy(t, py, path, "email", `{"to": "dana@example.com"}`)
	later := addJobPy(t, py, path, "email", "later", "--delay", "3600")
	bin := addJobPy(t, py, path, "bin", "\x00\x01\xffbinary")
	j := claim(t, q, "email", time.Minute)
	if j == nil || j.ID != id || string(j.Payload) != `{"to": "dana@example.com"}` || j.MaxAttempts != 10 {
		t.Fatalf("claimed %+v", j)
	}
	if j := claim(t, q, "email", time.Minute); j != nil {
		t.Fatalf("the delayed job %d ran early", later)
	}
	info := must[*JobInfo](t)(q.Get(bg, later))
	if d := time.Until(info.RunAt); info.State != StateDelayed || d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("delayed job: %s, due in %s", info.State, d)
	}
	if j := claim(t, q, "bin", time.Minute); j == nil || j.ID != bin || string(j.Payload) != "\x00\x01\xffbinary" {
		t.Fatalf("binary payload: %+v", j)
	}
}

func TestPythonAndGoShareAFileAtOnce(t *testing.T) {
	py := python(t)
	q, path := openTemp(t, WithKeepDone(true))
	const n = 200
	var mu sync.Mutex
	seen := map[string]int{}
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go q.Work(ctx, "mixed", func(ctx context.Context, j *Job) error {
		mu.Lock()
		seen[string(j.Payload)]++
		mu.Unlock()
		return nil
	}, Concurrency(4), PollInterval(10*time.Millisecond))

	// Python adds jobs in one process while Go claims and finishes them.
	script := `
import sys
sys.path.insert(0, sys.argv[1])
from add_job import add_job
for i in range(int(sys.argv[3])):
    add_job(sys.argv[2], "mixed", "job %d" % i)
`
	cmd := exec.Command(py, "-I", "-B", "-c", script, filepath.Join("examples", "python"), path, strconv.Itoa(n))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	waitFor(t, 30*time.Second, "all jobs done", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == n
	})
	for p, c := range seen {
		if c != 1 {
			t.Fatalf("%q ran %d times", p, c)
		}
	}
	if s := integrity(t, q); s != "ok" {
		t.Fatalf("integrity_check: %s", s)
	}
}
