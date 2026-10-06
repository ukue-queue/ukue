// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ukue-queue/ukue"
)

type harness struct {
	t     *testing.T
	q     *ukue.Queue
	path  string
	srv   *httptest.Server
	token string
}

func newHarness(t *testing.T, o Options, qopts ...ukue.Option) *harness {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jobs.ukue")
	q, err := ukue.Open(path, qopts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(q, o))
	t.Cleanup(func() { srv.Close(); q.Close() })
	return &harness{t: t, q: q, path: path, srv: srv, token: o.Token}
}

// do sends a request and decodes the JSON answer into a map.
func (h *harness) do(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rd)
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			h.t.Fatalf("%s %s: answer is not JSON: %s", method, path, raw)
		}
	}
	return resp.StatusCode, m
}

func (h *harness) expect(wantStatus int, method, path string, body any) map[string]any {
	h.t.Helper()
	status, m := h.do(method, path, body)
	if status != wantStatus {
		h.t.Fatalf("%s %s: status %d, want %d: %v", method, path, status, wantStatus, m)
	}
	return m
}

func TestAddClaimAck(t *testing.T) {
	h := newHarness(t, Options{})
	h.expect(200, "GET", "/v1/health", nil)
	m := h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "email", "payload": map[string]any{"to": "dana@example.com"}})
	id := int64(m["id"].(float64))

	job := h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "email", "lease": "30s"})
	if int64(job["id"].(float64)) != id || job["payload"] != `{"to":"dana@example.com"}` || job["attempt"].(float64) != 1 {
		t.Fatalf("claimed %v", job)
	}
	token := job["token"].(string)
	h.expect(204, "POST", "/v1/claim", map[string]any{"queue": "email"})
	h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/extend", id), map[string]any{"token": token, "lease": 60})
	h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/ack", id), map[string]any{"token": token})
	h.expect(409, "POST", fmt.Sprintf("/v1/jobs/%d/ack", id), map[string]any{"token": token})
	h.expect(404, "GET", fmt.Sprintf("/v1/jobs/%d", id), nil)
}

func TestFailRetryAndBulk(t *testing.T) {
	h := newHarness(t, Options{}, ukue.WithKeepDone(true))
	m := h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": "x", "max_attempts": 3, "priority": 2})
	id := int64(m["id"].(float64))
	job := h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "q"})
	res := h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/fail", id), map[string]any{"token": job["token"], "error": "smtp down", "retry_in": 0})
	if res["state"] != "ready" {
		t.Fatalf("fail answered %v", res)
	}
	job = h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "q"})
	if job["attempt"].(float64) != 2 || job["last_error"] != "smtp down" || job["priority"].(float64) != 2 {
		t.Fatalf("second claim %v", job)
	}
	res = h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/fail", id), map[string]any{"token": job["token"], "error": "bad address", "dead": true})
	if res["state"] != "dead" {
		t.Fatalf("fail answered %v", res)
	}
	list := h.expect(200, "GET", "/v1/jobs?state=dead&queue=q", nil)
	if jobs := list["jobs"].([]any); len(jobs) != 1 || jobs[0].(map[string]any)["last_error"] != "bad address" {
		t.Fatalf("dead list %v", list)
	}
	h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/retry", id), nil)
	h.expect(404, "POST", fmt.Sprintf("/v1/jobs/%d/retry", id), nil)
	job = h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "q"})
	h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/release", id), map[string]any{"token": job["token"]})
	job = h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "q"})
	if job["attempt"].(float64) != 1 {
		t.Fatalf("after retry and release: %v", job)
	}
	h.expect(200, "POST", fmt.Sprintf("/v1/jobs/%d/ack", id), map[string]any{"token": job["token"]})

	st := h.expect(200, "GET", "/v1/stats", nil)
	row := st["queues"].([]any)[0].(map[string]any)
	if row["queue"] != "q" || row["done"].(float64) != 1 {
		t.Fatalf("stats %v", st)
	}
	if m := h.expect(200, "POST", "/v1/purge", map[string]any{"state": "done"}); m["deleted"].(float64) != 1 {
		t.Fatalf("purge %v", m)
	}
	h.expect(400, "POST", "/v1/purge", map[string]any{"state": "ready"})
	if m := h.expect(200, "POST", "/v1/retry", map[string]any{"queue": "q"}); m["retried"].(float64) != 0 {
		t.Fatalf("retry all %v", m)
	}
	m = h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": "gone"})
	h.expect(200, "DELETE", fmt.Sprintf("/v1/jobs/%d", int64(m["id"].(float64))), nil)
}

func TestDelayAndRunAt(t *testing.T) {
	h := newHarness(t, Options{})
	h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": "a", "delay": "1h"})
	at := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": "b", "run_at": at})
	h.expect(204, "POST", "/v1/claim", map[string]any{"queue": "q"})
	list := h.expect(200, "GET", "/v1/jobs?state=delayed", nil)
	if n := len(list["jobs"].([]any)); n != 2 {
		t.Fatalf("%d delayed jobs", n)
	}
}

func TestBinaryPayload(t *testing.T) {
	h := newHarness(t, Options{})
	h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload_base64": "AP8Q"})
	job := h.expect(200, "POST", "/v1/claim", map[string]any{"queue": "q"})
	if job["payload_base64"] != "AP8Q" || job["payload"] != nil {
		t.Fatalf("claimed %v", job)
	}
}

func TestClaimWaitsForNewJobs(t *testing.T) {
	h := newHarness(t, Options{})
	type result struct {
		status int
		job    map[string]any
		took   time.Duration
	}
	got := make(chan result, 1)
	go func() {
		start := time.Now()
		s, m := h.do("POST", "/v1/claim", map[string]any{"queue": "q", "wait": 10})
		got <- result{s, m, time.Since(start)}
	}()
	time.Sleep(200 * time.Millisecond)
	h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": "through the server"})
	r := <-got
	if r.status != 200 || r.job["payload"] != "through the server" || r.took > 2*time.Second {
		t.Fatalf("status %d after %s: %v", r.status, r.took, r.job)
	}

	// A job written straight into the file by another program is found by
	// polling, within a fraction of a second.
	go func() {
		start := time.Now()
		s, m := h.do("POST", "/v1/claim", map[string]any{"queue": "q", "wait": 10})
		got <- result{s, m, time.Since(start)}
	}()
	time.Sleep(200 * time.Millisecond)
	other, err := ukue.Open(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Enqueue(context.Background(), "q", []byte("from another program")); err != nil {
		t.Fatal(err)
	}
	other.Close()
	r = <-got
	if r.status != 200 || r.job["payload"] != "from another program" || r.took > 2*time.Second {
		t.Fatalf("status %d after %s: %v", r.status, r.took, r.job)
	}

	start := time.Now()
	h.expect(204, "POST", "/v1/claim", map[string]any{"queue": "q", "wait": 0.3})
	if took := time.Since(start); took < 250*time.Millisecond || took > 2*time.Second {
		t.Fatalf("an empty claim with a 0.3 s wait took %s", took)
	}
}

func TestToken(t *testing.T) {
	h := newHarness(t, Options{Token: "s3cret"})
	h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "q"})
	h.token = "wrong"
	h.expect(401, "POST", "/v1/jobs", map[string]any{"queue": "q"})
	h.expect(401, "GET", "/v1/stats", nil)
	h.token = ""
	h.expect(401, "POST", "/v1/claim", map[string]any{"queue": "q"})
	h.expect(200, "GET", "/v1/health", nil)
}

func TestBadRequests(t *testing.T) {
	h := newHarness(t, Options{MaxBodyBytes: 1000})
	big := strings.Repeat("x", 2000)
	h.expect(413, "POST", "/v1/jobs", map[string]any{"queue": "q", "payload": big})
	h.expect(400, "POST", "/v1/jobs", map[string]any{"queue": ""})
	h.expect(400, "POST", "/v1/jobs", map[string]any{"queue": "q", "colour": "blue"})
	h.expect(400, "POST", "/v1/jobs", `{"queue": "q", "payload": "x", "payload_base64": "eA=="}`)
	h.expect(400, "POST", "/v1/jobs", map[string]any{"queue": "q", "delay": "soon"})
	h.expect(400, "POST", "/v1/jobs", map[string]any{"queue": "q", "delay": -5})
	h.expect(400, "POST", "/v1/jobs", map[string]any{"queue": "q", "run_at": "tomorrow"})
	h.expect(400, "POST", "/v1/jobs", "{not json")
	h.expect(400, "POST", "/v1/claim", map[string]any{"queue": "q", "lease": 0})
	h.expect(400, "POST", "/v1/jobs/abc/ack", map[string]any{"token": "t"})
	h.expect(400, "POST", "/v1/jobs/1/ack", map[string]any{})
	h.expect(409, "POST", "/v1/jobs/1/fail", map[string]any{"token": "t"})
	h.expect(400, "GET", "/v1/jobs?state=waiting", nil)
	h.expect(404, "GET", "/v2/jobs", nil)
	h.expect(405, "PUT", "/v1/jobs", nil)
}

// TestPythonHTTPWorker runs the Python example worker against the server.
func TestPythonHTTPWorker(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found")
	}
	h := newHarness(t, Options{Token: "t0ken"}, ukue.WithKeepDone(true))
	m := h.expect(201, "POST", "/v1/jobs", map[string]any{"queue": "email", "payload": "hello from Go"})
	id := int64(m["id"].(float64))
	cmd := exec.Command(py, "-I", filepath.Join("..", "examples", "python", "http_worker.py"), "email", "--once")
	cmd.Env = append(os.Environ(), "UKUE_URL="+h.srv.URL, "UKUE_TOKEN=t0ken")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !strings.Contains(string(out), "hello from Go") {
		t.Fatalf("worker printed %q", out)
	}
	info, err := h.q.Get(context.Background(), id)
	if err != nil || info.State != ukue.StateDone {
		t.Fatalf("job after the Python worker: %+v %v", info, err)
	}
}
