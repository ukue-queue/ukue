// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type result struct {
	code           int
	stdout, stderr string
}

func ukueCmd(t *testing.T, ctx context.Context, stdin string, args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(ctx, args, strings.NewReader(stdin), &out, &errOut)
	return result{code, out.String(), errOut.String()}
}

func ok(t *testing.T, args ...string) string {
	t.Helper()
	r := ukueCmd(t, context.Background(), "", args...)
	if r.code != 0 {
		t.Fatalf("ukue %s: exit %d: %s", strings.Join(args, " "), r.code, r.stderr)
	}
	return r.stdout
}

func TestCommands(t *testing.T) {
	file := filepath.Join(t.TempDir(), "jobs.ukue")
	if r := ukueCmd(t, context.Background(), "", "stats", file); r.code != 1 || !strings.Contains(r.stderr, "ukue init") {
		t.Fatalf("stats on a missing file: %+v", r)
	}
	ok(t, "init", file)
	if id := strings.TrimSpace(ok(t, "add", file, "email", `{"to":"dana@example.com"}`)); id != "1" {
		t.Fatalf("first id %q", id)
	}
	if r := ukueCmd(t, context.Background(), "from stdin", "add", file, "email"); r.code != 0 || strings.TrimSpace(r.stdout) != "2" {
		t.Fatalf("add from stdin: %+v", r)
	}
	ok(t, "add", file, "email", "--delay", "1h", "later")
	ok(t, "add", "--priority", "5", "--attempts", "2", file, "reports", "monthly")

	stats := ok(t, "stats", file)
	if !strings.Contains(stats, "email") || !strings.Contains(stats, "reports") {
		t.Fatalf("stats:\n%s", stats)
	}
	var st struct {
		Queues []struct {
			Queue          string
			Ready, Delayed int
		}
	}
	if err := json.Unmarshal([]byte(ok(t, "stats", "--json", file)), &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Queues) != 2 || st.Queues[0].Queue != "email" || st.Queues[0].Ready != 2 || st.Queues[0].Delayed != 1 {
		t.Fatalf("stats --json: %+v", st)
	}
	var list struct{ Jobs []map[string]any }
	json.Unmarshal([]byte(ok(t, "list", file, "--queue", "email", "--json")), &list)
	if len(list.Jobs) != 3 || list.Jobs[1]["payload"] != "from stdin" {
		t.Fatalf("list --json: %+v", list)
	}
	if out := ok(t, "list", file, "--state", "delayed"); !strings.Contains(out, "later") || strings.Contains(out, "monthly") {
		t.Fatalf("list --state delayed:\n%s", out)
	}
	if out := ok(t, "show", file, "4"); !strings.Contains(out, "monthly") || !strings.Contains(out, "priority:    5") {
		t.Fatalf("show:\n%s", out)
	}
	if r := ukueCmd(t, context.Background(), "", "show", file, "99"); r.code != 1 {
		t.Fatalf("show of a missing job: %+v", r)
	}
	ok(t, "delete", file, "3")
	if r := ukueCmd(t, context.Background(), "", "purge", file); r.code != 2 {
		t.Fatalf("purge without --state: %+v", r)
	}
	if r := ukueCmd(t, context.Background(), "", "frobnicate"); r.code != 2 {
		t.Fatalf("unknown command: %+v", r)
	}
	if out := ok(t, "version"); !strings.HasPrefix(out, "ukue 0.1.0") {
		t.Fatalf("version %q", out)
	}
}

func TestWorkRunsACommandPerJob(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "jobs.ukue")
	outFile := filepath.Join(dir, "out.txt")
	script := filepath.Join(dir, "handle.sh")
	os.WriteFile(script, []byte(`#!/bin/sh
payload=$(cat)
case "$payload" in
  bad) echo "cannot parse" >&2; exit 65 ;;
  flaky) [ "$UKUE_ATTEMPT" -lt 2 ] && { echo "try again" >&2; exit 1; } ;;
  slow) sleep 5 ;;
esac
echo "$UKUE_QUEUE $UKUE_JOB_ID $UKUE_ATTEMPT/$UKUE_MAX_ATTEMPTS $payload" >> "`+outFile+`"
`), 0o755)
	ok(t, "add", file, "q", "good")
	ok(t, "add", file, "q", "bad")
	ok(t, "add", file, "q", "flaky")
	ok(t, "add", "--attempts", "1", file, "q", "slow")

	// The default backoff is 10 seconds, so the flaky job's second try comes
	// after that. The worker stops when the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Second)
	defer cancel()
	r := ukueCmd(t, ctx, "", "work", "--timeout", "1s", "--concurrency", "2", "--poll", "50ms", file, "q", "--", script)
	if r.code != 0 {
		t.Fatalf("work: %+v", r)
	}
	got, _ := os.ReadFile(outFile)
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 2 || lines[0] != "q 1 1/10 good" || lines[1] != "q 3 2/10 flaky" {
		t.Fatalf("output:\n%s\nlog:\n%s", got, r.stderr)
	}
	var list struct{ Jobs []map[string]any }
	json.Unmarshal([]byte(ok(t, "list", file, "--state", "dead", "--json")), &list)
	if len(list.Jobs) != 2 {
		t.Fatalf("dead jobs: %+v", list.Jobs)
	}
	for _, j := range list.Jobs {
		le := j["last_error"].(string)
		switch j["payload"] {
		case "bad":
			if !strings.Contains(le, "status 65") || !strings.Contains(le, "cannot parse") {
				t.Errorf("bad job's error: %q", le)
			}
		case "slow":
			if !strings.Contains(le, "1s timeout") {
				t.Errorf("slow job's error: %q", le)
			}
		}
	}
	if out := ok(t, "retry", file, "--all"); !strings.HasPrefix(out, "2 dead") {
		t.Fatalf("retry --all: %q", out)
	}
	if out := ok(t, "purge", file, "--state", "dead"); !strings.HasPrefix(out, "0 dead") {
		t.Fatalf("purge: %q", out)
	}
}

func TestServeNeedsATokenOffThisMachine(t *testing.T) {
	file := filepath.Join(t.TempDir(), "jobs.ukue")
	ok(t, "init", file)
	r := ukueCmd(t, context.Background(), "", "serve", "--addr", "0.0.0.0:0", file)
	if r.code != 1 || !strings.Contains(r.stderr, "needs a token") {
		t.Fatalf("%+v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if r := ukueCmd(t, ctx, "", "serve", "--addr", "127.0.0.1:0", file); r.code != 0 || !strings.Contains(r.stderr, "ukue serving") {
		t.Fatalf("serve on localhost: %+v", r)
	}
}

func TestFlagErrorsExitTwoAndPrintOnce(t *testing.T) {
	r := ukueCmd(t, context.Background(), "", "add", "--nope", "f.ukue", "q", "x")
	if r.code != 2 || strings.Count(r.stderr, "flag provided but not defined") != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestDoubleDashKeepsThePayload(t *testing.T) {
	file := filepath.Join(t.TempDir(), "jobs.ukue")
	ok(t, "add", file, "q", "--", "-5")
	var list struct{ Jobs []map[string]any }
	json.Unmarshal([]byte(ok(t, "list", "--json", file)), &list)
	if len(list.Jobs) != 1 || list.Jobs[0]["payload"] != "-5" {
		t.Fatalf("%+v", list.Jobs)
	}
}

func TestOtherSQLiteFilesAreLeftAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"stats", path}, {"list", path}, {"work", path, "q", "--", "true"}, {"serve", path}} {
		r := ukueCmd(t, context.Background(), "", args...)
		if r.code != 1 || !strings.Contains(r.stderr, "not a ukue file") {
			t.Fatalf("ukue %s: %+v", args[0], r)
		}
	}
	var mode string
	var tables int
	db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE 'ukue%'").Scan(&tables)
	if mode != "delete" || tables != 0 {
		t.Fatalf("the file was changed: journal mode %s, %d ukue tables", mode, tables)
	}
}

func TestWorkChecksItsCommandAndFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "jobs.ukue")
	if r := ukueCmd(t, context.Background(), "", "work", file, "q", "--", "true"); r.code != 1 || !strings.Contains(r.stderr, "ukue init") {
		t.Fatalf("missing file: %+v", r)
	}
	ok(t, "init", file)
	if r := ukueCmd(t, context.Background(), "", "work", file, "q", "--", "./no-such-script.sh"); r.code != 2 || !strings.Contains(r.stderr, "can't run") {
		t.Fatalf("missing command: %+v", r)
	}
}

func TestWorkExitZeroIsDoneEvenWithABackgroundChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	killDelay = time.Second
	defer func() { killDelay = 10 * time.Second }()
	dir := t.TempDir()
	file := filepath.Join(dir, "jobs.ukue")
	ok(t, "add", file, "q", "x")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	// The background sleep keeps the command's output open after it exits.
	r := ukueCmd(t, ctx, "", "work", "--keep-done", "--poll", "50ms", file, "q", "--", "sh", "-c", "sleep 3 & echo started")
	var list struct{ Jobs []map[string]any }
	json.Unmarshal([]byte(ok(t, "list", "--json", file)), &list)
	if r.code != 0 || len(list.Jobs) != 1 || list.Jobs[0]["state"] != "done" {
		t.Fatalf("jobs %+v\nlog:\n%s", list.Jobs, r.stderr)
	}
}

func TestWorkStopsCommandsWithSIGTERMFirst(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh and signals")
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "jobs.ukue")
	marker := filepath.Join(dir, "got-term")
	ok(t, "add", file, "q", "x")
	script := "trap 'echo yes > " + marker + "; exit 0' TERM; sleep 30 & wait $!"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result)
	go func() {
		done <- ukueCmd(t, ctx, "", "work", "--grace", "200ms", "--poll", "50ms", file, "q", "--", "sh", "-c", script)
	}()
	time.Sleep(time.Second) // let the job start
	cancel()
	select {
	case r := <-done:
		if r.code != 0 {
			t.Fatalf("%+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("work didn't stop")
	}
	if b, err := os.ReadFile(marker); err != nil || strings.TrimSpace(string(b)) != "yes" {
		t.Fatalf("the command didn't get SIGTERM: %v", err)
	}
}

func TestServeEndsWaitingClaimsOnShutdown(t *testing.T) {
	file := filepath.Join(t.TempDir(), "jobs.ukue")
	ok(t, "init", file)
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan int)
	go func() {
		served <- run(ctx, []string{"serve", "--addr", "127.0.0.1:0", file}, strings.NewReader(""), io.Discard, pw)
		pw.Close()
	}()
	var url string
	sc := bufio.NewScanner(pr)
	for sc.Scan() {
		if i := strings.Index(sc.Text(), "url=http://"); i >= 0 {
			url = strings.Fields(sc.Text()[i+4:])[0]
			break
		}
	}
	go io.Copy(io.Discard, pr)
	if url == "" {
		t.Fatal("serve didn't print its address")
	}
	// A request addressed to another host name is refused without a token.
	req, _ := http.NewRequest("GET", url+"/v1/stats", nil)
	req.Host = "rebind.example"
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Host: %v %v", resp, err)
	}
	claimed := make(chan int)
	go func() {
		resp, err := http.Post(url+"/v1/claim", "application/json", strings.NewReader(`{"queue": "q", "wait": 25}`))
		if err != nil {
			claimed <- -1
			return
		}
		resp.Body.Close()
		claimed <- resp.StatusCode
	}()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	cancel()
	if code := <-claimed; code != http.StatusNoContent {
		t.Fatalf("the waiting claim got %d", code)
	}
	if code := <-served; code != 0 {
		t.Fatalf("serve exited %d", code)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("shutdown took %s", took)
	}
}
