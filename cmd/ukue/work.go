// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ukue-queue/ukue"
)

// Exit codes a command can use to steer what happens to its job.
const (
	exitPermanent = 65 // the payload is bad: send the job straight to dead letters
	exitTempFail  = 75 // try again after the usual delay (any other non-zero code does the same)
)

func cmdWork(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("work", "work [options] FILE QUEUE -- COMMAND [ARG...]", stderr)
	concurrency := fs.Int("concurrency", 1, "jobs to run at once")
	lease := fs.Duration("lease", ukue.DefaultLease, "how long a job is held if this worker stops answering; renewed while the command runs")
	timeout := fs.Duration("timeout", 0, "stop a command that runs longer than this and fail the attempt (0 means no limit)")
	poll := fs.Duration("poll", time.Second, "longest wait between looks when the queue is empty")
	grace := fs.Duration("grace", 30*time.Second, "after Ctrl-C or SIGTERM, how long running commands may finish before they are stopped")
	keepDone := fs.Bool("keep-done", false, "keep finished jobs in the file instead of deleting them")
	quiet := fs.Bool("quiet", false, "print only warnings and errors")
	pos, command, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errUsage{"give FILE and QUEUE, then -- and the command to run"}
	}
	if len(command) == 0 {
		return errUsage{"give the command to run after --, for example: ukue work jobs.ukue email -- ./send-email.sh"}
	}
	if *timeout < 0 {
		return errUsage{"--timeout can't be negative"}
	}
	file, queue := pos[0], pos[1]
	q, err := open(file, true, ukue.WithKeepDone(*keepDone))
	if err != nil {
		return err
	}
	defer q.Close()

	level := slog.LevelInfo
	if *quiet {
		level = slog.LevelWarn
	}
	// One lock for everything written out, so the commands' output and the
	// worker's own log lines never land in the middle of each other.
	var mu sync.Mutex
	out := &lockedWriter{w: stdout, mu: &mu}
	errOut := &lockedWriter{w: stderr, mu: &mu}
	log := newLogger(errOut, level)
	log.Info("ukue worker started", "file", file, "queue", queue, "command", strings.Join(command, " "), "concurrency", *concurrency)

	h := func(jctx context.Context, job *ukue.Job) error {
		if *timeout > 0 {
			var cancel context.CancelFunc
			jctx, cancel = context.WithTimeout(jctx, *timeout)
			defer cancel()
		}
		return runCommand(jctx, command, job, file, out, errOut, *timeout)
	}
	err = q.Work(ctx, queue, h,
		ukue.Concurrency(*concurrency),
		ukue.Lease(*lease),
		ukue.PollInterval(*poll),
		ukue.ShutdownGrace(*grace),
		ukue.Logger(log))
	log.Info("ukue worker stopped", "queue", queue)
	return err
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// tail keeps the last n bytes written to it.
type tail struct {
	mu  sync.Mutex
	n   int
	buf []byte
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.n {
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.n:]...)
	}
	return len(p), nil
}

func (t *tail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(bytes.ToValidUTF8(t.buf, []byte("?"))))
}

// runCommand runs the worker's command for one job: the payload goes to its
// standard input and the job's details to its environment.
func runCommand(ctx context.Context, command []string, job *ukue.Job, file string, stdout, stderr io.Writer, timeout time.Duration) error {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Stdin = bytes.NewReader(job.Payload)
	cmd.Stdout = stdout
	errTail := &tail{n: 2000}
	cmd.Stderr = io.MultiWriter(stderr, errTail)
	cmd.Env = append(os.Environ(),
		"UKUE_FILE="+file,
		"UKUE_QUEUE="+job.Queue,
		"UKUE_JOB_ID="+strconv.FormatInt(job.ID, 10),
		"UKUE_ATTEMPT="+strconv.Itoa(job.Attempt),
		"UKUE_MAX_ATTEMPTS="+strconv.Itoa(job.MaxAttempts),
	)
	setProcessGroup(cmd)
	cmd.Cancel = func() error { return killProcessGroup(cmd) }
	cmd.WaitDelay = 5 * time.Second

	err := cmd.Run()
	if err == nil {
		return nil
	}
	detail := errTail.String()
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && timeout > 0 {
			return withDetail(fmt.Errorf("stopped after the %s timeout", timeout), detail)
		}
		return withDetail(fmt.Errorf("stopped: %w", ctx.Err()), detail)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code := exitErr.ExitCode()
		e := withDetail(fmt.Errorf("command exited with status %d", code), detail)
		if code == exitPermanent {
			return ukue.Permanent(e)
		}
		return e
	}
	return withDetail(fmt.Errorf("could not run the command: %w", err), detail)
}

func withDetail(err error, detail string) error {
	if detail == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, detail)
}
