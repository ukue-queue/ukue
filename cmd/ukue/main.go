// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

// Command ukue adds, runs, inspects and serves jobs in a ukue file.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ukue-queue/ukue"
)

const usage = `ukue %s: a job queue in one file (https://ukue.com)

Usage:
  ukue init   FILE                         create an empty ukue file
  ukue add    FILE QUEUE [PAYLOAD|-]       add a job (payload from stdin if left out or -)
  ukue work   FILE QUEUE -- COMMAND [ARG]  run COMMAND for each job, payload on stdin
  ukue serve  FILE                         serve the HTTP API
  ukue stats  FILE                         count jobs per queue and state
  ukue list   FILE                         list jobs
  ukue show   FILE ID                      show one job and its payload
  ukue retry  FILE ID... | --all           move dead jobs back to ready
  ukue purge  FILE --state dead|done       delete dead or finished jobs
  ukue delete FILE ID...                   delete jobs
  ukue version                             print the version

Run "ukue COMMAND -h" for a command's options.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first Ctrl-C or SIGTERM asks ukue to stop cleanly; a second one
	// ends it at once.
	go func() {
		<-ctx.Done()
		stop()
	}()
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// errUsage means the command line was wrong; run prints the message and
// exits with status 2.
type errUsage struct{ msg string }

func (e errUsage) Error() string { return e.msg }

// errFlag is a flag error the flag package has already printed.
type errFlag struct{ err error }

func (e errFlag) Error() string { return e.err.Error() }

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, usage, ukue.Version)
		return 2
	}
	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "init":
		err = cmdInit(ctx, rest, stdout, stderr)
	case "add":
		err = cmdAdd(ctx, rest, stdin, stdout, stderr)
	case "work":
		err = cmdWork(ctx, rest, stdout, stderr)
	case "serve":
		err = cmdServe(ctx, rest, stdout, stderr)
	case "stats":
		err = cmdStats(ctx, rest, stdout, stderr)
	case "list":
		err = cmdList(ctx, rest, stdout, stderr)
	case "show":
		err = cmdShow(ctx, rest, stdout, stderr)
	case "retry":
		err = cmdRetry(ctx, rest, stdout, stderr)
	case "purge":
		err = cmdPurge(ctx, rest, stdout, stderr)
	case "delete":
		err = cmdDelete(ctx, rest, stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "ukue %s (file format %d)\n", ukue.Version, ukue.FormatVersion)
		return 0
	case "help", "-h", "--help":
		fmt.Fprintf(stdout, usage, ukue.Version)
		return 0
	default:
		fmt.Fprintf(stderr, "ukue: unknown command %q\n\n", cmd)
		fmt.Fprintf(stderr, usage, ukue.Version)
		return 2
	}
	if err == nil {
		return 0
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	var fe errFlag
	if errors.As(err, &fe) {
		return 2
	}
	var ue errUsage
	if errors.As(err, &ue) {
		fmt.Fprintf(stderr, "ukue %s: %s\n", cmd, ue.msg)
		return 2
	}
	fmt.Fprintf(stderr, "ukue %s: %s\n", cmd, strings.TrimPrefix(err.Error(), "ukue: "))
	return 1
}

// parseTail reads flags that may come before, between or after the
// positional arguments. Everything after a "--" is returned separately,
// untouched.
func parseTail(fs *flag.FlagSet, args []string) (pos, tail []string, err error) {
	for i, a := range args {
		if a == "--" {
			args, tail = args[:i], args[i+1:]
			break
		}
	}
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, nil, err
			}
			return nil, nil, errFlag{err}
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, tail, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// parse is parseTail for commands that take no command line of their own:
// anything after "--" is a positional argument too, such as a payload that
// starts with a dash.
func parse(fs *flag.FlagSet, args []string) (pos, tail []string, err error) {
	pos, tail, err = parseTail(fs, args)
	return append(pos, tail...), nil, err
}

func newFlags(name, synopsis string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: ukue %s\n", synopsis)
		var has bool
		fs.VisitAll(func(*flag.Flag) { has = true })
		if has {
			fmt.Fprintln(stderr, "\nOptions:")
			fs.PrintDefaults()
		}
	}
	return fs
}

// open opens a ukue file. Only init and add create one: every other command
// needs a file that already holds the ukue tables, and leaves anything else
// untouched, so a mistyped path is caught.
func open(path string, create bool, opts ...ukue.Option) (*ukue.Queue, error) {
	if !create {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%s doesn't exist (create it with \"ukue init %s\")", path, path)
			}
			return nil, err
		}
		opts = append(opts, ukue.WithExistingOnly())
	}
	q, err := ukue.Open(path, opts...)
	if err != nil && !create && strings.Contains(err.Error(), "not a ukue file") {
		return nil, fmt.Errorf("%s is not a ukue file", path)
	}
	return q, err
}

func cmdInit(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("init", "init FILE", stderr)
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errUsage{"give one FILE"}
	}
	q, err := open(pos[0], true)
	if err != nil {
		return err
	}
	defer q.Close()
	fmt.Fprintf(stdout, "%s is ready (ukue file format %d)\n", pos[0], ukue.FormatVersion)
	return nil
}

func cmdAdd(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := newFlags("add", "add [options] FILE QUEUE [PAYLOAD|-]", stderr)
	delay := fs.Duration("delay", 0, "wait this long before the job can run, such as 30s or 2h")
	at := fs.String("at", "", "run no earlier than this RFC 3339 time, such as 2026-10-06T09:00:00Z")
	attempts := fs.Int("attempts", 0, "tries before the job is dead (default 10)")
	priority := fs.Int("priority", 0, "higher runs first among due jobs")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 || len(pos) > 3 {
		return errUsage{"give FILE, QUEUE and an optional PAYLOAD"}
	}
	var payload []byte
	if len(pos) == 3 && pos[2] != "-" {
		payload = []byte(pos[2])
	} else {
		payload, err = readPayload(ctx, stdin, stderr)
		if err != nil {
			return err
		}
	}
	var opts []ukue.EnqueueOption
	if *delay != 0 {
		opts = append(opts, ukue.Delay(*delay))
	}
	if *at != "" {
		t, err := time.Parse(time.RFC3339, *at)
		if err != nil {
			return errUsage{"--at needs an RFC 3339 time such as 2026-10-06T09:00:00Z"}
		}
		opts = append(opts, ukue.At(t))
	}
	if *attempts != 0 {
		opts = append(opts, ukue.MaxAttempts(*attempts))
	}
	if *priority != 0 {
		opts = append(opts, ukue.Priority(*priority))
	}
	q, err := open(pos[0], true)
	if err != nil {
		return err
	}
	defer q.Close()
	id, err := q.Enqueue(ctx, pos[1], payload, opts...)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, id)
	return nil
}

func cmdStats(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("stats", "stats [--json] FILE", stderr)
	asJSON := fs.Bool("json", false, "print JSON")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errUsage{"give one FILE"}
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	st, err := q.Stats(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		type row struct {
			Queue     string  `json:"queue"`
			Ready     int64   `json:"ready"`
			Delayed   int64   `json:"delayed"`
			Running   int64   `json:"running"`
			Expired   int64   `json:"expired"`
			Dead      int64   `json:"dead"`
			Done      int64   `json:"done"`
			OldestDue *string `json:"oldest_due"`
		}
		rows := make([]row, 0, len(st))
		for _, s := range st {
			rows = append(rows, row{s.Queue, s.Ready, s.Delayed, s.Running, s.Expired, s.Dead, s.Done, isoPtr(s.OldestDue)})
		}
		return printJSON(stdout, map[string]any{"queues": rows})
	}
	if len(st) == 0 {
		fmt.Fprintln(stdout, "no jobs")
		return nil
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "QUEUE\tREADY\tDELAYED\tRUNNING\tEXPIRED\tDEAD\tDONE\tOLDEST WAIT")
	for _, s := range st {
		wait := "-"
		if !s.OldestDue.IsZero() {
			wait = time.Since(s.OldestDue).Round(time.Second).String()
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n", printable(s.Queue), s.Ready, s.Delayed, s.Running, s.Expired, s.Dead, s.Done, wait)
	}
	return tw.Flush()
}

func cmdList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("list", "list [options] FILE", stderr)
	queue := fs.String("queue", "", "only this queue")
	state := fs.String("state", "", "only this state: ready, delayed, running, dead or done")
	limit := fs.Int("limit", 50, "at most this many jobs (up to 1000)")
	after := fs.Int64("after", 0, "only jobs with a larger ID, for paging")
	asJSON := fs.Bool("json", false, "print JSON, payloads included")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errUsage{"give one FILE"}
	}
	f := ukue.ListFilter{Queue: *queue, AfterID: *after, Limit: *limit}
	if *state != "" {
		st, err := ukue.ParseState(*state)
		if err != nil {
			return err
		}
		f.State = st
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	jobs, err := q.List(ctx, f)
	if err != nil {
		return err
	}
	if *asJSON {
		out := make([]map[string]any, 0, len(jobs))
		for i := range jobs {
			out = append(out, infoMap(&jobs[i]))
		}
		return printJSON(stdout, map[string]any{"jobs": out})
	}
	if len(jobs) == 0 {
		fmt.Fprintln(stdout, "no jobs")
		return nil
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tQUEUE\tSTATE\tTRIES\tRUN AT\tPAYLOAD\tLAST ERROR")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d/%d\t%s\t%s\t%s\n", j.ID, printable(j.Queue), j.State, j.Attempts, j.MaxAttempts,
			j.RunAt.UTC().Format("2006-01-02 15:04:05"), preview(j.Payload, 40), preview([]byte(j.LastError), 50))
	}
	return tw.Flush()
}

func cmdShow(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("show", "show [--json] FILE ID", stderr)
	asJSON := fs.Bool("json", false, "print JSON")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errUsage{"give FILE and ID"}
	}
	id, err := strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return errUsage{"ID must be a whole number"}
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	j, err := q.Get(ctx, id)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(stdout, infoMap(j))
	}
	fmt.Fprintf(stdout, "id:          %d\nqueue:       %s\nstate:       %s\ntries:       %d of %d\npriority:    %d\nrun at:      %s\n",
		j.ID, printable(j.Queue), j.State, j.Attempts, j.MaxAttempts, j.Priority, j.RunAt.UTC().Format(time.RFC3339))
	if !j.LeaseUntil.IsZero() {
		fmt.Fprintf(stdout, "lease until: %s\n", j.LeaseUntil.UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(stdout, "created:     %s\nupdated:     %s\n", j.CreatedAt.UTC().Format(time.RFC3339), j.UpdatedAt.UTC().Format(time.RFC3339))
	if j.LastError != "" {
		fmt.Fprintf(stdout, "last error:  %s\n", printable(j.LastError))
	}
	fmt.Fprintf(stdout, "payload:     %d bytes\n", len(j.Payload))
	if utf8.Valid(j.Payload) {
		fmt.Fprintf(stdout, "%s\n", printable(string(j.Payload)))
	} else {
		fmt.Fprintln(stdout, "(binary; use --json to get it as base64)")
	}
	return nil
}

func cmdRetry(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("retry", "retry FILE ID... | retry --all [--queue QUEUE] FILE", stderr)
	all := fs.Bool("all", false, "retry every dead job")
	queue := fs.String("queue", "", "with --all, only this queue")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || (*all && len(pos) != 1) || (!*all && len(pos) < 2) {
		return errUsage{"give FILE and one or more IDs, or FILE with --all"}
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	if *all {
		n, err := q.RetryAll(ctx, *queue)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%d dead jobs moved back to ready\n", n)
		return nil
	}
	for _, s := range pos[1:] {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errUsage{fmt.Sprintf("%q is not a job ID", s)}
		}
		if err := q.Retry(ctx, id); err != nil {
			if errors.Is(err, ukue.ErrNotFound) {
				return fmt.Errorf("job %d is not dead, or doesn't exist", id)
			}
			return err
		}
		fmt.Fprintf(stdout, "job %d moved back to ready\n", id)
	}
	return nil
}

func cmdPurge(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("purge", "purge --state dead|done [--queue QUEUE] FILE", stderr)
	state := fs.String("state", "", "dead or done (required)")
	queue := fs.String("queue", "", "only this queue")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errUsage{"give one FILE"}
	}
	if *state != "dead" && *state != "done" {
		return errUsage{"--state must be dead or done"}
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	n, err := q.Purge(ctx, ukue.State(*state), *queue)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%d %s jobs deleted\n", n, *state)
	return nil
}

func cmdDelete(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("delete", "delete FILE ID...", stderr)
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return errUsage{"give FILE and one or more IDs"}
	}
	q, err := open(pos[0], false)
	if err != nil {
		return err
	}
	defer q.Close()
	for _, s := range pos[1:] {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errUsage{fmt.Sprintf("%q is not a job ID", s)}
		}
		if err := q.Delete(ctx, id); err != nil {
			if errors.Is(err, ukue.ErrNotFound) {
				return fmt.Errorf("there is no job %d", id)
			}
			return err
		}
		fmt.Fprintf(stdout, "job %d deleted\n", id)
	}
	return nil
}

// readPayload reads standard input, and gives up if ukue is interrupted
// while it waits.
func readPayload(ctx context.Context, stdin io.Reader, stderr io.Writer) ([]byte, error) {
	if f, ok := stdin.(*os.File); ok {
		if st, err := f.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
			fmt.Fprintln(stderr, "Reading the payload from the keyboard; end it with Ctrl-D (Ctrl-Z then Enter on Windows).")
		}
	}
	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(stdin)
		done <- result{b, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, fmt.Errorf("read payload: %w", r.err)
		}
		return r.b, nil
	case <-ctx.Done():
		return nil, errors.New("interrupted while reading the payload")
	}
}

// printable escapes control characters, so a payload or an error message
// can't send escape sequences to the terminal. Newlines and tabs stay.
func printable(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\n' || r == '\t':
			b.WriteRune(r)
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0):
			fmt.Fprintf(&b, "\\x%02x", r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func preview(b []byte, n int) string {
	if !utf8.Valid(b) {
		return fmt.Sprintf("(%d bytes, binary)", len(b))
	}
	s := strings.Join(strings.Fields(printable(string(b))), " ")
	if utf8.RuneCountInString(s) > n {
		r := []rune(s)
		s = string(r[:n-3]) + "..."
	}
	if s == "" {
		return "-"
	}
	return s
}

func isoPtr(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	return &s
}

func infoMap(j *ukue.JobInfo) map[string]any {
	m := map[string]any{
		"id": j.ID, "queue": j.Queue, "state": j.State, "priority": j.Priority,
		"attempts": j.Attempts, "max_attempts": j.MaxAttempts, "run_at": isoPtr(j.RunAt),
		"lease_until": isoPtr(j.LeaseUntil), "last_error": j.LastError,
		"created_at": isoPtr(j.CreatedAt), "updated_at": isoPtr(j.UpdatedAt),
	}
	if utf8.Valid(j.Payload) {
		m["payload"] = string(j.Payload)
	} else {
		m["payload_base64"] = j.Payload // encoded as base64 by encoding/json
	}
	return m
}

// newLogger writes log lines with the time of day only, which keeps them
// short enough to read in a terminal.
func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05"))
			}
			return a
		},
	}))
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
