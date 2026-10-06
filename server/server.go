// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

// Package server is the HTTP interface to a ukue file, so programs in any
// language can add, claim and finish jobs. It is what "ukue serve" runs, and
// a Go program can also mount it in its own HTTP server. API.md in the
// repository describes every endpoint.
package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ukue-queue/ukue"
)

// Options configures the HTTP interface.
type Options struct {
	// Token, when set, must come with every request except /v1/health,
	// as "Authorization: Bearer <token>".
	Token string
	// MaxBodyBytes limits the size of a request body. The default is 1 MiB.
	MaxBodyBytes int64
	// MaxWait is the longest a claim may wait for a job. The default is 30 seconds.
	MaxWait time.Duration
	// DefaultLease is the lease for claims that don't ask for one. The default is 1 minute.
	DefaultLease time.Duration
	// Logger receives a line for each request that fails on the server's
	// side. By default nothing is logged.
	Logger *slog.Logger
	// Hosts, when not empty, lists the host names requests may be addressed
	// to, such as "localhost". Requests naming any other host get 403. A
	// server without a token should set it, so that a web page can't reach
	// the server through a DNS name that points at this machine.
	Hosts []string
	// Stop, when closed, makes claims that are waiting for a job answer 204
	// at once, so the server can shut down without waiting for them.
	Stop <-chan struct{}
}

type api struct {
	q *ukue.Queue
	o Options
}

// New returns an http.Handler that serves the API for q.
func New(q *ukue.Queue, o Options) http.Handler {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20
	}
	if o.MaxWait <= 0 {
		o.MaxWait = 30 * time.Second
	}
	if o.DefaultLease <= 0 {
		o.DefaultLease = ukue.DefaultLease
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	a := &api{q: q, o: o}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", a.health)
	mux.HandleFunc("POST /v1/jobs", a.auth(a.enqueue))
	mux.HandleFunc("GET /v1/jobs", a.auth(a.list))
	mux.HandleFunc("GET /v1/jobs/{id}", a.auth(a.get))
	mux.HandleFunc("DELETE /v1/jobs/{id}", a.auth(a.delete))
	mux.HandleFunc("POST /v1/jobs/{id}/ack", a.auth(a.ack))
	mux.HandleFunc("POST /v1/jobs/{id}/fail", a.auth(a.fail))
	mux.HandleFunc("POST /v1/jobs/{id}/extend", a.auth(a.extend))
	mux.HandleFunc("POST /v1/jobs/{id}/release", a.auth(a.release))
	mux.HandleFunc("POST /v1/jobs/{id}/retry", a.auth(a.retry))
	mux.HandleFunc("POST /v1/claim", a.auth(a.claim))
	mux.HandleFunc("GET /v1/stats", a.auth(a.stats))
	mux.HandleFunc("POST /v1/retry", a.auth(a.retryAll))
	mux.HandleFunc("POST /v1/purge", a.auth(a.purge))

	var h http.Handler = jsonErrors(mux)
	// Web pages from other origins can't change anything through the API,
	// even when the browser runs on the same machine as the server.
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusForbidden, "requests from other web pages aren't allowed")
	}))
	h = cop.Handler(h)
	if len(o.Hosts) > 0 {
		h = hostCheck(o.Hosts, h)
	}
	return h
}

// hostCheck refuses requests addressed to a host name not on the list.
func hostCheck(hosts []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		allowed[strings.ToLower(strings.Trim(h, "[]"))] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if !allowed[strings.ToLower(strings.Trim(host, "[]"))] {
			writeErr(w, http.StatusForbidden, "this server only answers requests addressed to "+strings.Join(hosts, ", "))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// jsonErrors answers requests that match no endpoint with a JSON error, as
// every endpoint does, instead of the mux's plain text.
func jsonErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		var allow []string
		for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
			r2 := r.Clone(r.Context())
			r2.Method = m
			if _, p := mux.Handler(r2); p != "" {
				allow = append(allow, m)
			}
		}
		if len(allow) > 0 {
			w.Header().Set("Allow", strings.Join(allow, ", "))
			writeErr(w, http.StatusMethodNotAllowed, r.Method+" is not allowed here; use "+strings.Join(allow, " or "))
			return
		}
		writeErr(w, http.StatusNotFound, "no such endpoint; the API is described at https://ukue.com/http-api/")
	})
}

func (a *api) auth(next http.HandlerFunc) http.HandlerFunc {
	if a.o.Token == "" {
		return next
	}
	want := []byte("Bearer " + a.o.Token)
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ukue"`)
			writeErr(w, http.StatusUnauthorized, "missing or wrong token")
			return
		}
		next(w, r)
	}
}

// Duration reads a length of time given as a number of seconds or as a Go
// duration string such as "1m30s".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("bad duration %q", s)
		}
		*d = Duration(v)
		return nil
	}
	var secs float64
	if err := json.Unmarshal(b, &secs); err != nil {
		return errors.New(`a duration must be a number of seconds or a string such as "90s"`)
	}
	if secs > 1e9 || secs < -1e9 {
		return errors.New("duration out of range")
	}
	*d = Duration(secs * float64(time.Second))
	return nil
}

// Payload is a job payload in a request. A JSON string is stored as its
// text, and any other JSON value is stored as compact JSON.
type Payload []byte

// UnmarshalJSON implements json.Unmarshaler.
func (p *Payload) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*p = Payload(s)
		return nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return err
	}
	*p = Payload(buf.Bytes())
	return nil
}

func (a *api) readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.o.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeErr(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("the request body is larger than %d bytes", a.o.MaxBodyBytes))
			return false
		}
		writeErr(w, http.StatusBadRequest, "could not read the request body")
		return false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad JSON: "+err.Error())
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeErr(w, http.StatusBadRequest, "bad JSON: there is more after the end of the object")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// opErr answers for an error from the queue: bad input is the client's
// fault, a lost lease is a conflict, and anything else is the server's.
func (a *api) opErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ukue.ErrInvalid):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ukue.ErrLeaseLost):
		writeErr(w, http.StatusConflict, "this worker no longer holds the job: its lease ran out, or it was finished or deleted")
	case errors.Is(err, ukue.ErrNotFound):
		writeErr(w, http.StatusNotFound, "no such job")
	case r.Context().Err() != nil:
		// The client went away; there is no one to answer.
	default:
		a.o.Logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeErr(w, http.StatusBadRequest, "the job id must be a positive whole number")
		return 0, false
	}
	return id, true
}

func ts(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	return &s
}

// putPayload adds the payload as text when it is valid UTF-8, and as base64
// otherwise.
func putPayload(m map[string]any, p []byte) {
	if utf8.Valid(p) {
		m["payload"] = string(p)
	} else {
		m["payload_base64"] = base64.StdEncoding.EncodeToString(p)
	}
}

func jobJSON(j *ukue.Job) map[string]any {
	m := map[string]any{
		"id":           j.ID,
		"queue":        j.Queue,
		"attempt":      j.Attempt,
		"max_attempts": j.MaxAttempts,
		"priority":     j.Priority,
		"token":        j.Token,
		"lease_until":  ts(j.LeaseUntil),
		"created_at":   ts(j.CreatedAt),
		"last_error":   j.LastError,
	}
	putPayload(m, j.Payload)
	return m
}

func infoJSON(j *ukue.JobInfo) map[string]any {
	m := map[string]any{
		"id":           j.ID,
		"queue":        j.Queue,
		"state":        j.State,
		"priority":     j.Priority,
		"attempts":     j.Attempts,
		"max_attempts": j.MaxAttempts,
		"run_at":       ts(j.RunAt),
		"lease_until":  ts(j.LeaseUntil),
		"last_error":   j.LastError,
		"created_at":   ts(j.CreatedAt),
		"updated_at":   ts(j.UpdatedAt),
	}
	putPayload(m, j.Payload)
	return m
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "version": ukue.Version, "format_version": ukue.FormatVersion})
}

type enqueueReq struct {
	Queue         string    `json:"queue"`
	Payload       *Payload  `json:"payload"`
	PayloadBase64 *string   `json:"payload_base64"`
	Delay         *Duration `json:"delay"`
	RunAt         *string   `json:"run_at"`
	MaxAttempts   int       `json:"max_attempts"`
	Priority      int       `json:"priority"`
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueReq
	if !a.readJSON(w, r, &req) {
		return
	}
	var payload []byte
	switch {
	case req.Payload != nil && req.PayloadBase64 != nil:
		writeErr(w, http.StatusBadRequest, "send payload or payload_base64, not both")
		return
	case req.PayloadBase64 != nil:
		p, err := base64.StdEncoding.DecodeString(*req.PayloadBase64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "payload_base64 is not valid base64")
			return
		}
		payload = p
	case req.Payload != nil:
		payload = *req.Payload
	}
	var opts []ukue.EnqueueOption
	if req.Delay != nil {
		opts = append(opts, ukue.Delay(time.Duration(*req.Delay)))
	}
	if req.RunAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *req.RunAt)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "run_at must be an RFC 3339 time such as 2026-10-06T09:00:00Z")
			return
		}
		opts = append(opts, ukue.At(t))
	}
	if req.MaxAttempts != 0 {
		opts = append(opts, ukue.MaxAttempts(req.MaxAttempts))
	}
	if req.Priority != 0 {
		opts = append(opts, ukue.Priority(req.Priority))
	}
	id, err := a.q.Enqueue(r.Context(), req.Queue, payload, opts...)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

type claimReq struct {
	Queue string    `json:"queue"`
	Lease *Duration `json:"lease"`
	Wait  *Duration `json:"wait"`
}

func (a *api) claim(w http.ResponseWriter, r *http.Request) {
	var req claimReq
	if !a.readJSON(w, r, &req) {
		return
	}
	lease := a.o.DefaultLease
	if req.Lease != nil {
		if *req.Lease <= 0 {
			writeErr(w, http.StatusBadRequest, "lease must be positive")
			return
		}
		lease = time.Duration(*req.Lease)
	}
	var wait time.Duration
	if req.Wait != nil {
		wait = min(max(time.Duration(*req.Wait), 0), a.o.MaxWait)
	}
	ctx := r.Context()
	deadline := time.Now().Add(wait)
	for {
		wake := a.q.Wake()
		job, err := a.q.Claim(ctx, req.Queue, lease)
		if err != nil {
			a.opErr(w, r, err)
			return
		}
		if job != nil && ctx.Err() != nil {
			// The client left while the claim went through; put the job
			// back so it doesn't wait out the lease.
			_ = a.q.Release(context.WithoutCancel(ctx), job)
			return
		}
		if job != nil {
			writeJSON(w, http.StatusOK, jobJSON(job))
			return
		}
		left := time.Until(deadline)
		if left <= 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Jobs added through this server end the wait at once. Jobs that
		// other programs write straight into the file are found by polling.
		t := time.NewTimer(min(left, 250*time.Millisecond))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-a.o.Stop:
			t.Stop()
			w.WriteHeader(http.StatusNoContent)
			return
		case <-wake:
		case <-t.C:
		}
		t.Stop()
	}
}

type heldReq struct {
	Token   string    `json:"token"`
	Error   string    `json:"error"`
	RetryIn *Duration `json:"retry_in"`
	Dead    bool      `json:"dead"`
	Lease   *Duration `json:"lease"`
}

// held reads the request for a job a worker holds. Only the ID and the
// token are needed to find it.
func (a *api) held(w http.ResponseWriter, r *http.Request, req *heldReq) (*ukue.Job, bool) {
	id, ok := pathID(w, r)
	if !ok {
		return nil, false
	}
	if !a.readJSON(w, r, req) {
		return nil, false
	}
	if req.Token == "" {
		writeErr(w, http.StatusBadRequest, "token is required; it comes with the claimed job")
		return nil, false
	}
	return &ukue.Job{ID: id, Token: req.Token}, true
}

func (a *api) ack(w http.ResponseWriter, r *http.Request) {
	var req heldReq
	job, ok := a.held(w, r, &req)
	if !ok {
		return
	}
	if err := a.q.Ack(r.Context(), job); err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *api) fail(w http.ResponseWriter, r *http.Request) {
	var req heldReq
	job, ok := a.held(w, r, &req)
	if !ok {
		return
	}
	// The attempt counts are read from the file, so the client only has to
	// send the token. Fail checks the token again when it writes.
	info, err := a.q.Get(r.Context(), job.ID)
	if errors.Is(err, ukue.ErrNotFound) {
		err = ukue.ErrLeaseLost
	}
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	job.Queue, job.Attempt, job.MaxAttempts = info.Queue, info.Attempts, info.MaxAttempts
	msg := req.Error
	if msg == "" {
		msg = "failed"
	}
	cause := errors.New(msg)
	switch {
	case req.Dead:
		cause = ukue.Permanent(cause)
	case req.RetryIn != nil:
		cause = ukue.RetryAfter(cause, max(time.Duration(*req.RetryIn), 0))
	}
	res, err := a.q.Fail(r.Context(), job, cause)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	out := map[string]any{"state": res.State}
	if res.State == ukue.StateReady {
		out["run_at"] = ts(res.RunAt)
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) extend(w http.ResponseWriter, r *http.Request) {
	var req heldReq
	job, ok := a.held(w, r, &req)
	if !ok {
		return
	}
	lease := a.o.DefaultLease
	if req.Lease != nil {
		if *req.Lease <= 0 {
			writeErr(w, http.StatusBadRequest, "lease must be positive")
			return
		}
		lease = time.Duration(*req.Lease)
	}
	if err := a.q.Extend(r.Context(), job, lease); err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_until": ts(job.LeaseUntil)})
}

func (a *api) release(w http.ResponseWriter, r *http.Request) {
	var req heldReq
	job, ok := a.held(w, r, &req)
	if !ok {
		return
	}
	if err := a.q.Release(r.Context(), job); err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	info, err := a.q.Get(r.Context(), id)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, infoJSON(info))
}

func (a *api) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := a.q.Delete(r.Context(), id); err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *api) retry(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := a.q.Retry(r.Context(), id); err != nil {
		if errors.Is(err, ukue.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "no dead job with that id")
			return
		}
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	qv := r.URL.Query()
	f := ukue.ListFilter{Queue: qv.Get("queue")}
	if s := qv.Get("state"); s != "" {
		st, err := ukue.ParseState(s)
		if err != nil {
			a.opErr(w, r, err)
			return
		}
		f.State = st
	}
	if s := qv.Get("after"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil || v < 0 {
			writeErr(w, http.StatusBadRequest, "after must be a whole number")
			return
		}
		f.AfterID = v
	}
	if s := qv.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 {
			writeErr(w, http.StatusBadRequest, "limit must be a positive whole number")
			return
		}
		f.Limit = v
	}
	jobs, err := a.q.List(r.Context(), f)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(jobs))
	for i := range jobs {
		out = append(out, infoJSON(&jobs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.q.Stats(r.Context())
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(st))
	for _, s := range st {
		out = append(out, map[string]any{
			"queue": s.Queue, "ready": s.Ready, "delayed": s.Delayed, "running": s.Running,
			"expired": s.Expired, "dead": s.Dead, "done": s.Done, "oldest_due": ts(s.OldestDue),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": out})
}

type bulkReq struct {
	State string `json:"state"`
	Queue string `json:"queue"`
}

func (a *api) purge(w http.ResponseWriter, r *http.Request) {
	var req bulkReq
	if !a.readJSON(w, r, &req) {
		return
	}
	n, err := a.q.Purge(r.Context(), ukue.State(req.State), req.Queue)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

func (a *api) retryAll(w http.ResponseWriter, r *http.Request) {
	var req bulkReq
	if !a.readJSON(w, r, &req) {
		return
	}
	if req.State != "" && req.State != string(ukue.StateDead) {
		writeErr(w, http.StatusBadRequest, `only dead jobs can be retried; leave state out or send "dead"`)
		return
	}
	n, err := a.q.RetryAll(r.Context(), req.Queue)
	if err != nil {
		a.opErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": n})
}
