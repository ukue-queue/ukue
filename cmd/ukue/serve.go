// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ukue-queue/ukue"
	"github.com/ukue-queue/ukue/server"
)

// DefaultAddr is where "ukue serve" listens unless told otherwise.
const DefaultAddr = "127.0.0.1:7660"

func cmdServe(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("serve", "serve [options] FILE", stderr)
	addr := fs.String("addr", DefaultAddr, "address to listen on")
	tokenFile := fs.String("token-file", "", "file holding the token clients must send; UKUE_TOKEN works too")
	allowNoToken := fs.Bool("allow-no-token", false, "allow listening beyond this machine without a token")
	maxBody := fs.Int64("max-body", 1<<20, "largest request body in bytes, payload included")
	keepDone := fs.Bool("keep-done", false, "keep finished jobs in the file instead of deleting them")
	pos, _, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errUsage{"give one FILE"}
	}
	token := os.Getenv("UKUE_TOKEN")
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			return fmt.Errorf("read token file: %w", err)
		}
		token = strings.TrimSpace(string(b))
		if token == "" {
			return fmt.Errorf("token file %s is empty", *tokenFile)
		}
	}
	host, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return errUsage{fmt.Sprintf("--addr %q is not host:port", *addr)}
	}
	if token == "" && !isLoopback(host) && !*allowNoToken {
		return errors.New("listening beyond this machine needs a token: set UKUE_TOKEN or --token-file, or pass --allow-no-token if something else guards the port")
	}

	q, err := open(pos[0], false, ukue.WithKeepDone(*keepDone))
	if err != nil {
		return err
	}
	defer q.Close()

	log := newLogger(stderr, slog.LevelInfo)
	opts := server.Options{Token: token, MaxBodyBytes: *maxBody, Logger: log, Stop: ctx.Done()}
	if token == "" && isLoopback(host) {
		// Without a token, answer only requests addressed to this machine,
		// so a web page can't reach the server through a DNS name that
		// points here.
		opts.Hosts = []string{"localhost", "127.0.0.1", "::1"}
	}
	srv := &http.Server{
		Handler:           server.New(q, opts),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      45 * time.Second, // longer than the longest claim wait
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	auth := "no token"
	if token != "" {
		auth = "token required"
	}
	log.Info("ukue serving", "file", pos[0], "url", "http://"+ln.Addr().String(), "auth", auth, "version", ukue.Version)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	log.Info("ukue stopped")
	return err
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
