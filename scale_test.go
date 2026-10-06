// Copyright ukue.com 2026
// SPDX-License-Identifier: Apache-2.0

package ukue

import (
	"strings"
	"testing"
	"time"
)

// TestManyScheduledJobsDontSlowClaims fills a queue with 100,000 jobs
// scheduled for later, in five priorities, and checks that looking for work
// and claiming stay index lookups instead of scans.
func TestManyScheduledJobsDontSlowClaims(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	q, _ := openTemp(t)
	const n = 100_000
	nowMs := time.Now().UnixMilli()
	tx, err := q.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(insertJob)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		later := nowMs + int64(time.Hour/time.Millisecond) + int64(i)
		if _, err := stmt.Exec("reminders", []byte("x"), i%5, 10, later, nowMs, nowMs); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, query := range []struct{ sql, index string }{
		{`SELECT EXISTS (SELECT 1 FROM ukue_jobs WHERE queue = 'reminders' AND state = 'ready' AND run_at <= 0)`, "ukue_jobs_next"},
		{`SELECT max(priority) FROM ukue_jobs WHERE queue = 'reminders' AND state = 'ready' AND priority < 3`, "ukue_jobs_due"},
		{`SELECT id FROM ukue_jobs WHERE queue = 'reminders' AND state = 'ready' AND priority = 2 AND run_at <= 0 ORDER BY run_at, id LIMIT 1`, "ukue_jobs_due"},
		{`SELECT min(run_at) FROM ukue_jobs WHERE queue = 'reminders' AND state = 'ready'`, "ukue_jobs_next"},
	} {
		rows, err := q.DB().Query("EXPLAIN QUERY PLAN " + query.sql)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			rows.Scan(&id, &parent, &notUsed, &detail)
			plan = append(plan, detail)
		}
		rows.Close()
		if p := strings.Join(plan, "; "); !strings.Contains(p, query.index) || strings.Contains(p, "SCAN ukue_jobs ") {
			t.Errorf("plan for %s:\n%s", query.sql, p)
		}
	}

	timeIt := func(what string, f func()) time.Duration {
		const rounds = 50
		start := time.Now()
		for i := 0; i < rounds; i++ {
			f()
		}
		avg := time.Since(start) / rounds
		t.Logf("%s with %d jobs scheduled for later: %s on average", what, n, avg.Round(time.Microsecond))
		if avg > 20*time.Millisecond {
			t.Errorf("%s is slow: %s", what, avg)
		}
		return avg
	}
	timeIt("an empty claim", func() {
		if j := claim(t, q, "reminders", time.Minute); j != nil {
			t.Fatal("a scheduled job ran early")
		}
	})
	timeIt("finding the next due time", func() {
		if _, ok, err := q.nextChange(bg, "reminders"); err != nil || !ok {
			t.Fatal(ok, err)
		}
	})
	timeIt("adding a due job and claiming it", func() {
		must[int64](t)(q.Enqueue(bg, "reminders", []byte("now"), Priority(1)))
		j := claim(t, q, "reminders", time.Minute)
		if j == nil || string(j.Payload) != "now" {
			t.Fatalf("got %+v", j)
		}
		if err := q.Ack(bg, j); err != nil {
			t.Fatal(err)
		}
	})
}
