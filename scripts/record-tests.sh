#!/bin/sh
# Copyright ukue.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs every test, the race detector and the benchmarks, and writes the output
# to test/results/ with a note of the machine they ran on.
#
#   sh scripts/record-tests.sh
set -eu
cd "$(dirname "$0")/.."
out=test/results
mkdir -p "$out"

header() {
	echo "# $1"
	echo "# recorded $(date -u '+%Y-%m-%d %H:%M UTC') with $(go version | cut -d' ' -f3-)"
	echo "# $(uname -sr), $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //'), $(nproc 2>/dev/null || echo '?') CPUs"
	echo "# command: $2"
	echo
}

go vet ./...

cmd="go test -count=1 -v ./..."
{ header "Every test, verbose" "$cmd"; $cmd 2>&1 | grep -v '^time='; } > "$out/tests.txt"

cmd="go test -race -count=1 ./..."
{ header "Every test with the race detector" "$cmd"; $cmd 2>&1 | grep -v '^time='; } > "$out/race.txt"

cmd="go test -run ^$ -bench . -benchtime 3s -count 3 ."
{ header "Benchmarks, three runs each: every operation is a committed transaction with synchronous = FULL" "$cmd"; $cmd 2>&1; } > "$out/bench.txt"

grep -E '^(ok|FAIL|---)' "$out/tests.txt" "$out/race.txt" | grep -v -- '--- PASS' || true
echo "results written to $out/"
