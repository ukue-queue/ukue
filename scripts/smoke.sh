#!/bin/sh
# Copyright ukue.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# A quick check that a built ukue binary works: adds jobs, runs a worker,
# and serves the HTTP API for a moment.
#
#   sh scripts/smoke.sh ./ukue
set -eu
bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
dir=$(mktemp -d)
file="$dir/smoke.ukue"

"$bin" version
"$bin" init "$file"
"$bin" add "$file" smoke hello >/dev/null
"$bin" add "$file" smoke world >/dev/null

printf '#!/bin/sh\ncat >> "%s/out.txt"\necho >> "%s/out.txt"\n' "$dir" "$dir" > "$dir/handler.sh"
chmod +x "$dir/handler.sh"
"$bin" work --quiet --poll 50ms "$file" smoke -- "$dir/handler.sh" &
worker=$!
sleep 3
kill -TERM "$worker"
wait "$worker" || true
got=$(sort "$dir/out.txt" | tr '\n' ' ')
if [ "$got" != "hello world " ]; then
	echo "smoke: the worker wrote: $got"
	exit 1
fi
"$bin" stats "$file"

"$bin" serve --addr 127.0.0.1:7661 "$file" 2>/dev/null &
server=$!
sleep 1
curl -fsS -X POST 127.0.0.1:7661/v1/jobs -d '{"queue": "smoke", "payload": "over http"}' >/dev/null
curl -fsS -X POST 127.0.0.1:7661/v1/claim -d '{"queue": "smoke"}' | grep -q 'over http'
kill -TERM "$server"
wait "$server" || true
rm -rf "$dir"
echo "smoke test passed"
