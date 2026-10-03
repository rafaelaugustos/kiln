#!/bin/sh
set -o pipefail
out=/evidence/run-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$out"
export TMPDIR="$out"

while sleep 300; do
	date -u +%FT%TZ >> "$out/stats.txt"
	docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' kiln-soak-pg kiln-soak >> "$out/stats.txt"
done &
sampler=$!

soak -duration "$DURATION" -rate "$RATE" -workers "$WORKERS" \
	-dsn postgres://kiln:kiln@postgres:5432/kiln -container kiln-soak-pg "$@" 2>&1 | tee "$out/summary.txt"
status=$?
kill "$sampler"
echo "$status" > "$out/exit-status"
docker exec kiln-soak-pg pg_dump -U kiln -d kiln | gzip > "$out/database.sql.gz"
echo "evidence in $out (exit $status)"
exit "$status"
