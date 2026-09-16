#!/usr/bin/env bash
# scripts/benchmark.sh — Phase 6 systematic benchmark: sweeps worker count and
# submit concurrency against a release build of Relay (no -race — that flag
# adds real overhead you don't want in a performance number), multiple runs
# per configuration, full environment recorded, results written to a
# timestamped Markdown file under benchmarks/results/.
#
# The task used throughout is a near-instant `echo hi` in alpine, not an
# artificial sleep — the point is measuring Relay's own overhead (HTTP
# submit + queue + dispatch + container create/start/teardown), not hiding
# it behind a longer task duration.
set -euo pipefail

cd "$(dirname "$0")/.."

REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"
WORKER_COUNTS=(4 8)
CONCURRENCIES=(10 50 100)
RUNS_PER_CONFIG="${RUNS_PER_CONFIG:-3}"
N="${N:-200}"
IMAGE=alpine
TASK_CMD="echo hi"

RESULTS_DIR="benchmarks/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_FILE="$RESULTS_DIR/bench-$TIMESTAMP.md"

echo "Building release binaries (no -race)..."
go build -o /tmp/bench-relay ./cmd/relay
go build -o /tmp/bench-loadtest ./cmd/loadtest

if ! docker exec relay-redis redis-cli ping >/dev/null 2>&1; then
  echo "Starting Redis..."
  docker run -d --name relay-redis --restart unless-stopped -p 6379:6379 redis:7-alpine >/dev/null
  sleep 1
fi

{
  echo "# Relay Benchmark — $TIMESTAMP"
  echo
  echo "## Environment"
  echo
  echo '```'
  echo "CPU:      $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
  echo "Cores:    $(nproc)"
  echo "Memory:   $(free -h | awk '/^Mem:/{print $2}')"
  echo "OS:       $(uname -srm)"
  echo "Go:       $(go version | awk '{print $3}')"
  echo "Docker:   $(docker --version)"
  echo "Task:     image=$IMAGE cmd=[\"sh\",\"-c\",\"$TASK_CMD\"] (near-instant — measures Relay's own overhead)"
  echo "N/run:    $N requests"
  echo "Runs:     $RUNS_PER_CONFIG per configuration"
  echo '```'
  echo
  echo "Not an isolated benchmark box — this is the same dev laptop used"
  echo "throughout the project, running under normal desktop load. Numbers"
  echo "are directionally real, not lab-grade precise; noted honestly rather"
  echo "than presented as more rigorous than they are."
  echo
  echo "## Results"
  echo
  echo "| Workers | Concurrency | Run | Submitted | Rejected | Errors | Submit P50 | Submit P99 | E2E P50 | E2E P99 | Throughput (tasks/s) | Peak Busy |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
} > "$RESULTS_FILE"

for workers in "${WORKER_COUNTS[@]}"; do
  for c in "${CONCURRENCIES[@]}"; do
    echo "=== workers=$workers concurrency=$c ==="

    /tmp/bench-relay -workers "$workers" -redis-addr "$REDIS_ADDR" -max-retries 0 \
      > "/tmp/bench-relay-w${workers}-c${c}.log" 2>&1 &
    SERVER_PID=$!
    sleep 1
    if ! curl -s -m 2 localhost:8080/healthz > /dev/null; then
      echo "server failed to start (see /tmp/bench-relay-w${workers}-c${c}.log)" >&2
      cat "/tmp/bench-relay-w${workers}-c${c}.log" >&2
      exit 1
    fi

    for run in $(seq 1 "$RUNS_PER_CONFIG"); do
      echo "  run $run/$RUNS_PER_CONFIG..."
      OUT=$(/tmp/bench-loadtest -n "$N" -c "$c" -image "$IMAGE" -cmd "$TASK_CMD" -timeout 10 2>&1)

      submitted=$(echo "$OUT" | grep '^submitted (202):' | sed 's/.*: *//')
      rejected=$(echo "$OUT" | grep '^rejected (503' | sed 's/.*: *//')
      errors=$(echo "$OUT" | grep '^submit errors:' | sed 's/.*: *//')
      sub_p50=$(echo "$OUT" | grep '^submit latency:' | sed -E 's/.*p50=([^ ]+) p99=.*/\1/')
      sub_p99=$(echo "$OUT" | grep '^submit latency:' | sed -E 's/.*p99=([^ ]+)/\1/')
      e2e_p50=$(echo "$OUT" | grep '^end-to-end latency:' | sed -E 's/.*p50=([^ ]+) p99=.*/\1/')
      e2e_p99=$(echo "$OUT" | grep '^end-to-end latency:' | sed -E 's/.*p99=([^ ]+)/\1/')
      throughput=$(echo "$OUT" | grep '^throughput:' | awk '{print $2}')
      peak_busy=$(echo "$OUT" | grep '^peak workers busy:' | sed 's/.*: *//')

      echo "| $workers | $c | $run | $submitted | $rejected | $errors | $sub_p50 | $sub_p99 | $e2e_p50 | $e2e_p99 | $throughput | $peak_busy |" \
        >> "$RESULTS_FILE"

      if ! echo "$OUT" | grep -q "^PASS"; then
        echo "  !! run $run did not report PASS — see /tmp/bench-loadtest-w${workers}-c${c}-r${run}.log" >&2
        echo "$OUT" > "/tmp/bench-loadtest-w${workers}-c${c}-r${run}.log"
      fi
    done

    kill -INT "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true

    leaked=$(docker ps -a --filter "name=relay-task" -q | wc -l)
    if [ "$leaked" -gt 0 ]; then
      echo "  !! WARNING: $leaked leaked containers after workers=$workers c=$c" | tee -a "$RESULTS_FILE" >&2
    fi
  done
done

echo
echo "Results written to $RESULTS_FILE"
