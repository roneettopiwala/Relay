# Relay vs. raw execution

Same environment as `bench-20260916-131439.md` (11th Gen i7-1165G7, 8 cores,
14GB RAM, shared dev laptop under normal desktop load — not an isolated
benchmark box). Methodology and safety notes are per-section below.

## 1. Latency overhead

The question: how much slower is a task routed through Relay (HTTP submit →
Redis queue → dispatch → Docker) than running the same container directly?

**Method:** 20 sequential runs each — submit, wait for completion, *then* the
next one starts (not a burst). This matters: an earlier attempt using
`cmd/loadtest`'s own submit-then-poll design gave a misleadingly high number,
because that tool submits every task up front (correct for a load test, wrong
for this comparison) — task #20 wound up queued behind 19 others before it
ever started. Fixed by writing a plain sequential curl loop instead. Same
task both sides: `alpine sh -c "echo hi"`, `--cpus 0.5 --memory 128m` (Relay's
defaults, matched on the raw side too).

| | min | P50 | P99 | max |
|---|---|---|---|---|
| Raw `docker run` | 120ms | 137ms | 162ms | 179ms |
| Via Relay | 156ms | 159ms | 185ms | 188ms |

**Relay adds roughly 20-25ms per task** (~16% at P50) on top of Docker's own
container-start cost, which dominates either way. That overhead is the HTTP
round-trip(s), the Redis enqueue/dequeue, and the dispatcher's own
bookkeeping — the price of getting queueing, backpressure, retry, and a
durable failure record, none of which a raw `docker run` has at all.

## 2. Blast radius — what a misbehaving task does to the host

The question: a task that grows memory without bound — what actually happens
with no isolation, versus through Relay?

**Safety note, stated plainly:** I did not run truly unbounded memory growth
on this machine — it had well under 1GB free at the time, and a real
unbounded run risks the kernel OOM-killer taking down unrelated processes,
not just the offending one. The "raw" side below has a `ulimit -v 200000`
(200MB) wrapped around it *by me, for this demo* — that bound is not
something raw execution provides on its own; a real unprotected process has
no such ceiling unless an operator remembers to add one every time. Relay's
`--memory` cap, by contrast, is automatic and mandatory per task — nobody has
to remember it.

Same script both sides, doubling a shell variable until something stops it:
`x="a"; while true; do x="$x$x"; done`

| | Raw (manually `ulimit`-bounded to 200MB) | Via Relay (`--memory 200m`, automatic) |
|---|---|---|
| Time to failure | 1.57s | 2.46s |
| How it ended | **Segfault** (exit 139), core dump | Clean SIGKILL from the cgroup OOM killer (exit 137) |
| Result you get back | A crash dump — no structured info | `{"status":"failed","exit_code":137,"error":"exit code 137"}` |
| Host-level side effects | **Ubuntu's `apport` crash reporter fired**, writing a 664KB crash report to `/var/crash/` (cleaned up after) — an artifact I didn't ask for and had to notice and remove | None — container auto-removed, `docker ps -a` empty, host memory back to baseline |

The raw side only failed *cleanly-ish* because I put a ceiling on it myself,
and even then it crashed messily (segfault, core dump, an unrelated system
service — apport — reacting to it) rather than reporting a clean outcome. A
genuinely raw, unprotected task has no ceiling at all: nothing here stops it
from consuming memory until the kernel starts killing processes system-wide,
possibly including things that have nothing to do with the task. Relay's
container cap is automatic, the failure is structured and inspectable
(`GET /tasks/{id}`, and it lands in `/dead-letters`), and the host is
provably unaffected — verified by checking `docker ps -a` and `free -h`
before and after, not assumed.
