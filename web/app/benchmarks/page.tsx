import { StatTile } from "@/components/StatTile";

// This page is a deliberately static snapshot, not another live-polling
// view — a benchmark is a thing you run and capture, not something that
// updates every few seconds. See benchmarks/results/ in the repo for the
// full methodology, raw per-run data, and the anomaly investigation behind
// these numbers.
const CAPTURED_AT = "2026-09-16";

export default function BenchmarksPage() {
  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-8 px-6 py-10">
      <div>
        <h1 className="text-[28px] font-semibold tracking-tight text-ink-primary">Benchmarks</h1>
        <p className="mt-1 text-[13px] text-ink-secondary">
          Captured {CAPTURED_AT} on an 8-core i7-1165G7 laptop — a shared dev machine under
          normal load, not an isolated benchmark box. Full methodology and raw per-run data in{" "}
          <code className="text-ink-primary">benchmarks/results/</code>.
        </p>
      </div>

      <section className="flex flex-col gap-3">
        <h2 className="text-[13px] text-ink-secondary">Throughput &amp; concurrency safety</h2>
        <p className="text-[13px] text-ink-muted">
          18 runs: 4 vs. 8 workers × 10/50/100 concurrent submitters, 3 runs each, a near-instant
          task (<code className="text-ink-secondary">echo hi</code>).
        </p>
        <div className="grid grid-cols-2 divide-x divide-y divide-border rounded-lg border border-border sm:grid-cols-4">
          <StatTile label="Peak worker utilization" value="Exact, every run" statusColor="var(--status-good)" />
          <StatTile label="Throughput @ 4 workers" value="~6.9/s" />
          <StatTile label="Throughput @ 8 workers" value="~8.5/s" />
          <StatTile label="Scaling (4→8 workers)" value="~23%" sublabel="sub-linear — see notes" />
        </div>
        <p className="text-[12px] text-ink-muted">
          Doubling workers didn&apos;t double throughput — per-task time got measurably worse
          under 8-way container contention on this shared host, while peak worker utilization
          stayed exactly correct in every run. The scheduling is provably right; execution cost
          under contention is what&apos;s constrained. One configuration (4 workers, 10 concurrent)
          showed high variance on first pass — investigated rather than averaged blind: 5 extra
          runs confirmed the lower figure was the real steady state, not the two early outliers.
        </p>
      </section>

      <section className="flex flex-col gap-3 rounded-lg border border-border px-5 py-4">
        <h2 className="text-[13px] text-ink-secondary">Latency overhead — Relay vs. raw <code>docker run</code></h2>
        <p className="text-[12px] text-ink-muted">
          20 truly sequential runs each side (submit, wait for completion, then the next one
          starts) — not a burst. An earlier attempt using the load-test tool gave a misleading
          number for exactly this reason; fixed before reporting it.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[420px] border-collapse text-[13px]">
            <thead>
              <tr className="text-left text-[11px] uppercase tracking-wide text-ink-muted">
                <th className="pb-2 pr-4 font-medium"></th>
                <th className="pb-2 pr-4 font-medium">P50</th>
                <th className="pb-2 font-medium">P99</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-t border-border">
                <td className="py-2 pr-4 text-ink-secondary">Raw docker run</td>
                <td className="py-2 pr-4 text-ink-primary">137ms</td>
                <td className="py-2 text-ink-primary">162ms</td>
              </tr>
              <tr className="border-t border-border">
                <td className="py-2 pr-4 text-ink-secondary">Via Relay</td>
                <td className="py-2 pr-4 text-ink-primary">159ms</td>
                <td className="py-2 text-ink-primary">185ms</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="text-[12px] text-ink-muted">
          ~20-25ms overhead (~16% at P50) for queueing, backpressure, retry, and a durable
          failure record that a raw <code>docker run</code> has none of.
        </p>
      </section>

      <section className="flex flex-col gap-3 rounded-lg border border-border px-5 py-4">
        <h2 className="text-[13px] text-ink-secondary">Blast radius — a memory-runaway task</h2>
        <p className="text-[12px] text-ink-muted">
          Same script both sides — a shell loop that doubles a variable until something stops
          it. The raw side was capped with a manual <code>ulimit</code> purely as a safety net
          for running this demo on a real machine; a genuinely unprotected task has no ceiling
          at all.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[520px] border-collapse text-[13px]">
            <thead>
              <tr className="text-left text-[11px] uppercase tracking-wide text-ink-muted">
                <th className="pb-2 pr-4 font-medium"></th>
                <th className="pb-2 pr-4 font-medium">Raw (manually bounded)</th>
                <th className="pb-2 font-medium">Via Relay (automatic)</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-t border-border">
                <td className="py-2 pr-4 text-ink-secondary">Outcome</td>
                <td className="py-2 pr-4 text-ink-primary">
                  <span className="inline-flex items-center gap-1.5">
                    <span aria-hidden className="size-1.5 rounded-full bg-status-critical" />
                    Segfault, core dump
                  </span>
                </td>
                <td className="py-2 text-ink-primary">
                  <span className="inline-flex items-center gap-1.5">
                    <span aria-hidden className="size-1.5 rounded-full bg-status-good" />
                    Clean exit 137, structured result
                  </span>
                </td>
              </tr>
              <tr className="border-t border-border">
                <td className="py-2 pr-4 text-ink-secondary">Host side effects</td>
                <td className="py-2 pr-4 text-ink-primary">
                  Triggered the OS&apos;s own crash reporter (a report file I had to notice and clean up)
                </td>
                <td className="py-2 text-ink-primary">None — verified via <code>docker ps -a</code> and <code>free -h</code></td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
