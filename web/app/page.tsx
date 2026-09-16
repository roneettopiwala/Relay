"use client";

import { usePolling } from "@/lib/usePolling";
import type { Stats, DeadLettersResponse } from "@/lib/relay";
import { StatTile } from "@/components/StatTile";
import { TrendChart } from "@/components/TrendChart";
import { DeadLetterTable } from "@/components/DeadLetterTable";
import { formatCompact, formatMs } from "@/lib/format";

const POLL_INTERVAL_MS = 2000;

export default function DashboardPage() {
  const stats = usePolling<Stats>("/api/stats", POLL_INTERVAL_MS);
  const deadLetters = usePolling<DeadLettersResponse>("/api/dead-letters", POLL_INTERVAL_MS * 3);

  const s = stats.data;
  const totalTerminal = s ? s.completed + s.failed : 0;
  const failureRate = totalTerminal > 0 ? ((s!.failed / totalTerminal) * 100).toFixed(1) : "0.0";

  // Queue depth / worker utilization trend — built from the client's own
  // rolling poll history, since Relay only ever reports a live snapshot.
  const queueSeries = stats.history.map((h) => ({ t: h.t, pending: h.data.pending }));
  const workerSeries = stats.history.map((h) => ({
    t: h.t,
    busy: h.data.workers_busy,
    total: h.data.workers_total,
  }));
  const latencySeries = stats.history.map((h) => ({
    t: h.t,
    p50: h.data.p50_latency_ms,
    p99: h.data.p99_latency_ms,
  }));

  // Throughput isn't reported directly — completed+failed are cumulative
  // counters, so the delta between two consecutive samples, over the time
  // between them, is a genuine tasks/sec figure.
  const throughputSeries = stats.history.slice(1).map((h, i) => {
    const prev = stats.history[i];
    const dt = (h.t - prev.t) / 1000;
    const dCount =
      h.data.completed + h.data.failed - (prev.data.completed + prev.data.failed);
    return { t: h.t, throughput: dt > 0 ? Math.max(0, dCount / dt) : 0 };
  });

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-8 px-6 py-10">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-[28px] font-semibold tracking-tight text-ink-primary">Overview</h1>
          <p className="mt-1 text-[13px] text-ink-secondary">
            Live queue, worker, and failure state — polled every {POLL_INTERVAL_MS / 1000}s.
          </p>
        </div>
        <ConnectionPill hasLoaded={stats.hasLoaded} connected={stats.connected} error={stats.error} />
      </div>

      {/* stat tiles */}
      <div className="grid grid-cols-2 divide-x divide-y divide-border rounded-lg border border-border sm:grid-cols-3 lg:grid-cols-6">
        <StatTile label="Queue depth" value={s ? formatCompact(s.pending) : "—"} statusColor="var(--status-info)" />
        <StatTile label="Running" value={s ? formatCompact(s.running) : "—"} statusColor="var(--status-info)" />
        <StatTile
          label="Workers busy"
          value={s ? `${s.workers_busy} / ${s.workers_total}` : "—"}
        />
        <StatTile label="Completed" value={s ? formatCompact(s.completed) : "—"} statusColor="var(--status-good)" />
        <StatTile label="Failed" value={s ? formatCompact(s.failed) : "—"} statusColor="var(--status-critical)" />
        <StatTile label="Failure rate" value={`${failureRate}%`} />
      </div>

      {/* trend charts */}
      <div className="grid grid-cols-1 divide-y divide-border rounded-lg border border-border md:grid-cols-2 md:divide-x md:divide-y-0">
        <TrendChart
          title="Queue depth"
          data={queueSeries}
          series={[{ key: "pending", color: "var(--line)", label: "Pending" }]}
        />
        <TrendChart
          title="Worker utilization"
          data={workerSeries}
          series={[{ key: "busy", color: "var(--line)", label: "Busy" }]}
        />
        <TrendChart
          title="Throughput"
          data={throughputSeries}
          series={[{ key: "throughput", color: "var(--line)", label: "Tasks/sec" }]}
          valueFormatter={(v) => `${v.toFixed(2)}/s`}
        />
        <TrendChart
          title="Latency"
          data={latencySeries}
          series={[
            { key: "p50", color: "#3987e5", label: "P50" },
            { key: "p99", color: "#d95926", label: "P99" },
          ]}
          valueFormatter={(v) => formatMs(v)}
        />
      </div>

      {/* dead-letter feed */}
      <div className="rounded-lg border border-border">
        {deadLetters.data === null ? (
          <div className="flex h-24 items-center justify-center text-[12px] text-ink-muted">
            Loading…
          </div>
        ) : (
          <DeadLetterTable entries={deadLetters.data.entries} />
        )}
      </div>
    </div>
  );
}

function ConnectionPill({
  hasLoaded,
  connected,
  error,
}: {
  hasLoaded: boolean;
  connected: boolean;
  error: string | null;
}) {
  // Distinguish three real states, not two: mid-flight on the very first
  // poll looks nothing like "we polled and it failed" or "we lost a
  // connection that was working" — collapsing them reads as a false
  // "disconnected" for the first ~2s of every page load.
  const label = connected ? "Live" : hasLoaded ? (error ?? "Disconnected") : "Connecting…";
  const color = connected ? "var(--status-good)" : hasLoaded ? "var(--status-critical)" : "var(--status-info)";

  return (
    <div className="flex items-center gap-1.5 rounded-full border border-border px-2.5 py-1">
      <span aria-hidden className="size-1.5 rounded-full" style={{ backgroundColor: color }} />
      <span className="text-[12px] text-ink-secondary">{label}</span>
    </div>
  );
}
