import type { DeadLetterEntry } from "@/lib/relay";

function timeAgo(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const s = Math.max(0, Math.round(diffMs / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  return `${h}h ago`;
}

export function DeadLetterTable({ entries }: { entries: DeadLetterEntry[] }) {
  return (
    <div className="flex flex-col gap-3 px-5 py-4">
      <h3 className="text-[13px] text-ink-secondary">Recent permanent failures</h3>
      {entries.length === 0 ? (
        <div className="flex h-24 items-center justify-center text-[12px] text-ink-muted">
          No permanent failures — nothing has exhausted its retries.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] border-collapse text-[13px]">
            <thead>
              <tr className="text-left text-[11px] uppercase tracking-wide text-ink-muted">
                <th className="pb-2 pr-4 font-medium">Task</th>
                <th className="pb-2 pr-4 font-medium">Command</th>
                <th className="pb-2 pr-4 font-medium">Error</th>
                <th className="pb-2 pr-4 font-medium">Attempts</th>
                <th className="pb-2 font-medium">Failed</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <tr key={e.task_id} className="border-t border-border">
                  <td className="py-2 pr-4">
                    <span className="inline-flex items-center gap-1.5">
                      <span aria-hidden className="size-1.5 shrink-0 rounded-full bg-status-critical" />
                      <span className="font-mono text-[12px] text-ink-secondary">
                        {e.task_id.slice(0, 8)}
                      </span>
                    </span>
                  </td>
                  <td className="py-2 pr-4 font-mono text-[12px] text-ink-secondary">
                    {e.spec.image} {e.spec.cmd.join(" ")}
                  </td>
                  <td className="max-w-[280px] truncate py-2 pr-4 text-ink-primary" title={e.error}>
                    {e.error}
                  </td>
                  <td className="py-2 pr-4 text-ink-secondary">{e.attempts}</td>
                  <td className="py-2 text-ink-muted">{timeAgo(e.failed_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
