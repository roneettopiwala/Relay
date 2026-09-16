// Auto-compact number formatting for stat-tile values (1,284 / 12.9K / 4.2M),
// per the dataviz skill's figure spec.
export function formatCompact(n: number): string {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(n);
}

export function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function formatTime(t: number): string {
  return new Date(t).toLocaleTimeString("en-US", { hour12: false });
}
