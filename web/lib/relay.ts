// Server-only config for reaching the Relay API. RELAY_URL is read here
// (not exposed to the client bundle via NEXT_PUBLIC_*) — the browser only
// ever talks to this Next.js app's own /api/* routes, which proxy to Relay
// server-side. That sidesteps CORS entirely (no changes needed to Relay's
// Go API) and keeps Relay's address out of the client bundle, which is the
// right shape if this dashboard is ever deployed somewhere Relay itself
// isn't publicly reachable.
export const RELAY_URL = process.env.RELAY_URL ?? "http://localhost:8080";

// Types mirroring Relay's actual JSON responses (internal/api/api.go).
export interface Stats {
  pending: number;
  running: number;
  completed: number;
  failed: number;
  workers_total: number;
  workers_busy: number;
  p50_latency_ms: number;
  p99_latency_ms: number;
  latency_sample_count: number;
}

export interface TaskSpec {
  image: string;
  cmd: string[];
  cpu_limit: number;
  mem_limit: string;
  timeout: number; // nanoseconds, matches Go's time.Duration JSON encoding
}

export interface DeadLetterEntry {
  task_id: string;
  spec: TaskSpec;
  error: string;
  attempts: number;
  failed_at: string; // RFC3339
}

export interface DeadLettersResponse {
  entries: DeadLetterEntry[];
}
