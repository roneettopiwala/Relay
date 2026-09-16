"use client";

import { useEffect, useState } from "react";

// MAX_HISTORY * pollIntervalMs = how much trend history the dashboard shows.
// Relay itself only ever reports a live snapshot (see internal/api's
// statsResponse) — this is where the "trend over time" actually comes from:
// the dashboard keeps its own rolling window of what it's already polled,
// client-side, rather than Relay storing a time-series.
const MAX_HISTORY = 60;

export interface Sample<T> {
  t: number; // ms since epoch, when this sample was received
  data: T;
}

export interface PollState<T> {
  data: T | null;
  history: Sample<T>[];
  connected: boolean;
  error: string | null;
  // hasLoaded distinguishes "the very first request hasn't resolved yet"
  // from "we have a real answer" (success or failure) — without it, the
  // pre-first-response state and a genuine failure both look identical
  // (connected=false, error=null vs connected=false, error=set at t=0), and
  // a caller ends up rendering the first ~poll-interval of every page load
  // as a false "disconnected"/"empty" instead of "loading".
  hasLoaded: boolean;
}

/** Polls url every intervalMs and keeps a rolling window of recent samples. */
export function usePolling<T>(url: string, intervalMs: number): PollState<T> {
  const [data, setData] = useState<T | null>(null);
  const [history, setHistory] = useState<Sample<T>[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasLoaded, setHasLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;

    async function poll() {
      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) {
          const body = (await res.json().catch(() => null)) as { error?: string } | null;
          throw new Error(body?.error ?? `status ${res.status}`);
        }
        const json = (await res.json()) as T;
        if (cancelled) return;
        setData(json);
        setConnected(true);
        setError(null);
        setHistory((h) => {
          const next = [...h, { t: Date.now(), data: json }];
          return next.length > MAX_HISTORY ? next.slice(next.length - MAX_HISTORY) : next;
        });
      } catch (err) {
        if (cancelled) return;
        // Relay being unreachable is a real, expected state (it might not be
        // running yet) — surface it, don't throw and crash the page.
        setConnected(false);
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) {
          setHasLoaded(true);
          timer = setTimeout(poll, intervalMs);
        }
      }
    }

    poll();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [url, intervalMs]);

  return { data, history, connected, error, hasLoaded };
}
