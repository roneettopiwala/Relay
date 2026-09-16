"use client";

import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { TooltipContentProps } from "recharts";
import type { ValueType, NameType } from "recharts/types/component/DefaultTooltipContent";
import { formatTime } from "@/lib/format";

type ChartTooltipProps = TooltipContentProps<ValueType, NameType>;

export interface Series {
  key: string;
  color: string;
  label: string;
}

interface TrendChartProps {
  title: string;
  data: Array<{ t: number } & Record<string, number>>;
  series: Series[];
  valueFormatter?: (v: number) => string;
}

// Mark specs from the dataviz skill: 2px line, round cap/join, no dots along
// the line (a legend + tooltip carry identity, not a label on every point),
// no gridlines/axes (the reference aesthetic is the bare line), a legend
// only when there's more than one series.
export function TrendChart({ title, data, series, valueFormatter = String }: TrendChartProps) {
  const hasData = data.length > 1;

  // Closure over series/valueFormatter instead of spreading them through
  // Recharts' typed content callback — its ContentType<TValue, TName> is
  // narrower (string|number union, not our plain `number`) than what
  // spreading extra props onto it can satisfy.
  function renderTooltip({ active, payload, label }: ChartTooltipProps) {
    if (!active || !payload?.length) return null;
    return (
      <div className="rounded-md border border-border bg-surface-raised px-3 py-2 text-[12px] shadow-lg">
        <div className="mb-1 text-ink-muted">{formatTime(Number(label))}</div>
        {payload.map((p) => {
          const s = series.find((s) => s.key === p.dataKey);
          return (
            <div key={String(p.dataKey)} className="flex items-center gap-1.5">
              <span aria-hidden className="size-1.5 rounded-full" style={{ backgroundColor: s?.color }} />
              <span className="text-ink-secondary">{s?.label ?? String(p.dataKey)}:</span>
              <span className="font-medium text-ink-primary">
                {typeof p.value === "number" ? valueFormatter(p.value) : String(p.value)}
              </span>
            </div>
          );
        })}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 px-5 py-4">
      <div className="flex items-center justify-between">
        <h3 className="text-[13px] text-ink-secondary">{title}</h3>
        {series.length > 1 && (
          <div className="flex items-center gap-3">
            {series.map((s) => (
              <div key={s.key} className="flex items-center gap-1.5">
                <span aria-hidden className="size-1.5 rounded-full" style={{ backgroundColor: s.color }} />
                <span className="text-[11px] text-ink-muted">{s.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>
      <div className="h-[140px]">
        {hasData ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 4 }}>
              <XAxis dataKey="t" hide />
              <YAxis hide domain={["auto", "auto"]} />
              <Tooltip content={renderTooltip} cursor={{ stroke: "var(--border)", strokeWidth: 1 }} />
              {series.map((s) => (
                <Line
                  key={s.key}
                  type="monotone"
                  dataKey={s.key}
                  stroke={s.color}
                  strokeWidth={2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-full items-center justify-center text-[12px] text-ink-muted">
            Waiting for data…
          </div>
        )}
      </div>
    </div>
  );
}
