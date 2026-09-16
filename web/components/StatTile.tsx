// Stat tile per the dataviz skill's contract: label (sentence case, no
// trailing colon) · value (sans semibold, proportional figures — never
// tabular-nums at this size, that's reserved for columns) · optional status
// dot, always paired with the label text, never color alone.
interface StatTileProps {
  label: string;
  value: string;
  statusColor?: string;
  sublabel?: string;
}

export function StatTile({ label, value, statusColor, sublabel }: StatTileProps) {
  return (
    <div className="flex flex-col gap-1.5 px-5 py-4">
      <div className="flex items-center gap-1.5">
        {statusColor && (
          <span
            aria-hidden
            className="size-1.5 shrink-0 rounded-full"
            style={{ backgroundColor: statusColor }}
          />
        )}
        <span className="text-[13px] text-ink-secondary">{label}</span>
      </div>
      <div className="text-[28px] font-semibold leading-none tracking-tight text-ink-primary">
        {value}
      </div>
      {sublabel && <div className="text-[12px] text-ink-muted">{sublabel}</div>}
    </div>
  );
}
